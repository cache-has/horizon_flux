// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Resource catalog API routes (planning doc 34).
//!
//! Exposes a browseable, searchable view of resources that flux pipelines
//! produce and consume, with user-authored annotation metadata layered on
//! top of auto-derived facts from the lineage graph.

use crate::api::ApiError;
use crate::state::{AppState, CatalogEvent};
use axum::Json;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use flux_datafusion::{PipelineRun, RunStatus};
use flux_engine::catalog::{
    self, AnnotationFile, AnnotationOwner, AnnotationResource, Catalog, CatalogEntry,
    ColumnAnnotation, DiscoveredResource, ResourceAnnotation,
};
use flux_engine::lineage::{LineageGraph, ResourceBinding, ResourceFingerprint};
use flux_engine::pipeline_store::PipelineId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use tracing::warn;

/// Build the `/catalog` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/resources", get(list_resources))
        .route("/resources/detail", get(get_resource))
        .route("/resources/metadata", put(update_metadata))
        .route("/describe", post(describe))
        .route("/tags", get(list_tags))
        .route("/owners", get(list_owners))
}

// ---------------------------------------------------------------------------
// Query / request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ResourceListQuery {
    /// Full-text search query.
    #[serde(default)]
    q: Option<String>,
    /// Filter by tag.
    #[serde(default)]
    tag: Option<String>,
    /// Filter by owner team.
    #[serde(default)]
    owner: Option<String>,
    /// Filter by environment.
    #[serde(default)]
    environment: Option<String>,
    /// Environment for lineage graph (defaults to "default").
    #[serde(default = "default_env")]
    env: String,
}

fn default_env() -> String {
    "default".into()
}

#[derive(Debug, Serialize)]
struct ResourceListResponse {
    data: Vec<CatalogEntry>,
    total: usize,
}

#[derive(Debug, Serialize)]
struct TagsResponse {
    tags: Vec<String>,
}

#[derive(Debug, Serialize)]
struct OwnersResponse {
    owners: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct MetadataUpdateRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    owner: Option<OwnerInput>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    columns: Option<BTreeMap<String, ColumnInput>>,
    #[serde(default)]
    custom: Option<BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct OwnerInput {
    team: Option<String>,
    contact: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ColumnInput {
    description: Option<String>,
    accepted_values: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct DescribeRequest {
    /// Specific fingerprint to scaffold. If omitted and `all` is true,
    /// scaffolds all undocumented resources.
    #[serde(default)]
    fingerprint: Option<String>,
    /// Scaffold metadata files for every resource that doesn't have one yet.
    #[serde(default)]
    all: bool,
    /// Environment for lineage graph.
    #[serde(default = "default_env")]
    environment: String,
}

#[derive(Debug, Serialize)]
struct DescribeResponse {
    /// Paths of files that were created.
    created: Vec<String>,
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Build a [`Catalog`] from the current lineage state and metadata directory,
/// then enrich entries with freshness data from the run store.
fn build_catalog(
    state: &AppState,
    environment: &str,
) -> Result<Catalog, (StatusCode, Json<ApiError>)> {
    let graph = build_lineage_graph(state, environment)?;

    let metadata_dir = state
        .metadata_dir
        .as_deref()
        .unwrap_or_else(|| std::path::Path::new("metadata"));

    let mut catalog = Catalog::build(&graph, metadata_dir);
    enrich_from_runs(state, &mut catalog);
    Ok(catalog)
}

/// Populate `last_updated` and `row_count` on catalog entries from the run
/// store. For each resource with producers, we find the latest successful run
/// of any producing pipeline and extract the sink node's stats.
fn enrich_from_runs(state: &AppState, catalog: &mut Catalog) {
    // Collect unique producer pipeline IDs across all entries.
    let producer_ids: HashSet<PipelineId> = catalog
        .entries
        .iter()
        .flat_map(|e| e.derived.producers.iter().map(|p| p.pipeline_id.clone()))
        .collect();

    if producer_ids.is_empty() {
        return;
    }

    // Build pipeline_id → pipeline_name map.
    let mut id_to_name: HashMap<PipelineId, String> = HashMap::new();
    for pid in &producer_ids {
        if let Ok(Some(record)) = state.pipeline_store.get(pid) {
            id_to_name.insert(pid.clone(), record.pipeline.name.clone());
        }
    }

    // Fetch the latest successful run for each pipeline name.
    let mut name_to_latest: HashMap<String, PipelineRun> = HashMap::new();
    for name in id_to_name.values() {
        if name_to_latest.contains_key(name) {
            continue;
        }
        if let Ok(runs) = state.run_store.list_runs(Some(name), 10) {
            if let Some(run) = runs.into_iter().find(|r| r.status == RunStatus::Success) {
                name_to_latest.insert(name.clone(), run);
            }
        }
    }

    // Enrich each entry.
    for entry in &mut catalog.entries {
        for producer in &entry.derived.producers {
            let Some(name) = id_to_name.get(&producer.pipeline_id) else {
                continue;
            };
            let Some(run) = name_to_latest.get(name) else {
                continue;
            };

            // Update last_updated from run end_time.
            if let Some(end_time) = run.end_time {
                let ts = end_time
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let iso = format_epoch_ms(ts.as_millis() as i64);
                // Keep the most recent timestamp if multiple producers.
                if entry
                    .derived
                    .last_updated
                    .as_ref()
                    .is_none_or(|existing| iso > *existing)
                {
                    entry.derived.last_updated = Some(iso);
                }
            }

            // Extract row_count from the sink node's stats.
            if let Some(node_stats) = run
                .node_stats
                .iter()
                .find(|s| s.node_id.0 == producer.node_id)
            {
                let rows = node_stats.rows_out;
                if rows > 0 {
                    entry.derived.row_count = Some(entry.derived.row_count.unwrap_or(0).max(rows));
                }
            }

            // Break after first match — first producer with a run is sufficient
            // for freshness. Multiple producers will use the most recent.
            break;
        }
    }
}

/// Format epoch milliseconds as an ISO 8601 UTC timestamp string.
fn format_epoch_ms(ms: i64) -> String {
    let secs = ms / 1000;
    let nanos = ((ms % 1000) * 1_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, nanos)
        .unwrap_or(chrono::DateTime::UNIX_EPOCH);
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Build a [`LineageGraph`] from stored bindings for the given environment.
fn build_lineage_graph(
    state: &AppState,
    environment: &str,
) -> Result<LineageGraph, (StatusCode, Json<ApiError>)> {
    let stored = state
        .lineage_store
        .all_bindings(environment)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let bindings: Vec<ResourceBinding> = stored
        .into_iter()
        .filter_map(|sb| {
            let pipeline_id = sb.pipeline_id.parse::<PipelineId>().ok()?;
            Some(ResourceBinding {
                pipeline_id,
                node_id: sb.node_id,
                direction: sb.direction,
                fingerprint: sb.resource_fingerprint,
            })
        })
        .collect();

    Ok(LineageGraph {
        edges: Vec::new(),
        bindings,
    })
}

/// Get the metadata directory path from state, defaulting to `metadata/`.
fn metadata_dir(state: &AppState) -> PathBuf {
    state
        .metadata_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("metadata"))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /api/catalog/resources` — list resources with optional filters and search.
async fn list_resources(
    State(state): State<AppState>,
    Query(q): Query<ResourceListQuery>,
) -> Result<Json<ResourceListResponse>, (StatusCode, Json<ApiError>)> {
    let catalog = build_catalog(&state, &q.env)?;

    let entries: Vec<CatalogEntry> = if let Some(query) = &q.q {
        // Full-text search first, then apply filters.
        catalog.search(query).into_iter().cloned().collect()
    } else {
        catalog.entries.clone()
    };

    // Apply filters.
    let entries: Vec<CatalogEntry> = entries
        .into_iter()
        .filter(|e| {
            if let Some(tag) = &q.tag {
                if !e.tags.iter().any(|t| t == tag) {
                    return false;
                }
            }
            if let Some(owner) = &q.owner {
                let matches = e
                    .owner
                    .as_ref()
                    .and_then(|o| o.team.as_ref())
                    .is_some_and(|t| t == owner);
                if !matches {
                    return false;
                }
            }
            if let Some(env) = &q.environment {
                if e.environment.as_ref() != Some(env) {
                    return false;
                }
            }
            true
        })
        .collect();

    let total = entries.len();
    Ok(Json(ResourceListResponse {
        data: entries,
        total,
    }))
}

/// `GET /api/catalog/resources/detail?fingerprint=...` — full detail for a single resource.
async fn get_resource(
    State(state): State<AppState>,
    Query(q): Query<ResourceDetailQuery>,
) -> Result<Json<CatalogEntry>, (StatusCode, Json<ApiError>)> {
    let catalog = build_catalog(&state, &q.env)?;
    let fp = ResourceFingerprint::new(&q.fingerprint);

    catalog
        .get(&fp)
        .cloned()
        .map(Json)
        .ok_or_else(|| ApiError::not_found("Resource", &q.fingerprint))
}

#[derive(Debug, Deserialize)]
struct ResourceDetailQuery {
    fingerprint: String,
    #[serde(default = "default_env")]
    env: String,
}

#[derive(Debug, Deserialize)]
struct MetadataFingerprintQuery {
    fingerprint: String,
}

#[derive(Debug, Deserialize)]
struct EnvQuery {
    #[serde(default = "default_env")]
    env: String,
}

/// `PUT /api/catalog/resources/metadata?fingerprint=...` — create or update annotation.
///
/// Writes the annotation to a YAML file in the metadata directory using atomic
/// file replacement (write to temp file, then rename).
async fn update_metadata(
    State(state): State<AppState>,
    Query(q): Query<MetadataFingerprintQuery>,
    Json(body): Json<MetadataUpdateRequest>,
) -> Result<(StatusCode, Json<CatalogEntry>), (StatusCode, Json<ApiError>)> {
    let fingerprint = q.fingerprint;
    let fp = ResourceFingerprint::new(&fingerprint);
    let meta_dir = metadata_dir(&state);

    // Build the annotation. If a file already exists, merge with existing.
    let relative_path = catalog::fingerprint_to_filename(&fp);
    let file_path = meta_dir.join(&relative_path);

    let existing: Option<ResourceAnnotation> = if file_path.exists() {
        match catalog::parse_annotation_file(&file_path) {
            Ok(ann) => Some(ann),
            Err(e) => {
                warn!(
                    "Failed to parse existing annotation at {}: {e}",
                    file_path.display()
                );
                None
            }
        }
    } else {
        None
    };

    let annotation = build_annotation_from_request(&fingerprint, &body, existing.as_ref());

    // Serialize to YAML.
    let yaml = serde_yaml::to_string(&annotation)
        .map_err(|e| ApiError::internal(format!("failed to serialize annotation: {e}")))?;

    // Atomic write: write to a temp file, then rename.
    if let Some(parent) = file_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| ApiError::internal(format!("failed to create directory: {e}")))?;
    }

    let temp_path = file_path.with_extension("yaml.tmp");
    std::fs::write(&temp_path, &yaml)
        .map_err(|e| ApiError::internal(format!("failed to write temp file: {e}")))?;

    std::fs::rename(&temp_path, &file_path)
        .map_err(|e| ApiError::internal(format!("failed to rename temp file: {e}")))?;

    // Broadcast catalog event.
    let _ = state.catalog_event_tx.send(CatalogEvent::MetadataUpdated {
        fingerprint: fingerprint.clone(),
    });

    // Rebuild the catalog to return the updated entry.
    let catalog = build_catalog(&state, "default")?;
    let entry = catalog.get(&fp).cloned().unwrap_or_else(|| {
        // Resource not in lineage — create a standalone entry from the annotation.
        let discovered = HashMap::new();
        let mut annotations = HashMap::new();
        annotations.insert(
            fp.clone(),
            AnnotationFile {
                annotation,
                path: file_path,
            },
        );
        let cat = Catalog::from_parts(&discovered, &annotations);
        cat.entries.into_iter().next().unwrap()
    });

    Ok((StatusCode::OK, Json(entry)))
}

/// Build a `ResourceAnnotation` from the API request, optionally merging with
/// an existing annotation.
fn build_annotation_from_request(
    fingerprint: &str,
    body: &MetadataUpdateRequest,
    existing: Option<&ResourceAnnotation>,
) -> ResourceAnnotation {
    let default_ann = ResourceAnnotation {
        resource: AnnotationResource {
            fingerprint: fingerprint.to_string(),
            environment: None,
        },
        name: None,
        description: None,
        owner: None,
        tags: Vec::new(),
        columns: BTreeMap::new(),
        custom: BTreeMap::new(),
    };

    let base = existing.unwrap_or(&default_ann);

    let owner = body
        .owner
        .as_ref()
        .map(|o| AnnotationOwner {
            team: o
                .team
                .clone()
                .or_else(|| base.owner.as_ref().and_then(|bo| bo.team.clone())),
            contact: o
                .contact
                .clone()
                .or_else(|| base.owner.as_ref().and_then(|bo| bo.contact.clone())),
        })
        .or_else(|| base.owner.clone());

    let columns = body
        .columns
        .as_ref()
        .map(|cols| {
            cols.iter()
                .map(|(name, col)| {
                    (
                        name.clone(),
                        ColumnAnnotation {
                            description: col.description.clone(),
                            accepted_values: col.accepted_values.clone(),
                        },
                    )
                })
                .collect()
        })
        .unwrap_or_else(|| base.columns.clone());

    let custom = body
        .custom
        .as_ref()
        .map(|c| {
            c.iter()
                .map(|(k, v)| (k.clone(), json_to_yaml_value(v)))
                .collect()
        })
        .unwrap_or_else(|| base.custom.clone());

    ResourceAnnotation {
        resource: AnnotationResource {
            fingerprint: fingerprint.to_string(),
            environment: base.resource.environment.clone(),
        },
        name: body.name.clone().or_else(|| base.name.clone()),
        description: body
            .description
            .clone()
            .or_else(|| base.description.clone()),
        owner,
        tags: body.tags.clone().unwrap_or_else(|| base.tags.clone()),
        columns,
        custom,
    }
}

/// Convert a `serde_json::Value` to a `serde_yaml::Value`.
fn json_to_yaml_value(v: &serde_json::Value) -> serde_yaml::Value {
    // Round-trip through string is simple and handles all cases.
    let json = serde_json::to_string(v).unwrap_or_default();
    serde_yaml::from_str(&json).unwrap_or(serde_yaml::Value::Null)
}

/// `POST /api/catalog/describe` — scaffold metadata files for resources.
async fn describe(
    State(state): State<AppState>,
    Json(body): Json<DescribeRequest>,
) -> Result<(StatusCode, Json<DescribeResponse>), (StatusCode, Json<ApiError>)> {
    let graph = build_lineage_graph(&state, &body.environment)?;
    let discovered = catalog::discover_resources(&graph);
    let meta_dir = metadata_dir(&state);

    let mut created = Vec::new();

    if let Some(fp_str) = &body.fingerprint {
        // Scaffold a single resource.
        let fp = ResourceFingerprint::new(fp_str);
        let resource = discovered
            .get(&fp)
            .ok_or_else(|| ApiError::not_found("Resource", fp_str))?;

        let path = scaffold_one(resource, &meta_dir)?;
        created.push(path);
    } else if body.all {
        // Scaffold all resources without existing annotation files.
        for resource in discovered.values() {
            let relative_path = catalog::fingerprint_to_filename(&resource.fingerprint);
            let file_path = meta_dir.join(&relative_path);
            if !file_path.exists() {
                let path = scaffold_one(resource, &meta_dir)?;
                created.push(path);
            }
        }
        created.sort();
    } else {
        return Err(ApiError::bad_request(
            "provide either a fingerprint or set all=true",
        ));
    }

    Ok((StatusCode::CREATED, Json(DescribeResponse { created })))
}

/// Write a scaffold YAML file for a single resource. Returns the relative path.
fn scaffold_one(
    resource: &DiscoveredResource,
    meta_dir: &std::path::Path,
) -> Result<String, (StatusCode, Json<ApiError>)> {
    let relative_path = catalog::fingerprint_to_filename(&resource.fingerprint);
    let file_path = meta_dir.join(&relative_path);

    if let Some(parent) = file_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| ApiError::internal(format!("failed to create directory: {e}")))?;
    }

    let yaml = catalog::scaffold_annotation(resource);
    std::fs::write(&file_path, &yaml)
        .map_err(|e| ApiError::internal(format!("failed to write scaffold file: {e}")))?;

    Ok(relative_path.to_string_lossy().to_string())
}

/// `GET /api/catalog/tags` — list all unique tags across resources.
async fn list_tags(
    State(state): State<AppState>,
    Query(q): Query<EnvQuery>,
) -> Result<Json<TagsResponse>, (StatusCode, Json<ApiError>)> {
    let catalog = build_catalog(&state, &q.env)?;
    Ok(Json(TagsResponse {
        tags: catalog.all_tags(),
    }))
}

/// `GET /api/catalog/owners` — list all unique owner teams.
async fn list_owners(
    State(state): State<AppState>,
    Query(q): Query<EnvQuery>,
) -> Result<Json<OwnersResponse>, (StatusCode, Json<ApiError>)> {
    let catalog = build_catalog(&state, &q.env)?;
    Ok(Json(OwnersResponse {
        owners: catalog.all_owners(),
    }))
}
