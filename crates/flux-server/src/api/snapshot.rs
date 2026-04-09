// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Snapshot SCD2 read endpoints (planning doc 28).
//!
//! - `POST /api/pipelines/{id}/nodes/{node_id}/snapshot/history` — single-key
//!   version history (sibling to `flux snapshot history`).
//! - `POST /api/pipelines/{id}/nodes/{node_id}/snapshot/diff` — dry-run
//!   diff preview: runs the upstream DAG with `dry_run_no_sinks`, then
//!   classifies the staged batch against the target's `flux_is_current`
//!   slice. The headline differentiator vs. dbt/Dagster — engineers can
//!   see exactly which rows would be inserted / closed / unchanged before
//!   any write touches prod.
//!
//! v1 is postgresql-only — non-postgres connectors are rejected with the
//! actionable error from `SnapshotQueryError`.
//!
//! The diff endpoint guards against OOMs with a hard staged-row cap
//! ([`DEFAULT_DIFF_STAGED_ROW_CAP`]) and caches results in-memory keyed on
//! `(pipeline_id, pipeline_updated_at, node_id, environment)` so reopening
//! the modal doesn't re-run a 30s diff. Cache invalidates automatically
//! when the pipeline is saved (since `updated_at` changes).

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use crate::api::ApiError;
use crate::state::AppState;
use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use flux_connectors::snapshot_query::{
    DEFAULT_DIFF_SAMPLE_LIMIT, DEFAULT_DIFF_STAGED_ROW_CAP, DiffSummary, HistoryRow,
    SnapshotQueryError, align_key_values, classify_diff, read_current_signals_pg, read_history_pg,
    resolve_snapshot_sink, stringify_rows,
};
use flux_datafusion::{ExecutionOptions, PipelineExecutor};
use flux_engine::NodeId;
use flux_engine::pipeline_store::PipelineId;
use serde::{Deserialize, Serialize};
use tracing::error;

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/{id}/nodes/{node_id}/snapshot/history",
            post(snapshot_history),
        )
        .route("/{id}/nodes/{node_id}/snapshot/diff", post(snapshot_diff))
}

#[derive(Debug, Deserialize)]
pub struct SnapshotHistoryRequest {
    /// Optional environment override; falls back to the pipeline's default
    /// environment when omitted.
    #[serde(default)]
    pub environment: Option<String>,
    /// `column → value` map. Must cover every column in the sink's
    /// `unique_keys` (no more, no less).
    pub key: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotHistoryResponse {
    pub node_id: String,
    pub table: String,
    pub unique_keys: Vec<String>,
    pub comparison_columns: Vec<String>,
    pub key: HashMap<String, String>,
    pub version_count: usize,
    pub versions: Vec<HistoryVersionDto>,
}

#[derive(Debug, Serialize)]
pub struct HistoryVersionDto {
    pub flux_scd_id: String,
    pub flux_valid_from: String,
    pub flux_valid_to: Option<String>,
    pub flux_is_current: bool,
    /// Tracked comparison columns for this version, keyed by column name.
    pub comparison: HashMap<String, String>,
}

async fn snapshot_history(
    State(state): State<AppState>,
    Path((id, node_id)): Path<(String, String)>,
    Json(req): Json<SnapshotHistoryRequest>,
) -> Result<Json<SnapshotHistoryResponse>, (StatusCode, Json<ApiError>)> {
    let pipeline_id = id
        .parse::<PipelineId>()
        .map_err(|_| ApiError::bad_request(format!("invalid pipeline ID: {id}")))?;
    let record = state
        .pipeline_store
        .get(&pipeline_id)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("pipeline", &id))?;

    let environment = req
        .environment
        .clone()
        .unwrap_or_else(|| record.pipeline.default_environment.clone());

    let secret_resolver = state.secret_resolver();
    let resolved = resolve_snapshot_sink(
        &record,
        &node_id,
        &environment,
        &HashMap::new(),
        secret_resolver.as_ref(),
    )
    .map_err(map_snapshot_error)?;

    let key_values = align_key_values(&resolved, &req.key).map_err(map_snapshot_error)?;

    let rows = read_history_pg(&resolved, &key_values)
        .await
        .map_err(map_snapshot_error)?;

    let versions = rows
        .into_iter()
        .map(|r: HistoryRow| HistoryVersionDto {
            flux_scd_id: r.scd_id,
            flux_valid_from: r.valid_from,
            flux_valid_to: r.valid_to,
            flux_is_current: r.is_current,
            comparison: resolved
                .comparison_columns
                .iter()
                .cloned()
                .zip(r.comparison_values.into_iter())
                .collect(),
        })
        .collect::<Vec<_>>();

    Ok(Json(SnapshotHistoryResponse {
        node_id: resolved.node_id.clone(),
        table: resolved.table.clone(),
        unique_keys: resolved.unique_keys.clone(),
        comparison_columns: resolved.comparison_columns.clone(),
        key: req.key,
        version_count: versions.len(),
        versions,
    }))
}

/// Translate snapshot-query errors into HTTP responses. Validation/lookup
/// errors are 4xx; database errors are 5xx with the original error in
/// `details` so the frontend can surface it.
fn map_snapshot_error(err: SnapshotQueryError) -> (StatusCode, Json<ApiError>) {
    use SnapshotQueryError::*;
    match err {
        NodeNotFound(_) => (StatusCode::NOT_FOUND, Json(ApiError::new(err.to_string()))),
        NotASink(_)
        | NoMaterialization(_)
        | NotASnapshot { .. }
        | MissingSnapshotBlock(_)
        | MissingUniqueKeys(_)
        | UnsupportedConnector { .. }
        | MissingCheckColumns
        | MissingUpdatedAt
        | MissingTable(_)
        | InvalidConfig { .. }
        | KeyMismatch { .. }
        | StagedColumnMissing(_)
        | StagedFormat { .. } => ApiError::bad_request(err.to_string()),
        SecretResolution { .. } => ApiError::internal(err.to_string()),
        Connect { .. } | Query { .. } => {
            error!(error = %err, "snapshot query failed");
            (
                StatusCode::BAD_GATEWAY,
                Json(ApiError::with_details(
                    "snapshot query failed",
                    err.to_string(),
                )),
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot diff preview (planning doc 28)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SnapshotDiffRequest {
    /// Optional environment override; falls back to the pipeline's default
    /// environment when omitted.
    #[serde(default)]
    pub environment: Option<String>,
    /// Variable overrides applied to the dry-run, mirroring `RunRequest`.
    #[serde(default)]
    pub variables: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotDiffResponse {
    pub node_id: String,
    pub table: String,
    pub environment: String,
    pub unique_keys: Vec<String>,
    pub comparison_columns: Vec<String>,
    pub stats: SnapshotDiffStats,
    pub sample: Vec<SnapshotDiffSampleDto>,
    /// Number of staged rows that contributed to the diff. If
    /// `sample_truncated` is `true`, more staged rows existed than were
    /// materialized — the diff covers only `staged_row_count` of them.
    pub staged_row_count: usize,
    pub sample_truncated: bool,
    pub staged_row_cap: usize,
    /// Whether this response was served from the in-memory cache.
    pub cached: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotDiffStats {
    pub unchanged: u64,
    pub changed: u64,
    pub new_versions: u64,
    pub gone: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotDiffSampleDto {
    pub classification: String,
    pub unique_key: Vec<String>,
}

async fn snapshot_diff(
    State(state): State<AppState>,
    Path((id, node_id)): Path<(String, String)>,
    Json(req): Json<SnapshotDiffRequest>,
) -> Result<Json<SnapshotDiffResponse>, (StatusCode, Json<ApiError>)> {
    let pipeline_id = id
        .parse::<PipelineId>()
        .map_err(|_| ApiError::bad_request(format!("invalid pipeline ID: {id}")))?;
    let record = state
        .pipeline_store
        .get(&pipeline_id)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("pipeline", &id))?;

    let environment = req
        .environment
        .clone()
        .unwrap_or_else(|| record.pipeline.default_environment.clone());

    // Validate variable overrides exactly the way `run_pipeline` does so a
    // diff preview surfaces the same errors a real run would.
    let override_errors =
        flux_engine::variables::validate_overrides(&record.pipeline, &req.variables);
    if !override_errors.is_empty() {
        return Err(ApiError::bad_request(override_errors.join("; ")));
    }

    // ---- Cache lookup ----------------------------------------------------
    let cache_key = DiffCacheKey {
        pipeline_id: id.clone(),
        updated_at: record.updated_at,
        node_id: node_id.clone(),
        environment: environment.clone(),
        variables_hash: hash_variables(&req.variables),
    };
    if let Some(mut cached) = diff_cache().get(&cache_key) {
        cached.cached = true;
        return Ok(Json(cached));
    }

    // ---- Resolve sink (validates that it's a snapshot postgres sink) ----
    let secret_resolver = state.secret_resolver();
    let resolved = resolve_snapshot_sink(
        &record,
        &node_id,
        &environment,
        &req.variables,
        secret_resolver.as_ref(),
    )
    .map_err(map_snapshot_error)?;

    // ---- Run upstream pipeline as a dry-run ------------------------------
    let provider_registry = state.connector_registry.to_provider_registry();
    let cancel = Arc::new(AtomicBool::new(false));
    let options = ExecutionOptions {
        environment: environment.clone(),
        run_store: None,
        cancel: Arc::clone(&cancel),
        environment_resolver: None,
        progress: None,
        variable_overrides: req.variables.clone(),
        secret_resolver: state.secret_resolver(),
        session_factory: state.session_factory.clone(),
        incremental_state_store: None,
        full_refresh: false,
        bootstrap_incremental: false,
        dry_run_no_sinks: true,
    };
    let (pipeline_result, _run) = PipelineExecutor::execute(
        &record.pipeline,
        &provider_registry,
        &options,
    )
    .await
    .map_err(|e| {
        error!(pipeline = %record.pipeline.name, error = %e, "snapshot diff dry-run failed");
        ApiError::internal(format!("snapshot diff dry-run failed: {e}"))
    })?;

    // ---- Collect upstream-of-sink batches as staged input ----------------
    let sink_node_id = NodeId::new(&node_id);
    let upstream_ids = record.pipeline.upstream_of(&sink_node_id);
    let mut staged_batches = Vec::new();
    for uid in upstream_ids {
        if let Some(batches) = pipeline_result.node_outputs.get(uid) {
            staged_batches.extend(batches.iter().cloned());
        }
    }

    // ---- Stringify (with row cap) + read current target + classify ------
    let (staged, sample_truncated) = stringify_rows(
        &staged_batches,
        &resolved.unique_keys,
        &resolved.comparison_columns,
        DEFAULT_DIFF_STAGED_ROW_CAP,
    )
    .map_err(map_snapshot_error)?;
    let staged_row_count = staged.len();
    let current = read_current_signals_pg(&resolved)
        .await
        .map_err(map_snapshot_error)?;
    let summary = classify_diff(staged, current, DEFAULT_DIFF_SAMPLE_LIMIT);

    let response = build_diff_response(
        &resolved.node_id,
        &resolved.table,
        &environment,
        &resolved.unique_keys,
        &resolved.comparison_columns,
        summary,
        staged_row_count,
        sample_truncated,
    );

    diff_cache().insert(cache_key, response.clone());
    Ok(Json(response))
}

#[allow(clippy::too_many_arguments)]
fn build_diff_response(
    node_id: &str,
    table: &str,
    environment: &str,
    unique_keys: &[String],
    comparison_columns: &[String],
    summary: DiffSummary,
    staged_row_count: usize,
    sample_truncated: bool,
) -> SnapshotDiffResponse {
    let sample = summary
        .sample
        .into_iter()
        .map(|s| SnapshotDiffSampleDto {
            classification: match s.classification {
                flux_connectors::snapshot_query::DiffClassification::Unchanged => {
                    "unchanged".into()
                }
                flux_connectors::snapshot_query::DiffClassification::Changed => "changed".into(),
                flux_connectors::snapshot_query::DiffClassification::New => "new".into(),
                flux_connectors::snapshot_query::DiffClassification::Gone => "gone".into(),
            },
            unique_key: s.unique_key,
        })
        .collect();
    SnapshotDiffResponse {
        node_id: node_id.to_string(),
        table: table.to_string(),
        environment: environment.to_string(),
        unique_keys: unique_keys.to_vec(),
        comparison_columns: comparison_columns.to_vec(),
        stats: SnapshotDiffStats {
            unchanged: summary.unchanged,
            changed: summary.changed,
            new_versions: summary.new_versions,
            gone: summary.gone,
        },
        sample,
        staged_row_count,
        sample_truncated,
        staged_row_cap: DEFAULT_DIFF_STAGED_ROW_CAP,
        cached: false,
    }
}

// ---------------------------------------------------------------------------
// In-memory diff cache
//
// Bounded map keyed on (pipeline_id, pipeline_updated_at, node_id, env,
// variables_hash). Capacity 16, TTL 5 minutes. Pipeline saves change
// `updated_at` so cache entries naturally invalidate when the user edits
// the pipeline. Lives in a process-global `OnceLock` so we don't have to
// add a field to `AppState` (and update every test that constructs one).
// ---------------------------------------------------------------------------

const DIFF_CACHE_CAPACITY: usize = 16;
const DIFF_CACHE_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DiffCacheKey {
    pipeline_id: String,
    updated_at: SystemTime,
    node_id: String,
    environment: String,
    variables_hash: u64,
}

struct DiffCache {
    entries: Vec<(DiffCacheKey, Instant, SnapshotDiffResponse)>,
}

impl DiffCache {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(DIFF_CACHE_CAPACITY),
        }
    }

    fn get(&self, key: &DiffCacheKey) -> Option<SnapshotDiffResponse> {
        let now = Instant::now();
        for (k, inserted, value) in &self.entries {
            if k == key && now.duration_since(*inserted) < DIFF_CACHE_TTL {
                return Some(value.clone());
            }
        }
        None
    }

    fn insert(&mut self, key: DiffCacheKey, value: SnapshotDiffResponse) {
        let now = Instant::now();
        self.entries
            .retain(|(_, inserted, _)| now.duration_since(*inserted) < DIFF_CACHE_TTL);
        if let Some(idx) = self.entries.iter().position(|(k, _, _)| k == &key) {
            self.entries.remove(idx);
        }
        if self.entries.len() >= DIFF_CACHE_CAPACITY {
            self.entries.remove(0);
        }
        self.entries.push((key, now, value));
    }
}

struct DiffCacheHandle {
    inner: Arc<Mutex<DiffCache>>,
}

impl DiffCacheHandle {
    fn get(&self, key: &DiffCacheKey) -> Option<SnapshotDiffResponse> {
        self.inner.lock().ok()?.get(key)
    }
    fn insert(&self, key: DiffCacheKey, value: SnapshotDiffResponse) {
        if let Ok(mut g) = self.inner.lock() {
            g.insert(key, value);
        }
    }
}

fn diff_cache() -> DiffCacheHandle {
    static CACHE: OnceLock<Arc<Mutex<DiffCache>>> = OnceLock::new();
    DiffCacheHandle {
        inner: Arc::clone(CACHE.get_or_init(|| Arc::new(Mutex::new(DiffCache::new())))),
    }
}

fn hash_variables(vars: &HashMap<String, serde_json::Value>) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut keys: Vec<&String> = vars.keys().collect();
    keys.sort();
    let mut h = DefaultHasher::new();
    for k in keys {
        k.hash(&mut h);
        // serde_json::Value isn't Hash; serialize to a stable string.
        if let Ok(s) = serde_json::to_string(&vars[k]) {
            s.hash(&mut h);
        }
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(suffix: &str) -> DiffCacheKey {
        DiffCacheKey {
            pipeline_id: "p1".into(),
            updated_at: SystemTime::UNIX_EPOCH,
            node_id: format!("sink-{suffix}"),
            environment: "dev".into(),
            variables_hash: 0,
        }
    }

    fn dummy_response(node_id: &str) -> SnapshotDiffResponse {
        SnapshotDiffResponse {
            node_id: node_id.to_string(),
            table: "t".into(),
            environment: "dev".into(),
            unique_keys: vec![],
            comparison_columns: vec![],
            stats: SnapshotDiffStats {
                unchanged: 0,
                changed: 0,
                new_versions: 0,
                gone: 0,
            },
            sample: vec![],
            staged_row_count: 0,
            sample_truncated: false,
            staged_row_cap: DEFAULT_DIFF_STAGED_ROW_CAP,
            cached: false,
        }
    }

    #[test]
    fn diff_cache_round_trip_and_overwrite() {
        let mut c = DiffCache::new();
        let k = key("a");
        c.insert(k.clone(), dummy_response("first"));
        c.insert(k.clone(), dummy_response("second"));
        let got = c.get(&k).expect("hit");
        assert_eq!(got.node_id, "second");
        assert_eq!(c.entries.len(), 1);
    }

    #[test]
    fn diff_cache_evicts_at_capacity() {
        let mut c = DiffCache::new();
        for i in 0..(DIFF_CACHE_CAPACITY + 4) {
            c.insert(key(&i.to_string()), dummy_response(&i.to_string()));
        }
        assert_eq!(c.entries.len(), DIFF_CACHE_CAPACITY);
        // Oldest entries evicted.
        assert!(c.get(&key("0")).is_none());
        assert!(
            c.get(&key(&(DIFF_CACHE_CAPACITY + 3).to_string()))
                .is_some()
        );
    }

    #[test]
    fn variables_hash_is_order_independent() {
        let mut a = HashMap::new();
        a.insert("x".to_string(), serde_json::json!(1));
        a.insert("y".to_string(), serde_json::json!("two"));
        let mut b = HashMap::new();
        b.insert("y".to_string(), serde_json::json!("two"));
        b.insert("x".to_string(), serde_json::json!(1));
        assert_eq!(hash_variables(&a), hash_variables(&b));
    }
}
