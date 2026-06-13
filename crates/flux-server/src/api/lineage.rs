// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cross-pipeline lineage API routes (planning doc 31) and column-level
//! lineage API routes (planning doc 35).
//!
//! Exposes the lineage graph, upstream/downstream queries, impact analysis,
//! cycle detection, orphan/dangling-reference detection, and column-level
//! lineage traces.

use crate::api::ApiError;
use crate::state::AppState;
use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use flux_engine::column_lineage::{
    BoundaryColumn, ColumnKey, ColumnLineageGraph, Confidence, RelationshipKind, TraceOptions,
    derive_cross_pipeline_column_lineage,
};
use flux_engine::lineage::{
    BindingDirection, EdgeSource, LineageEdge, LineageGraph, ResourceBinding, ResourceFingerprint,
};
use flux_engine::pipeline_store::PipelineId;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Build the `/lineage` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/graph", get(lineage_graph))
        .route("/pipelines/{id}/upstream", get(upstream))
        .route("/pipelines/{id}/downstream", get(downstream))
        .route("/pipelines/{id}/impact", get(impact))
        .route("/cycles", get(cycles))
        .route("/orphans", get(orphans))
        // Column-level lineage endpoints (planning doc 35).
        .route(
            "/columns/{fingerprint}/{column}/upstream",
            get(column_upstream),
        )
        .route(
            "/columns/{fingerprint}/{column}/downstream",
            get(column_downstream),
        )
        .route("/columns/{fingerprint}/{column}/impact", get(column_impact))
        .route("/columns/search", get(column_search))
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Optional environment query parameter (defaults to "dev").
#[derive(Debug, Deserialize)]
pub struct EnvQuery {
    #[serde(default = "default_env")]
    pub environment: String,
}

fn default_env() -> String {
    "dev".into()
}

/// Default observation window: 90 days in milliseconds.
const DEFAULT_OBSERVATION_WINDOW_MS: i64 = 90 * 24 * 60 * 60 * 1000;

/// Build a [`LineageGraph`] from stored bindings and runtime observations for
/// the given environment.
fn build_graph(
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

    let edges = derive_edges(&bindings);
    let mut graph = LineageGraph { edges, bindings };

    // Merge runtime-observed edges from recent pipeline executions.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let since_ms = now_ms - DEFAULT_OBSERVATION_WINDOW_MS;

    let observations = state
        .lineage_store
        .query_observations(environment, since_ms)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let observed_edges = derive_observed_edges(&observations);
    graph.add_observed_edges(observed_edges);

    Ok(graph)
}

/// Derive edges from runtime observations by matching sink writes to source
/// reads on the same resource fingerprint.
fn derive_observed_edges(observations: &[flux_datafusion::LineageObservation]) -> Vec<LineageEdge> {
    use std::collections::HashMap;

    let mut sinks: HashMap<&ResourceFingerprint, Vec<&flux_datafusion::LineageObservation>> =
        HashMap::new();
    for obs in observations {
        if obs.direction == BindingDirection::Sink {
            sinks
                .entry(&obs.resource_fingerprint)
                .or_default()
                .push(obs);
        }
    }

    let mut edges = Vec::new();
    for obs in observations {
        if obs.direction != BindingDirection::Source {
            continue;
        }
        if let Some(sink_obs) = sinks.get(&obs.resource_fingerprint) {
            for sink in sink_obs {
                if sink.pipeline_id == obs.pipeline_id && sink.node_id == obs.node_id {
                    continue;
                }
                let upstream_id = match sink.pipeline_id.parse::<PipelineId>() {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                let downstream_id = match obs.pipeline_id.parse::<PipelineId>() {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                edges.push(LineageEdge {
                    upstream_pipeline_id: upstream_id,
                    upstream_node_id: sink.node_id.clone(),
                    downstream_pipeline_id: downstream_id,
                    downstream_node_id: obs.node_id.clone(),
                    fingerprint: obs.resource_fingerprint.clone(),
                    source: EdgeSource::Observed,
                });
            }
        }
    }

    edges
}

/// Derive static edges by matching sink fingerprints to source fingerprints.
///
/// This mirrors [`LineageGraph::derive_static_edges`] but operates on bindings
/// we already have from the store, rather than computing from pipelines.
fn derive_edges(bindings: &[ResourceBinding]) -> Vec<LineageEdge> {
    use std::collections::HashMap;

    let mut sinks_by_fp: HashMap<&ResourceFingerprint, Vec<&ResourceBinding>> = HashMap::new();
    for b in bindings {
        if b.direction == BindingDirection::Sink {
            sinks_by_fp.entry(&b.fingerprint).or_default().push(b);
        }
    }

    let mut edges = Vec::new();
    for b in bindings {
        if b.direction != BindingDirection::Source {
            continue;
        }
        if let Some(sinks) = sinks_by_fp.get(&b.fingerprint) {
            for sink in sinks {
                if sink.pipeline_id == b.pipeline_id && sink.node_id == b.node_id {
                    continue;
                }
                edges.push(LineageEdge {
                    upstream_pipeline_id: sink.pipeline_id.clone(),
                    upstream_node_id: sink.node_id.clone(),
                    downstream_pipeline_id: b.pipeline_id.clone(),
                    downstream_node_id: b.node_id.clone(),
                    fingerprint: b.fingerprint.clone(),
                    source: EdgeSource::Static,
                });
            }
        }
    }

    edges
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct GraphResponse {
    pub pipelines: Vec<String>,
    pub edges: Vec<EdgeDto>,
    pub bindings: Vec<BindingDto>,
}

#[derive(Debug, Serialize)]
pub struct EdgeDto {
    pub upstream_pipeline_id: String,
    pub upstream_node_id: String,
    pub downstream_pipeline_id: String,
    pub downstream_node_id: String,
    pub resource: String,
    pub source: EdgeSource,
}

#[derive(Debug, Serialize)]
pub struct BindingDto {
    pub pipeline_id: String,
    pub node_id: String,
    pub direction: BindingDirection,
    pub resource: String,
}

#[derive(Debug, Serialize)]
pub struct UpstreamDownstreamResponse {
    pub pipeline_id: String,
    /// Direct edges (immediate upstream or downstream).
    pub direct: Vec<EdgeDto>,
    /// All transitively related pipeline IDs.
    pub transitive: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ImpactResponse {
    pub pipeline_id: String,
    /// Downstream pipeline IDs affected if any sink in this pipeline changes.
    pub affected_pipelines: Vec<String>,
    /// The edges from this pipeline to direct downstream consumers.
    pub direct_edges: Vec<EdgeDto>,
}

#[derive(Debug, Serialize)]
pub struct CyclesResponse {
    pub cycles: Vec<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct OrphansResponse {
    /// Sources that read from resources no known sink writes to.
    pub dangling_sources: Vec<BindingDto>,
    /// Sinks that write to resources no known source reads from.
    pub orphaned_sinks: Vec<BindingDto>,
}

fn edge_dto(e: &LineageEdge) -> EdgeDto {
    EdgeDto {
        upstream_pipeline_id: e.upstream_pipeline_id.to_string(),
        upstream_node_id: e.upstream_node_id.clone(),
        downstream_pipeline_id: e.downstream_pipeline_id.to_string(),
        downstream_node_id: e.downstream_node_id.clone(),
        resource: e.fingerprint.to_string(),
        source: e.source,
    }
}

fn binding_dto(b: &ResourceBinding) -> BindingDto {
    BindingDto {
        pipeline_id: b.pipeline_id.to_string(),
        node_id: b.node_id.clone(),
        direction: b.direction,
        resource: b.fingerprint.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /api/lineage/graph` — full project lineage graph.
async fn lineage_graph(
    State(state): State<AppState>,
    Query(q): Query<EnvQuery>,
) -> Result<Json<GraphResponse>, (StatusCode, Json<ApiError>)> {
    let graph = build_graph(&state, &q.environment)?;
    let pipelines: Vec<String> = graph
        .pipeline_ids()
        .into_iter()
        .map(|id| id.to_string())
        .collect();

    Ok(Json(GraphResponse {
        pipelines,
        edges: graph.edges.iter().map(edge_dto).collect(),
        bindings: graph.bindings.iter().map(binding_dto).collect(),
    }))
}

/// `GET /api/lineage/pipelines/:id/upstream` — upstream pipelines.
async fn upstream(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<EnvQuery>,
) -> Result<Json<UpstreamDownstreamResponse>, (StatusCode, Json<ApiError>)> {
    let pipeline_id = id
        .parse::<PipelineId>()
        .map_err(|_| ApiError::bad_request(format!("invalid pipeline ID: {id}")))?;

    let graph = build_graph(&state, &q.environment)?;
    let direct = graph.upstream_of(&pipeline_id);
    let transitive = graph.all_upstream(&pipeline_id);

    Ok(Json(UpstreamDownstreamResponse {
        pipeline_id: id,
        direct: direct.iter().map(|e| edge_dto(e)).collect(),
        transitive: transitive.into_iter().map(|id| id.to_string()).collect(),
    }))
}

/// `GET /api/lineage/pipelines/:id/downstream` — downstream pipelines.
async fn downstream(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<EnvQuery>,
) -> Result<Json<UpstreamDownstreamResponse>, (StatusCode, Json<ApiError>)> {
    let pipeline_id = id
        .parse::<PipelineId>()
        .map_err(|_| ApiError::bad_request(format!("invalid pipeline ID: {id}")))?;

    let graph = build_graph(&state, &q.environment)?;
    let direct = graph.downstream_of(&pipeline_id);
    let transitive = graph.all_downstream(&pipeline_id);

    Ok(Json(UpstreamDownstreamResponse {
        pipeline_id: id,
        direct: direct.iter().map(|e| edge_dto(e)).collect(),
        transitive: transitive.into_iter().map(|id| id.to_string()).collect(),
    }))
}

/// `GET /api/lineage/pipelines/:id/impact` — impact analysis.
///
/// Returns all downstream pipelines that would be affected if any sink in this
/// pipeline changed, along with the direct edges connecting them.
async fn impact(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<EnvQuery>,
) -> Result<Json<ImpactResponse>, (StatusCode, Json<ApiError>)> {
    let pipeline_id = id
        .parse::<PipelineId>()
        .map_err(|_| ApiError::bad_request(format!("invalid pipeline ID: {id}")))?;

    let graph = build_graph(&state, &q.environment)?;
    let direct_edges = graph.downstream_of(&pipeline_id);
    let all_downstream = graph.all_downstream(&pipeline_id);

    Ok(Json(ImpactResponse {
        pipeline_id: id,
        affected_pipelines: all_downstream
            .into_iter()
            .map(|id| id.to_string())
            .collect(),
        direct_edges: direct_edges.iter().map(|e| edge_dto(e)).collect(),
    }))
}

/// `GET /api/lineage/cycles` — detected cycles across the project.
async fn cycles(
    State(state): State<AppState>,
    Query(q): Query<EnvQuery>,
) -> Result<Json<CyclesResponse>, (StatusCode, Json<ApiError>)> {
    let graph = build_graph(&state, &q.environment)?;
    let detected = graph.detect_cycles();

    Ok(Json(CyclesResponse {
        cycles: detected
            .into_iter()
            .map(|cycle| cycle.into_iter().map(|id| id.to_string()).collect())
            .collect(),
    }))
}

/// `GET /api/lineage/orphans` — dangling references and unused outputs.
async fn orphans(
    State(state): State<AppState>,
    Query(q): Query<EnvQuery>,
) -> Result<Json<OrphansResponse>, (StatusCode, Json<ApiError>)> {
    let graph = build_graph(&state, &q.environment)?;

    Ok(Json(OrphansResponse {
        dangling_sources: graph
            .dangling_sources()
            .iter()
            .map(|b| binding_dto(b))
            .collect(),
        orphaned_sinks: graph
            .orphaned_sinks()
            .iter()
            .map(|b| binding_dto(b))
            .collect(),
    }))
}

// ---------------------------------------------------------------------------
// Column-level lineage helpers (planning doc 35)
// ---------------------------------------------------------------------------

/// Query parameters for column lineage endpoints.
#[derive(Debug, Deserialize)]
pub struct ColumnLineageQuery {
    #[serde(default = "default_env")]
    pub environment: String,
    /// Maximum traversal depth (default: 10).
    #[serde(default = "default_max_depth")]
    pub max_depth: Option<usize>,
    /// Comma-separated relationship kinds to include (empty = all).
    pub relationships: Option<String>,
    /// Comma-separated confidence levels to include (empty = all).
    pub confidence: Option<String>,
}

fn default_max_depth() -> Option<usize> {
    Some(10)
}

/// Column search query parameters.
#[derive(Debug, Deserialize)]
pub struct ColumnSearchQuery {
    #[serde(default = "default_env")]
    pub environment: String,
    /// Substring to search for in column names.
    pub query: String,
}

/// Build a [`ColumnLineageGraph`] from stored column edges and cross-pipeline
/// edges for the given environment.
fn build_column_graph(
    state: &AppState,
    environment: &str,
) -> Result<ColumnLineageGraph, (StatusCode, Json<ApiError>)> {
    let store = state
        .column_lineage_store
        .as_ref()
        .ok_or_else(|| ApiError::internal("column lineage storage not available"))?;

    let stored_edges = store
        .all_column_edges(environment)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    // Group edges by pipeline_id for the graph constructor.
    let mut by_pipeline: std::collections::HashMap<PipelineId, Vec<flux_engine::ColumnEdge>> =
        std::collections::HashMap::new();
    let mut boundary_columns = Vec::new();

    for se in &stored_edges {
        let pipeline_id = se
            .pipeline_id
            .parse::<PipelineId>()
            .map_err(|_| ApiError::internal(format!("invalid pipeline ID: {}", se.pipeline_id)))?;
        by_pipeline
            .entry(pipeline_id.clone())
            .or_default()
            .push(se.edge.clone());

        // Collect boundary columns for cross-pipeline derivation.
        if let Some(ref fp) = se.edge.downstream_resource {
            if let Some(ref node) = se.edge.downstream_node {
                boundary_columns.push(BoundaryColumn {
                    pipeline_id: pipeline_id.clone(),
                    node_id: node.clone(),
                    column: se.edge.downstream_column.clone(),
                    fingerprint: fp.clone(),
                    direction: BindingDirection::Sink,
                });
            }
        }
        if let Some(ref fp) = se.edge.upstream_resource {
            if let Some(ref node) = se.edge.upstream_node {
                boundary_columns.push(BoundaryColumn {
                    pipeline_id,
                    node_id: node.clone(),
                    column: se.edge.upstream_column.clone(),
                    fingerprint: fp.clone(),
                    direction: BindingDirection::Source,
                });
            }
        }
    }

    let pipeline_edges: Vec<(PipelineId, Vec<flux_engine::ColumnEdge>)> =
        by_pipeline.into_iter().collect();
    let pipeline_edge_refs: Vec<(PipelineId, &[flux_engine::ColumnEdge])> = pipeline_edges
        .iter()
        .map(|(id, edges)| (id.clone(), edges.as_slice()))
        .collect();

    let cross_pipeline = derive_cross_pipeline_column_lineage(&boundary_columns);

    Ok(ColumnLineageGraph::new(
        &pipeline_edge_refs,
        &cross_pipeline.edges,
    ))
}

/// Parse a [`ColumnLineageQuery`] into [`TraceOptions`].
fn trace_options(q: &ColumnLineageQuery) -> TraceOptions {
    let mut opts = TraceOptions {
        max_depth: q.max_depth.unwrap_or(10),
        ..TraceOptions::default()
    };
    if let Some(ref rels) = q.relationships {
        opts.relationship_filter = rels
            .split(',')
            .filter_map(|s| parse_relationship(s.trim()))
            .collect();
    }
    if let Some(ref conf) = q.confidence {
        opts.confidence_filter = conf
            .split(',')
            .filter_map(|s| parse_confidence(s.trim()))
            .collect();
    }
    opts
}

fn parse_relationship(s: &str) -> Option<RelationshipKind> {
    match s {
        "direct" => Some(RelationshipKind::Direct),
        "derived" => Some(RelationshipKind::Derived),
        "cast" => Some(RelationshipKind::Cast),
        "filter" => Some(RelationshipKind::Filter),
        "join_key" => Some(RelationshipKind::JoinKey),
        "join_passthrough" => Some(RelationshipKind::JoinPassthrough),
        "group_by" => Some(RelationshipKind::GroupBy),
        "aggregate_input" => Some(RelationshipKind::AggregateInput),
        "window_partition" => Some(RelationshipKind::WindowPartition),
        "window_order" => Some(RelationshipKind::WindowOrder),
        "window_input" => Some(RelationshipKind::WindowInput),
        "opaque" => Some(RelationshipKind::Opaque),
        _ => None,
    }
}

fn parse_confidence(s: &str) -> Option<Confidence> {
    match s {
        "exact" => Some(Confidence::Exact),
        "lazyframe" => Some(Confidence::LazyFrame),
        "annotation" => Some(Confidence::Annotation),
        "opaque" => Some(Confidence::Opaque),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Column-level lineage response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct ColumnKeyDto {
    pub pipeline_id: String,
    pub node_id: String,
    pub column: String,
}

impl From<&ColumnKey> for ColumnKeyDto {
    fn from(k: &ColumnKey) -> Self {
        Self {
            pipeline_id: k.pipeline_id.to_string(),
            node_id: k.node_id.0.clone(),
            column: k.column.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TraceEdgeDto {
    pub upstream: ColumnKeyDto,
    pub downstream: ColumnKeyDto,
    pub relationship: RelationshipKind,
    pub confidence: Confidence,
    pub expression_text: Option<String>,
    pub depth: usize,
}

#[derive(Debug, Serialize)]
pub struct ColumnTraceResponse {
    pub fingerprint: String,
    pub column: String,
    pub edges: Vec<TraceEdgeDto>,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct ColumnImpactResponse {
    pub fingerprint: String,
    pub column: String,
    /// Downstream columns grouped by pipeline.
    pub affected_columns: Vec<TraceEdgeDto>,
    /// Distinct downstream pipeline IDs.
    pub affected_pipelines: Vec<String>,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct ColumnSearchResponse {
    pub query: String,
    pub results: Vec<ColumnKeyDto>,
}

fn trace_edges_to_dtos(edges: &[flux_engine::column_lineage::TraceEdge]) -> Vec<TraceEdgeDto> {
    edges
        .iter()
        .map(|e| TraceEdgeDto {
            upstream: ColumnKeyDto::from(&e.upstream),
            downstream: ColumnKeyDto::from(&e.downstream),
            relationship: e.relationship,
            confidence: e.confidence,
            expression_text: e.expression_text.clone(),
            depth: e.depth,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Column-level lineage handlers
// ---------------------------------------------------------------------------

/// Path parameters for column lineage endpoints.
#[derive(Debug, Deserialize)]
pub struct ColumnPath {
    pub fingerprint: String,
    pub column: String,
}

/// `GET /api/lineage/columns/:fingerprint/:column/upstream` — column upstream trace.
async fn column_upstream(
    State(state): State<AppState>,
    Path(path): Path<ColumnPath>,
    Query(q): Query<ColumnLineageQuery>,
) -> Result<Json<ColumnTraceResponse>, (StatusCode, Json<ApiError>)> {
    let graph = build_column_graph(&state, &q.environment)?;
    let fp = ResourceFingerprint::new(&path.fingerprint);
    let keys = graph.resolve_by_fingerprint(&fp, &path.column);

    if keys.is_empty() {
        return Ok(Json(ColumnTraceResponse {
            fingerprint: path.fingerprint,
            column: path.column,
            edges: Vec::new(),
            truncated: false,
        }));
    }

    let opts = trace_options(&q);
    let mut all_edges = Vec::new();
    let mut truncated = false;
    for key in &keys {
        let result = graph.upstream_trace(key, &opts);
        all_edges.extend(result.edges);
        truncated |= result.truncated;
    }

    Ok(Json(ColumnTraceResponse {
        fingerprint: path.fingerprint,
        column: path.column,
        edges: trace_edges_to_dtos(&all_edges),
        truncated,
    }))
}

/// `GET /api/lineage/columns/:fingerprint/:column/downstream` — column downstream trace.
async fn column_downstream(
    State(state): State<AppState>,
    Path(path): Path<ColumnPath>,
    Query(q): Query<ColumnLineageQuery>,
) -> Result<Json<ColumnTraceResponse>, (StatusCode, Json<ApiError>)> {
    let graph = build_column_graph(&state, &q.environment)?;
    let fp = ResourceFingerprint::new(&path.fingerprint);
    let keys = graph.resolve_by_fingerprint(&fp, &path.column);

    if keys.is_empty() {
        return Ok(Json(ColumnTraceResponse {
            fingerprint: path.fingerprint,
            column: path.column,
            edges: Vec::new(),
            truncated: false,
        }));
    }

    let opts = trace_options(&q);
    let mut all_edges = Vec::new();
    let mut truncated = false;
    for key in &keys {
        let result = graph.downstream_trace(key, &opts);
        all_edges.extend(result.edges);
        truncated |= result.truncated;
    }

    Ok(Json(ColumnTraceResponse {
        fingerprint: path.fingerprint,
        column: path.column,
        edges: trace_edges_to_dtos(&all_edges),
        truncated,
    }))
}

/// `GET /api/lineage/columns/:fingerprint/:column/impact` — column impact analysis.
///
/// Returns all downstream columns that transitively depend on this one,
/// grouped by pipeline.
async fn column_impact(
    State(state): State<AppState>,
    Path(path): Path<ColumnPath>,
    Query(q): Query<ColumnLineageQuery>,
) -> Result<Json<ColumnImpactResponse>, (StatusCode, Json<ApiError>)> {
    let graph = build_column_graph(&state, &q.environment)?;
    let fp = ResourceFingerprint::new(&path.fingerprint);
    let keys = graph.resolve_by_fingerprint(&fp, &path.column);

    if keys.is_empty() {
        return Ok(Json(ColumnImpactResponse {
            fingerprint: path.fingerprint,
            column: path.column,
            affected_columns: Vec::new(),
            affected_pipelines: Vec::new(),
            truncated: false,
        }));
    }

    let opts = trace_options(&q);
    let mut all_edges = Vec::new();
    let mut truncated = false;
    let mut affected_pipeline_set: HashSet<String> = HashSet::new();

    for key in &keys {
        let result = graph.downstream_trace(key, &opts);
        for edge in &result.edges {
            affected_pipeline_set.insert(edge.downstream.pipeline_id.to_string());
        }
        all_edges.extend(result.edges);
        truncated |= result.truncated;
    }

    let mut affected_pipelines: Vec<String> = affected_pipeline_set.into_iter().collect();
    affected_pipelines.sort();

    Ok(Json(ColumnImpactResponse {
        fingerprint: path.fingerprint,
        column: path.column,
        affected_columns: trace_edges_to_dtos(&all_edges),
        affected_pipelines,
        truncated,
    }))
}

/// `GET /api/lineage/columns/search?query=...` — search columns by name.
async fn column_search(
    State(state): State<AppState>,
    Query(q): Query<ColumnSearchQuery>,
) -> Result<Json<ColumnSearchResponse>, (StatusCode, Json<ApiError>)> {
    let graph = build_column_graph(&state, &q.environment)?;
    let query_lower = q.query.to_lowercase();

    let mut results: Vec<ColumnKeyDto> = graph
        .all_columns()
        .into_iter()
        .filter(|k| k.column.to_lowercase().contains(&query_lower))
        .map(ColumnKeyDto::from)
        .collect();

    // Deduplicate by (pipeline_id, node_id, column).
    results.sort_by(|a, b| {
        a.pipeline_id
            .cmp(&b.pipeline_id)
            .then_with(|| a.node_id.cmp(&b.node_id))
            .then_with(|| a.column.cmp(&b.column))
    });
    results.dedup_by(|a, b| {
        a.pipeline_id == b.pipeline_id && a.node_id == b.node_id && a.column == b.column
    });

    Ok(Json(ColumnSearchResponse {
        query: q.query,
        results,
    }))
}
