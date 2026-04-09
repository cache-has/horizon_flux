// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cross-pipeline lineage API routes (planning doc 31).
//!
//! Exposes the lineage graph, upstream/downstream queries, impact analysis,
//! cycle detection, and orphan/dangling-reference detection.

use crate::api::ApiError;
use crate::state::AppState;
use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use flux_engine::lineage::{
    BindingDirection, EdgeSource, LineageEdge, LineageGraph, ResourceBinding, ResourceFingerprint,
};
use flux_engine::pipeline_store::PipelineId;
use serde::{Deserialize, Serialize};

/// Build the `/lineage` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/graph", get(lineage_graph))
        .route("/pipelines/{id}/upstream", get(upstream))
        .route("/pipelines/{id}/downstream", get(downstream))
        .route("/pipelines/{id}/impact", get(impact))
        .route("/cycles", get(cycles))
        .route("/orphans", get(orphans))
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Optional environment query parameter (defaults to "default").
#[derive(Debug, Deserialize)]
pub struct EnvQuery {
    #[serde(default = "default_env")]
    pub environment: String,
}

fn default_env() -> String {
    "default".into()
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
