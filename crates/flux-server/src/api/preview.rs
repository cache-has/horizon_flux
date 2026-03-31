// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Single-node preview API route.
//!
//! Used by the modal editor for live, debounced preview of a single node's
//! output given its configuration and (for transforms) upstream data.

use crate::api::ApiError;
use crate::api::pipelines::{ColumnInfo, PreviewNodeResponse, batches_to_json_rows};
use flux_datafusion::compute_column_stats;
use crate::state::AppState;
use arrow::json::ReaderBuilder;
use arrow::record_batch::RecordBatch;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use flux_datafusion::PipelineExecutor;
use flux_datafusion::preview::sample_batches;
use flux_engine::NodeId;
use flux_engine::node::{SourceConfig, TransformMode};
use flux_engine::sample::SampleConfig;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error};

/// Timeout for single-node preview execution.
const PREVIEW_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Build the `/preview` sub-router.
pub fn router() -> Router<AppState> {
    Router::new().route("/node", post(preview_node))
}

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Request body for single-node preview.
#[derive(Debug, Deserialize)]
struct NodePreviewRequest {
    /// The node configuration to preview.
    node: NodeConfig,
    /// Upstream data keyed by node ID (required for transform nodes).
    /// Each value is an array of JSON row objects.
    #[serde(default)]
    upstream: HashMap<String, Vec<serde_json::Value>>,
    /// Optional sampling configuration (applied to source nodes).
    #[serde(default)]
    sample: Option<SampleConfig>,
}

/// Discriminated node configuration sent by the frontend.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum NodeConfig {
    Source {
        connector: String,
        #[serde(default)]
        config: serde_json::Value,
    },
    Transform {
        mode: TransformMode,
        #[serde(default)]
        code: String,
    },
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `POST /api/preview/node` — preview a single node's output.
async fn preview_node(
    State(state): State<AppState>,
    Json(req): Json<NodePreviewRequest>,
) -> Result<Json<PreviewNodeResponse>, (StatusCode, Json<ApiError>)> {
    let start = Instant::now();

    match req.node {
        NodeConfig::Source { connector, config } => {
            let registry = state.connector_registry.to_provider_registry();
            let src_cfg = SourceConfig {
                connector: connector.clone(),
                config,
                cache_row_limit: None,
            };
            let node_id = NodeId::new("preview");

            debug!(connector = %connector, "previewing source node");

            let batches = tokio::time::timeout(
                PREVIEW_TIMEOUT,
                PipelineExecutor::execute_source(&node_id, &src_cfg, &registry),
            )
            .await
            .map_err(|_| ApiError::gateway_timeout("preview timed out after 5 seconds"))?
            .map_err(|e| {
                error!(connector = %connector, error = %e, "source preview failed");
                ApiError::internal(e.to_string())
            })?;

            let sampled = sample_batches(batches, &req.sample.unwrap_or_default());
            build_preview_response("preview", &sampled, start)
        }

        NodeConfig::Transform { mode, code } => {
            if req.upstream.is_empty() {
                return Err(ApiError::bad_request(
                    "transform preview requires at least one upstream dataset",
                ));
            }

            // Convert JSON row objects → Arrow RecordBatches.
            let mut upstream_batches: HashMap<NodeId, Vec<RecordBatch>> = HashMap::new();
            for (name, rows) in &req.upstream {
                let batches = json_rows_to_batches(rows)
                    .map_err(|e| ApiError::bad_request(format!("upstream `{name}`: {e}")))?;
                upstream_batches.insert(NodeId::new(name), batches);
            }

            // Build the upstream reference map that execute_sql_transform expects.
            let upstream_refs: HashMap<NodeId, &Vec<RecordBatch>> = upstream_batches
                .iter()
                .map(|(k, v)| (k.clone(), v))
                .collect();

            debug!(mode = ?mode, upstreams = upstream_refs.len(), "previewing transform node");

            let batches = tokio::time::timeout(PREVIEW_TIMEOUT, async {
                match mode {
                    TransformMode::Sql => {
                        PipelineExecutor::execute_sql_transform(&code, upstream_refs, None)
                            .await
                            .map_err(|e| {
                                error!(error = %e, "SQL transform preview failed");
                                ApiError::internal(e.to_string())
                            })
                    }
                    TransformMode::Python => {
                        let variables = HashMap::new();
                        flux_datafusion::python_runtime::execute_python_transform(
                            &code,
                            upstream_refs,
                            &variables,
                        )
                        .await
                        .map_err(|e| {
                            error!(error = %e, "Python transform preview failed");
                            ApiError::internal(e.to_string())
                        })
                    }
                }
            })
            .await
            .map_err(|_| ApiError::gateway_timeout("preview timed out after 5 seconds"))??;

            build_preview_response("preview", &batches, start)
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a `PreviewNodeResponse` from Arrow batches.
fn build_preview_response(
    node_id: &str,
    batches: &[RecordBatch],
    start: Instant,
) -> Result<Json<PreviewNodeResponse>, (StatusCode, Json<ApiError>)> {
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let columns: Vec<ColumnInfo> = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| ColumnInfo {
                    name: f.name().clone(),
                    data_type: format!("{}", f.data_type()),
                    nullable: f.is_nullable(),
                })
                .collect()
        })
        .unwrap_or_default();

    let rows = batches_to_json_rows(batches);
    let column_stats = compute_column_stats(batches);

    Ok(Json(PreviewNodeResponse {
        node_id: node_id.to_string(),
        columns,
        row_count,
        duration_ms: start.elapsed().as_millis() as u64,
        rows,
        column_stats,
        status: flux_datafusion::PreviewStatus::ReExecuted,
    }))
}

/// Convert a slice of JSON row objects into Arrow `RecordBatch`es.
///
/// Uses Arrow's built-in JSON reader with schema inference.
fn json_rows_to_batches(rows: &[serde_json::Value]) -> Result<Vec<RecordBatch>, String> {
    if rows.is_empty() {
        return Ok(vec![]);
    }

    // Serialize rows as newline-delimited JSON for the Arrow reader.
    let mut buf = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut buf, row).map_err(|e| e.to_string())?;
        buf.push(b'\n');
    }

    // Infer schema from the JSON records.
    let (schema, _) =
        arrow::json::reader::infer_json_schema_from_seekable(&mut std::io::Cursor::new(&buf), None)
            .map_err(|e| format!("schema inference failed: {e}"))?;

    let reader = ReaderBuilder::new(Arc::new(schema))
        .build(std::io::Cursor::new(buf))
        .map_err(|e| format!("JSON reader creation failed: {e}"))?;

    let batches: Result<Vec<_>, _> = reader.collect();
    batches.map_err(|e| format!("JSON to Arrow conversion failed: {e}"))
}
