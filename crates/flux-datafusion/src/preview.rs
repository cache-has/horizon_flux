// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Preview execution — loads cached node outputs from prior pipeline runs and
//! samples them for display.
//!
//! Key design:
//! - Preview **never** executes the full pipeline — it reads from the output
//!   cache populated by prior runs.
//! - Nodes without cached data report `PreviewStatus::NoCache`.
//! - Sinks are always skipped (`PreviewStatus::Skipped`).
//! - An optional re-execute mode lets the user re-run a single node's
//!   SQL/Python against cached upstream outputs.

use crate::error::ExecutorError;
use crate::executor::PipelineExecutor;
use crate::output_cache::OutputCache;
use crate::provider::ProviderRegistry;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use flux_engine::node::NodeKind;
use flux_engine::sample::SampleConfig;
use flux_engine::variables::{BuiltinContext, ResolvedVariables};
use flux_engine::{NodeId, Pipeline, dag};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::run::ExecutionEvent;

/// Options for a preview execution.
pub struct PreviewOptions {
    /// How to sample cached data for display.
    pub sample: SampleConfig,
    /// Set to `true` from another thread/task to cancel the preview.
    pub cancel: Arc<AtomicBool>,
    /// Optional channel for real-time preview progress events.
    pub progress: Option<mpsc::UnboundedSender<ExecutionEvent>>,
    /// Runtime variable overrides for preview (used during re-execute).
    pub variable_overrides: HashMap<String, Value>,
    /// Optional: re-execute a single materialized node against cached upstream.
    pub re_execute_node: Option<NodeId>,
}

impl Default for PreviewOptions {
    fn default() -> Self {
        Self {
            sample: SampleConfig::default(),
            cancel: Arc::new(AtomicBool::new(false)),
            progress: None,
            variable_overrides: HashMap::new(),
            re_execute_node: None,
        }
    }
}

/// Status of a node in the preview result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreviewStatus {
    /// Node has cached data from a prior run.
    Cached,
    /// Node has no cached data — pipeline must be run first.
    NoCache,
    /// Node was skipped (e.g. sinks).
    Skipped,
    /// Node was re-executed against cached upstream data.
    ReExecuted,
}

/// Per-node preview output.
#[derive(Debug)]
pub struct PreviewNodeResult {
    pub node_id: NodeId,
    /// Output schema (column names and types).
    pub schema: SchemaRef,
    /// Sample output rows.
    pub batches: Vec<RecordBatch>,
    /// Total row count in the preview output.
    pub row_count: u64,
    /// How long this node took to load/execute.
    pub duration: Duration,
    /// Status of this node in the preview.
    pub status: PreviewStatus,
}

/// The result of a preview execution.
#[derive(Debug)]
pub struct PreviewResult {
    pub pipeline_name: String,
    pub sample_config: SampleConfig,
    /// Per-node results, keyed by node ID.
    pub nodes: HashMap<NodeId, PreviewNodeResult>,
    /// Execution order (topological).
    pub execution_order: Vec<NodeId>,
    /// Total wall-clock duration.
    pub duration: Duration,
}

impl PreviewResult {
    /// Get preview output for a specific node.
    pub fn node_output(&self, node_id: &NodeId) -> Option<&PreviewNodeResult> {
        self.nodes.get(node_id)
    }
}

impl PipelineExecutor {
    /// Run a preview by loading cached node outputs.
    ///
    /// Validates the DAG and walks nodes in topological order, loading cached
    /// data for each node and sampling it for display. Nodes without cached
    /// data are reported as `PreviewStatus::NoCache`. Sinks are always skipped.
    ///
    /// If `options.re_execute_node` is set, that single node is re-executed
    /// against cached upstream data instead of loading its own cache.
    pub async fn preview(
        pipeline: &Pipeline,
        cache: &OutputCache,
        registry: &ProviderRegistry,
        options: &PreviewOptions,
    ) -> Result<PreviewResult, ExecutorError> {
        dag::validate(pipeline).map_err(ExecutorError::Validation)?;
        let order = dag::topological_sort(pipeline);

        info!(
            pipeline = %pipeline.name,
            nodes = order.len(),
            sample = ?options.sample,
            "starting preview (cache-based)"
        );

        let start = Instant::now();
        let mut node_results: HashMap<NodeId, PreviewNodeResult> = HashMap::new();

        for node_id in &order {
            if options.cancel.load(Ordering::Relaxed) {
                return Err(ExecutorError::Cancelled);
            }

            let node = pipeline
                .node(node_id)
                .expect("topological_sort returned an ID not in the pipeline");

            let node_start = Instant::now();

            // Sinks are always skipped — they write, not read.
            if node.kind.is_sink() {
                node_results.insert(
                    node_id.clone(),
                    PreviewNodeResult {
                        node_id: node_id.clone(),
                        schema: Arc::new(arrow::datatypes::Schema::empty()),
                        batches: Vec::new(),
                        row_count: 0,
                        duration: node_start.elapsed(),
                        status: PreviewStatus::Skipped,
                    },
                );
                continue;
            }

            // Check if this node should be re-executed against cached upstream.
            let is_re_execute = options
                .re_execute_node
                .as_ref()
                .is_some_and(|id| id == node_id);

            if is_re_execute {
                let result =
                    Self::re_execute_node(pipeline, cache, registry, node_id, options, node_start)
                        .await?;
                node_results.insert(node_id.clone(), result);
                continue;
            }

            // Normal path: load from cache.
            match cache.read_node(&pipeline.name, &node_id.0) {
                Ok(Some(batches)) => {
                    let sampled = sample_batches(batches, &options.sample);
                    let row_count: u64 = sampled.iter().map(|b| b.num_rows() as u64).sum();
                    let schema = schema_from_batches(&sampled);

                    debug!(node = %node_id, rows = row_count, "preview loaded from cache");

                    node_results.insert(
                        node_id.clone(),
                        PreviewNodeResult {
                            node_id: node_id.clone(),
                            schema,
                            batches: sampled,
                            row_count,
                            duration: node_start.elapsed(),
                            status: PreviewStatus::Cached,
                        },
                    );
                }
                Ok(None) => {
                    debug!(node = %node_id, "no cache available");
                    node_results.insert(
                        node_id.clone(),
                        PreviewNodeResult {
                            node_id: node_id.clone(),
                            schema: Arc::new(arrow::datatypes::Schema::empty()),
                            batches: Vec::new(),
                            row_count: 0,
                            duration: node_start.elapsed(),
                            status: PreviewStatus::NoCache,
                        },
                    );
                }
                Err(e) => {
                    warn!(node = %node_id, error = %e, "failed to read cached output");
                    node_results.insert(
                        node_id.clone(),
                        PreviewNodeResult {
                            node_id: node_id.clone(),
                            schema: Arc::new(arrow::datatypes::Schema::empty()),
                            batches: Vec::new(),
                            row_count: 0,
                            duration: node_start.elapsed(),
                            status: PreviewStatus::NoCache,
                        },
                    );
                }
            }
        }

        let total_duration = start.elapsed();

        info!(
            pipeline = %pipeline.name,
            duration_ms = total_duration.as_millis(),
            "preview complete"
        );

        Ok(PreviewResult {
            pipeline_name: pipeline.name.clone(),
            sample_config: options.sample.clone(),
            nodes: node_results,
            execution_order: order,
            duration: total_duration,
        })
    }

    /// Re-execute a single node against cached upstream data.
    ///
    /// Loads each upstream node's cached output, then runs the target node's
    /// SQL/Python against that data. Returns the sampled result without
    /// persisting it to the cache.
    async fn re_execute_node(
        pipeline: &Pipeline,
        cache: &OutputCache,
        registry: &ProviderRegistry,
        node_id: &NodeId,
        options: &PreviewOptions,
        node_start: Instant,
    ) -> Result<PreviewNodeResult, ExecutorError> {
        let node = pipeline.node(node_id).expect("node must exist");

        match &node.kind {
            NodeKind::Transform(xform_cfg) => {
                // Resolve code.
                let code = pipeline
                    .resolve_code(xform_cfg)
                    .map_err(|e| ExecutorError::Node {
                        node_id: node_id.clone(),
                        kind: crate::error::NodeErrorKind::CodeFileRead {
                            path: xform_cfg
                                .code_path
                                .clone()
                                .unwrap_or_else(|| "(inline)".into()),
                            source: e,
                        },
                    })?;

                // Resolve variables.
                let builtin = BuiltinContext {
                    run_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                    run_id: uuid::Uuid::new_v4().to_string(),
                    pipeline_name: pipeline.name.clone(),
                    environment: pipeline.default_environment.clone(),
                };
                let resolved_vars =
                    ResolvedVariables::resolve(pipeline, &options.variable_overrides, &builtin);

                // Load cached upstream outputs.
                let upstream_ids = pipeline.upstream_of(node_id);
                let mut upstream_batches: HashMap<NodeId, Vec<RecordBatch>> = HashMap::new();
                for uid in &upstream_ids {
                    match cache.read_node(&pipeline.name, &uid.0) {
                        Ok(Some(batches)) => {
                            upstream_batches.insert((*uid).clone(), batches);
                        }
                        _ => {
                            return Ok(PreviewNodeResult {
                                node_id: node_id.clone(),
                                schema: Arc::new(arrow::datatypes::Schema::empty()),
                                batches: Vec::new(),
                                row_count: 0,
                                duration: node_start.elapsed(),
                                status: PreviewStatus::NoCache,
                            });
                        }
                    }
                }

                // Build upstream data references.
                let upstream_data: HashMap<NodeId, &Vec<RecordBatch>> = upstream_batches
                    .iter()
                    .map(|(k, v)| (k.clone(), v))
                    .collect();

                let batches = match xform_cfg.mode {
                    flux_engine::node::TransformMode::Sql => {
                        let interpolated_sql = resolved_vars.interpolate(&code);
                        Self::execute_sql_transform(&interpolated_sql, upstream_data, None, None)
                            .await
                            .map_err(|kind| ExecutorError::Node {
                                node_id: node_id.clone(),
                                kind,
                            })?
                    }
                    flux_engine::node::TransformMode::Python => {
                        let py_config = crate::python_runtime::PythonConfig::default();
                        crate::python_runtime::execute_python_transform(
                            &code,
                            upstream_data,
                            resolved_vars.as_map(),
                            &py_config,
                        )
                        .await
                        .map_err(|kind| ExecutorError::Node {
                            node_id: node_id.clone(),
                            kind,
                        })?
                    }
                };

                let sampled = sample_batches(batches, &options.sample);
                let row_count: u64 = sampled.iter().map(|b| b.num_rows() as u64).sum();
                let schema = schema_from_batches(&sampled);

                debug!(node = %node_id, rows = row_count, "re-executed transform against cached upstream");

                Ok(PreviewNodeResult {
                    node_id: node_id.clone(),
                    schema,
                    batches: sampled,
                    row_count,
                    duration: node_start.elapsed(),
                    status: PreviewStatus::ReExecuted,
                })
            }
            NodeKind::Source(src_cfg) => {
                // Re-execute a source: run the actual source query.
                let mut interpolated_cfg = src_cfg.clone();
                let builtin = BuiltinContext {
                    run_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                    run_id: uuid::Uuid::new_v4().to_string(),
                    pipeline_name: pipeline.name.clone(),
                    environment: pipeline.default_environment.clone(),
                };
                let resolved_vars =
                    ResolvedVariables::resolve(pipeline, &options.variable_overrides, &builtin);
                interpolated_cfg.config = resolved_vars.interpolate_json(&interpolated_cfg.config);

                let batches =
                    Self::execute_source(node_id, &interpolated_cfg, registry, None, None)
                        .await
                        .map_err(|kind| ExecutorError::Node {
                            node_id: node_id.clone(),
                            kind,
                        })?;

                let sampled = sample_batches(batches, &options.sample);
                let row_count: u64 = sampled.iter().map(|b| b.num_rows() as u64).sum();
                let schema = schema_from_batches(&sampled);

                Ok(PreviewNodeResult {
                    node_id: node_id.clone(),
                    schema,
                    batches: sampled,
                    row_count,
                    duration: node_start.elapsed(),
                    status: PreviewStatus::ReExecuted,
                })
            }
            NodeKind::Sink(_) => Ok(PreviewNodeResult {
                node_id: node_id.clone(),
                schema: Arc::new(arrow::datatypes::Schema::empty()),
                batches: Vec::new(),
                row_count: 0,
                duration: node_start.elapsed(),
                status: PreviewStatus::Skipped,
            }),
        }
    }
}

/// Extract schema from batches, returning an empty schema if no batches exist.
fn schema_from_batches(batches: &[RecordBatch]) -> SchemaRef {
    batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()))
}

// ---------------------------------------------------------------------------
// Sampling helpers (operate on Arrow RecordBatches)
// ---------------------------------------------------------------------------

/// Apply a [`SampleConfig`] to a set of record batches.
pub fn sample_batches(batches: Vec<RecordBatch>, config: &SampleConfig) -> Vec<RecordBatch> {
    match config {
        SampleConfig::Full => batches,
        SampleConfig::FirstN { count } => take_first_n(batches, *count),
        SampleConfig::Random { count, seed } => take_random(batches, *count, *seed),
    }
}

fn take_first_n(batches: Vec<RecordBatch>, limit: usize) -> Vec<RecordBatch> {
    let mut remaining = limit;
    let mut result = Vec::new();

    for batch in batches {
        if remaining == 0 {
            break;
        }
        let rows = batch.num_rows();
        if rows <= remaining {
            remaining -= rows;
            result.push(batch);
        } else {
            result.push(batch.slice(0, remaining));
            remaining = 0;
        }
    }

    result
}

fn take_random(batches: Vec<RecordBatch>, limit: usize, seed: u64) -> Vec<RecordBatch> {
    if batches.is_empty() {
        return batches;
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows <= limit {
        return batches;
    }

    let schema = batches[0].schema();

    // Build a shuffled index array using xorshift64 for reproducibility.
    let mut indices: Vec<usize> = (0..total_rows).collect();
    let mut rng_state = seed;
    for i in (1..indices.len()).rev() {
        rng_state = xorshift64(rng_state);
        let j = (rng_state as usize) % (i + 1);
        indices.swap(i, j);
    }
    indices.truncate(limit);
    indices.sort_unstable();

    let concatenated = arrow::compute::concat_batches(&schema, &batches)
        .expect("concat_batches should succeed for same-schema batches");

    // Slice contiguous ranges for efficiency.
    let mut result = Vec::new();
    let mut range_start = indices[0];
    let mut range_end = indices[0];

    for &idx in &indices[1..] {
        if idx == range_end + 1 {
            range_end = idx;
        } else {
            result.push(concatenated.slice(range_start, range_end - range_start + 1));
            range_start = idx;
            range_end = idx;
        }
    }
    result.push(concatenated.slice(range_start, range_end - range_start + 1));

    result
}

fn xorshift64(mut state: u64) -> u64 {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}
