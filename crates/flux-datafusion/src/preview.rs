// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Preview execution — runs a pipeline on sampled source data and collects
//! per-node outputs for the frontend to display.
//!
//! Key differences from full execution:
//! - Source data is sampled (first N rows, random, or full)
//! - Sink nodes are **skipped** (no side effects)
//! - All intermediate node outputs are retained for inspection
//! - No run history is persisted

use crate::error::ExecutorError;
use crate::executor::PipelineExecutor;
use crate::provider::ProviderRegistry;
use crate::run::ExecutionEvent;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use flux_engine::node::NodeKind;
use flux_engine::sample::SampleConfig;
use flux_engine::{NodeId, Pipeline, dag};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info};

/// Options for a preview execution.
pub struct PreviewOptions {
    /// How to sample source data.
    pub sample: SampleConfig,
    /// Set to `true` from another thread/task to cancel the preview.
    pub cancel: Arc<AtomicBool>,
    /// Optional channel for real-time preview progress events.
    pub progress: Option<mpsc::UnboundedSender<ExecutionEvent>>,
}

impl Default for PreviewOptions {
    fn default() -> Self {
        Self {
            sample: SampleConfig::default(),
            cancel: Arc::new(AtomicBool::new(false)),
            progress: None,
        }
    }
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
    /// How long this node took to execute.
    pub duration: Duration,
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
    /// Run a preview execution of the pipeline.
    ///
    /// Validates the DAG, walks nodes in topological order, samples source
    /// data, executes transforms, and skips sinks. Returns per-node outputs
    /// with schema and sample rows.
    pub async fn preview(
        pipeline: &Pipeline,
        registry: &ProviderRegistry,
        options: &PreviewOptions,
    ) -> Result<PreviewResult, ExecutorError> {
        dag::validate(pipeline).map_err(ExecutorError::Validation)?;
        let order = dag::topological_sort(pipeline);

        info!(
            pipeline = %pipeline.name,
            nodes = order.len(),
            sample = ?options.sample,
            "starting preview execution"
        );

        let start = Instant::now();
        let mut outputs: HashMap<NodeId, Vec<RecordBatch>> = HashMap::new();
        let mut node_results: HashMap<NodeId, PreviewNodeResult> = HashMap::new();

        for node_id in &order {
            if options.cancel.load(Ordering::Relaxed) {
                return Err(ExecutorError::Cancelled);
            }

            let node = pipeline
                .node(node_id)
                .expect("topological_sort returned an ID not in the pipeline");

            let node_start = Instant::now();

            match &node.kind {
                NodeKind::Source(src_cfg) => {
                    let batches = Self::execute_source(node_id, src_cfg, registry)
                        .await
                        .map_err(|kind| ExecutorError::Node {
                            node_id: node_id.clone(),
                            kind,
                        })?;

                    // Apply sampling to source output.
                    let sampled = sample_batches(batches, &options.sample);
                    let duration = node_start.elapsed();

                    let row_count: u64 = sampled.iter().map(|b| b.num_rows() as u64).sum();
                    let schema = schema_from_batches(&sampled);

                    debug!(node = %node_id, rows = row_count, "preview source sampled");

                    node_results.insert(
                        node_id.clone(),
                        PreviewNodeResult {
                            node_id: node_id.clone(),
                            schema,
                            batches: sampled.clone(),
                            row_count,
                            duration,
                        },
                    );
                    outputs.insert(node_id.clone(), sampled);
                }

                NodeKind::Transform(xform_cfg) => {
                    let upstream_ids = pipeline.upstream_of(node_id);
                    let mut rows_in: u64 = 0;
                    let upstream_data =
                        Self::gather_upstream(&upstream_ids, &outputs, &mut rows_in).map_err(
                            |kind| ExecutorError::Node {
                                node_id: node_id.clone(),
                                kind,
                            },
                        )?;

                    let batches = match xform_cfg.mode {
                        flux_engine::node::TransformMode::Sql => {
                            Self::execute_sql_transform(&xform_cfg.code, upstream_data, None)
                                .await
                                .map_err(|kind| ExecutorError::Node {
                                    node_id: node_id.clone(),
                                    kind,
                                })?
                        }
                        flux_engine::node::TransformMode::Python => {
                            let variables = pipeline
                                .variables
                                .iter()
                                .filter_map(|(k, v)| {
                                    v.default.as_ref().map(|d| (k.clone(), d.clone()))
                                })
                                .collect();
                            crate::python_runtime::execute_python_transform(
                                &xform_cfg.code,
                                upstream_data,
                                &variables,
                            )
                            .await
                            .map_err(|kind| ExecutorError::Node {
                                node_id: node_id.clone(),
                                kind,
                            })?
                        }
                    };

                    let duration = node_start.elapsed();
                    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                    let schema = schema_from_batches(&batches);

                    debug!(node = %node_id, rows_in, rows_out = row_count, "preview transform complete");

                    node_results.insert(
                        node_id.clone(),
                        PreviewNodeResult {
                            node_id: node_id.clone(),
                            schema,
                            batches: batches.clone(),
                            row_count,
                            duration,
                        },
                    );
                    outputs.insert(node_id.clone(), batches);
                }

                NodeKind::Sink(_) => {
                    // Sinks are skipped during preview — no side effects.
                    // We still record the upstream data as this node's output
                    // so the frontend can show what *would* be written.
                    let upstream_ids = pipeline.upstream_of(node_id);
                    let mut rows_in: u64 = 0;
                    let upstream_data =
                        Self::gather_upstream(&upstream_ids, &outputs, &mut rows_in).map_err(
                            |kind| ExecutorError::Node {
                                node_id: node_id.clone(),
                                kind,
                            },
                        )?;

                    let all_batches: Vec<RecordBatch> = upstream_data
                        .into_values()
                        .flat_map(|batches| batches.iter().cloned())
                        .collect();

                    let duration = node_start.elapsed();
                    let row_count: u64 = all_batches.iter().map(|b| b.num_rows() as u64).sum();
                    let schema = schema_from_batches(&all_batches);

                    debug!(node = %node_id, rows = row_count, "preview sink skipped");

                    node_results.insert(
                        node_id.clone(),
                        PreviewNodeResult {
                            node_id: node_id.clone(),
                            schema,
                            batches: all_batches.clone(),
                            row_count,
                            duration,
                        },
                    );
                    outputs.insert(node_id.clone(), all_batches);
                }
            }
        }

        let total_duration = start.elapsed();

        info!(
            pipeline = %pipeline.name,
            duration_ms = total_duration.as_millis(),
            "preview execution complete"
        );

        Ok(PreviewResult {
            pipeline_name: pipeline.name.clone(),
            sample_config: options.sample.clone(),
            nodes: node_results,
            execution_order: order,
            duration: total_duration,
        })
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
