// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline execution engine.
//!
//! Walks the validated DAG in topological order, dispatching each node to the
//! appropriate handler (source connector → DataFusion TableProvider, SQL
//! transform, or pipeline sink).

use crate::error::{ExecutorError, NodeErrorKind};
use crate::provider::{ProviderRegistry, WriteOptions};
use crate::resolver::EnvironmentResolver;
use crate::result::PipelineResult;
use crate::run::{ExecutionEvent, NodeRunStats, PipelineRun, RunStatus};
use crate::run_store::RunStore;
use crate::stats::NodeStats;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use flux_engine::node::{NodeKind, SourceConfig, TransformMode};
use flux_engine::{NodeId, Pipeline, dag};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Options controlling a pipeline execution.
pub struct ExecutionOptions {
    /// Environment name for this run (e.g. "dev", "prod").
    pub environment: String,
    /// Optional run store for persisting execution history.
    pub run_store: Option<Arc<RunStore>>,
    /// Set to `true` from another thread/task to cancel execution after the
    /// current node completes.
    pub cancel: Arc<AtomicBool>,
    /// Optional environment resolver for catalog-based table resolution.
    /// When set, the `SessionContext` for SQL transforms is configured with
    /// this resolver's catalog hierarchy.
    pub environment_resolver: Option<Arc<EnvironmentResolver>>,
    /// Optional channel for real-time execution progress events.
    /// When set, the executor sends [`ExecutionEvent`]s as nodes start,
    /// complete, or fail. The receiver is typically the server layer that
    /// broadcasts to WebSocket clients.
    pub progress: Option<mpsc::UnboundedSender<ExecutionEvent>>,
}

impl Default for ExecutionOptions {
    fn default() -> Self {
        Self {
            environment: "default".to_string(),
            run_store: None,
            cancel: Arc::new(AtomicBool::new(false)),
            environment_resolver: None,
            progress: None,
        }
    }
}

/// Executes a pipeline by walking the DAG in topological order.
pub struct PipelineExecutor;

impl PipelineExecutor {
    /// Execute a pipeline to completion.
    ///
    /// Validates the DAG, walks nodes in topological order, and dispatches each
    /// node to the appropriate handler. Returns a [`PipelineResult`] containing
    /// all node outputs and per-node statistics, along with a [`PipelineRun`]
    /// record suitable for persistence.
    ///
    /// Uses fail-fast semantics: execution stops at the first node error.
    /// If `options.cancel` is set, execution stops after the current node and
    /// returns [`ExecutorError::Cancelled`].
    pub async fn execute(
        pipeline: &Pipeline,
        registry: &ProviderRegistry,
        options: &ExecutionOptions,
    ) -> Result<(PipelineResult, PipelineRun), ExecutorError> {
        dag::validate(pipeline).map_err(ExecutorError::Validation)?;
        let order = dag::topological_sort(pipeline);

        info!(pipeline = %pipeline.name, nodes = order.len(), "starting pipeline execution");

        // Create the run record.
        let mut run = match &options.run_store {
            Some(store) => store
                .create_run(&pipeline.name, &options.environment)
                .map_err(ExecutorError::RunStore)?,
            None => PipelineRun::new(&pipeline.name, &options.environment),
        };

        let run_start_wall = SystemTime::now();
        let run_start = Instant::now();

        // Mark running.
        run.status = RunStatus::Running;
        run.start_time = Some(run_start_wall);
        if let Some(store) = &options.run_store {
            let _ = store.set_running(&run.id, run_start_wall);
        }

        emit(
            &options.progress,
            ExecutionEvent::RunStarted {
                run_id: run.id.clone(),
                pipeline_name: pipeline.name.clone(),
            },
        );

        let mut outputs: HashMap<NodeId, Vec<RecordBatch>> = HashMap::new();
        let mut stats: Vec<NodeStats> = Vec::new();

        for node_id in &order {
            // Check cancellation before each node.
            if options.cancel.load(Ordering::Relaxed) {
                warn!(pipeline = %pipeline.name, "execution cancelled");
                let end_wall = SystemTime::now();
                run.status = RunStatus::Cancelled;
                run.end_time = Some(end_wall);
                if let Some(store) = &options.run_store {
                    let _ = store.finish_run(&run.id, RunStatus::Cancelled, end_wall, None);
                }
                emit(
                    &options.progress,
                    ExecutionEvent::RunCompleted {
                        run_id: run.id.clone(),
                        status: RunStatus::Cancelled,
                        duration_ms: Instant::now().duration_since(run_start).as_millis() as u64,
                    },
                );
                return Err(ExecutorError::Cancelled);
            }

            let node = pipeline
                .node(node_id)
                .expect("topological_sort returned an ID not in the pipeline");

            debug!(node = %node_id, kind = ?std::mem::discriminant(&node.kind), "executing node");

            emit(
                &options.progress,
                ExecutionEvent::NodeStarted {
                    run_id: run.id.clone(),
                    node_id: node_id.clone(),
                },
            );

            let node_start = Instant::now();
            let node_start_wall = SystemTime::now();
            let mut rows_in: u64 = 0;

            let result: Result<Vec<RecordBatch>, NodeErrorKind> = match &node.kind {
                NodeKind::Source(src_cfg) => Self::execute_source(node_id, src_cfg, registry).await,

                NodeKind::Transform(xform_cfg) => match xform_cfg.mode {
                    TransformMode::Sql => {
                        let upstream_ids = pipeline.upstream_of(node_id);
                        match Self::gather_upstream(&upstream_ids, &outputs, &mut rows_in) {
                            Ok(data) => {
                                Self::execute_sql_transform(
                                    &xform_cfg.code,
                                    data,
                                    options.environment_resolver.as_ref(),
                                )
                                .await
                            }
                            Err(e) => Err(e),
                        }
                    }
                    TransformMode::Python => {
                        let upstream_ids = pipeline.upstream_of(node_id);
                        match Self::gather_upstream(&upstream_ids, &outputs, &mut rows_in) {
                            Ok(data) => {
                                let variables = pipeline
                                    .variables
                                    .iter()
                                    .filter_map(|(k, v)| {
                                        v.default.as_ref().map(|d| (k.clone(), d.clone()))
                                    })
                                    .collect();
                                crate::python_runtime::execute_python_transform(
                                    &xform_cfg.code,
                                    data,
                                    &variables,
                                )
                                .await
                            }
                            Err(e) => Err(e),
                        }
                    }
                },

                NodeKind::Sink(sink_cfg) => {
                    let upstream_ids = pipeline.upstream_of(node_id);
                    match Self::gather_upstream(&upstream_ids, &outputs, &mut rows_in) {
                        Ok(upstream_data) => {
                            let all_batches: Vec<RecordBatch> = upstream_data
                                .into_values()
                                .flat_map(|batches| batches.iter().cloned())
                                .collect();
                            Self::execute_sink(sink_cfg, all_batches, registry)
                                .await
                                .map(|(batches, _write_stats)| batches)
                        }
                        Err(e) => Err(e),
                    }
                }
            };

            let node_end = Instant::now();
            let node_end_wall = SystemTime::now();

            match result {
                Ok(batches) => {
                    let rows_out: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                    debug!(node = %node_id, rows_in, rows_out, "node completed");

                    stats.push(NodeStats {
                        node_id: node_id.clone(),
                        start_time: node_start,
                        end_time: node_end,
                        rows_in,
                        rows_out,
                        error: None,
                    });

                    let node_run_stats = NodeRunStats {
                        node_id: node_id.clone(),
                        start_time: node_start_wall,
                        end_time: node_end_wall,
                        rows_in,
                        rows_out,
                        error: None,
                    };
                    if let Some(store) = &options.run_store {
                        let _ = store.save_node_stats(&run.id, &node_run_stats);
                    }
                    run.node_stats.push(node_run_stats);

                    emit(
                        &options.progress,
                        ExecutionEvent::NodeCompleted {
                            run_id: run.id.clone(),
                            node_id: node_id.clone(),
                            rows_out,
                            duration_ms: node_end.duration_since(node_start).as_millis() as u64,
                        },
                    );

                    outputs.insert(node_id.clone(), batches);
                }
                Err(kind) => {
                    let error_msg = kind.to_string();

                    stats.push(NodeStats {
                        node_id: node_id.clone(),
                        start_time: node_start,
                        end_time: node_end,
                        rows_in,
                        rows_out: 0,
                        error: Some(error_msg.clone()),
                    });

                    let node_run_stats = NodeRunStats {
                        node_id: node_id.clone(),
                        start_time: node_start_wall,
                        end_time: node_end_wall,
                        rows_in,
                        rows_out: 0,
                        error: Some(error_msg.clone()),
                    };
                    if let Some(store) = &options.run_store {
                        let _ = store.save_node_stats(&run.id, &node_run_stats);
                    }
                    run.node_stats.push(node_run_stats);

                    emit(
                        &options.progress,
                        ExecutionEvent::NodeFailed {
                            run_id: run.id.clone(),
                            node_id: node_id.clone(),
                            error: error_msg.clone(),
                        },
                    );

                    // Finalize as failed.
                    let end_wall = SystemTime::now();
                    run.status = RunStatus::Failed;
                    run.end_time = Some(end_wall);
                    run.error = Some(error_msg);
                    if let Some(store) = &options.run_store {
                        let _ = store.finish_run(
                            &run.id,
                            RunStatus::Failed,
                            end_wall,
                            run.error.as_deref(),
                        );
                    }

                    emit(
                        &options.progress,
                        ExecutionEvent::RunCompleted {
                            run_id: run.id.clone(),
                            status: RunStatus::Failed,
                            duration_ms: Instant::now().duration_since(run_start).as_millis()
                                as u64,
                        },
                    );

                    return Err(ExecutorError::Node {
                        node_id: node_id.clone(),
                        kind,
                    });
                }
            }
        }

        let run_end = Instant::now();
        let run_end_wall = SystemTime::now();

        info!(
            pipeline = %pipeline.name,
            duration_ms = run_end.duration_since(run_start).as_millis(),
            "pipeline execution complete"
        );

        // Finalize as success.
        run.status = RunStatus::Success;
        run.end_time = Some(run_end_wall);
        if let Some(store) = &options.run_store {
            let _ = store.finish_run(&run.id, RunStatus::Success, run_end_wall, None);
        }

        emit(
            &options.progress,
            ExecutionEvent::RunCompleted {
                run_id: run.id.clone(),
                status: RunStatus::Success,
                duration_ms: run_end.duration_since(run_start).as_millis() as u64,
            },
        );

        let pipeline_result = PipelineResult {
            pipeline_name: pipeline.name.clone(),
            start_time: run_start,
            end_time: run_end,
            node_outputs: outputs,
            node_stats: stats,
        };

        Ok((pipeline_result, run))
    }

    /// Create a `TableProvider` from the source connector and read all rows.
    pub async fn execute_source(
        node_id: &NodeId,
        config: &SourceConfig,
        registry: &ProviderRegistry,
    ) -> Result<Vec<RecordBatch>, NodeErrorKind> {
        let connector = registry
            .get_source(&config.connector)
            .ok_or_else(|| NodeErrorKind::SourceProviderNotFound(config.connector.clone()))?;

        let table_provider = connector
            .create_table_provider(config)
            .map_err(NodeErrorKind::Source)?;

        // Use a DataFusion session to read all data from the TableProvider.
        // This path supports filter/projection pushdown for providers that
        // implement it (e.g. Parquet row-group pruning, PostgreSQL WHERE pushdown).
        let ctx = SessionContext::new();
        let table_name = node_id.to_string();
        ctx.register_table(table_name.as_str(), table_provider)?;
        let df = ctx
            .sql(&format!("SELECT * FROM \"{}\"", table_name))
            .await?;
        let batches = df.collect().await?;
        Ok(batches)
    }

    pub(crate) fn gather_upstream<'a>(
        upstream_ids: &[&NodeId],
        outputs: &'a HashMap<NodeId, Vec<RecordBatch>>,
        rows_in: &mut u64,
    ) -> Result<HashMap<NodeId, &'a Vec<RecordBatch>>, NodeErrorKind> {
        let mut upstream_data = HashMap::new();
        for uid in upstream_ids {
            let batches = outputs
                .get(*uid)
                .ok_or_else(|| NodeErrorKind::MissingUpstreamOutput((*uid).clone()))?;
            *rows_in += batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
            upstream_data.insert((*uid).clone(), batches);
        }
        Ok(upstream_data)
    }

    pub async fn execute_sql_transform(
        sql: &str,
        upstream_data: HashMap<NodeId, &Vec<RecordBatch>>,
        resolver: Option<&Arc<EnvironmentResolver>>,
    ) -> Result<Vec<RecordBatch>, NodeErrorKind> {
        let ctx = SessionContext::new();

        // Register the environment resolver if provided, so that SQL queries
        // can reference tables from the active environment's catalog with
        // fallback resolution.
        if let Some(resolver) = resolver {
            ctx.register_catalog_list(resolver.clone());
        }

        // Build schema map for the friendly SQL preprocessor and register tables.
        let mut table_schemas = HashMap::new();
        for (node_id, batches) in &upstream_data {
            if batches.is_empty() {
                continue;
            }
            let schema = batches[0].schema();
            table_schemas.insert(node_id.to_string(), schema.clone());
            let mem_table = MemTable::try_new(schema, vec![batches.to_vec()])?;
            ctx.register_table(node_id.to_string().as_str(), Arc::new(mem_table))?;
        }

        // Preprocess friendly SQL syntax (GROUP BY ALL, EXCLUDE, COLUMNS, bare FROM)
        // into standard SQL that DataFusion understands.
        let processed_sql = crate::friendly_sql::preprocess_sql(sql, &table_schemas)?;

        let df = ctx.sql(&processed_sql).await?;
        let batches = df.collect().await?;
        Ok(batches)
    }

    /// Write data through the sink connector, returning the batches and write stats.
    async fn execute_sink(
        config: &flux_engine::node::SinkConfig,
        batches: Vec<RecordBatch>,
        registry: &ProviderRegistry,
    ) -> Result<(Vec<RecordBatch>, crate::provider::WriteStats), NodeErrorKind> {
        let sink = registry
            .get_sink(&config.connector)
            .ok_or_else(|| NodeErrorKind::SinkNotFound(config.connector.clone()))?;
        let write_stats = sink
            .write(config, batches.clone(), &WriteOptions::default())
            .await
            .map_err(NodeErrorKind::Sink)?;
        Ok((batches, write_stats))
    }
}

/// Send a progress event if a sender is available. Silently ignores closed channels.
fn emit(sender: &Option<mpsc::UnboundedSender<ExecutionEvent>>, event: ExecutionEvent) {
    if let Some(tx) = sender {
        let _ = tx.send(event);
    }
}
