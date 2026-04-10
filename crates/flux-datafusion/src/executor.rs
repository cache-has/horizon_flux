// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline execution engine.
//!
//! Walks the validated DAG in topological order, dispatching each node to the
//! appropriate handler (source connector → DataFusion TableProvider, SQL
//! transform, or pipeline sink).

use crate::error::{ExecutorError, NodeErrorKind};
use crate::incremental_coordinator::{IncrementalReadPlan, IncrementalSinkPlan, build_plans};
use crate::incremental_state::{IncrementalSchemaRecord, IncrementalState};
use crate::provider::{
    MaterializationContext, MaterializationReceipt, ProviderRegistry, WatermarkValue, WriteOptions,
};
use crate::resolver::EnvironmentResolver;
use crate::result::PipelineResult;
use crate::run::{ExecutionEvent, NodeRunStats, PipelineRun, RunId, RunStatus, TestResultSummary};
use crate::schema_diff::{SchemaAction, apply_policy, compute_schema_diff, schema_fingerprint};
use crate::session::SessionFactory;
use crate::stats::NodeStats;
use crate::storage::{
    ColumnLineageStorage, IncrementalStateStorage, LineageObservation, LineageStorage, RunStorage,
    StoredColumnEdge,
};
use crate::udfs::UdfRegistry;
use crate::watermark::{
    build_filter_expr, fold_max_watermark, scalar_to_stored, stored_to_scalar,
    watermark_type_matches,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use flux_engine::node::{NodeKind, SourceConfig, TransformMode};
use flux_engine::variables::{BuiltinContext, ResolvedVariables};
use flux_engine::{NodeId, Pipeline, dag};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Trait for resolving `{{ secret:name }}` references in connector configs.
///
/// This abstraction lets the executor resolve secrets without depending on the
/// `flux-secrets` crate directly. The server provides an implementation backed
/// by `SecretSession`; CLI callers can provide one backed by a directly-opened
/// `SecretStore`.
pub trait SecretResolver: Send + Sync {
    /// Resolve all `{{ secret:... }}` references in a JSON value.
    ///
    /// The `environment` parameter controls environment-scoped secret lookup.
    fn resolve_json(
        &self,
        value: &Value,
        environment: Option<&str>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;

    /// Resolve all `{{ secret:... }}` references and also return the plaintext
    /// secret values that were substituted. The returned values are used by
    /// [`flux_secrets::scrub_secrets`] to redact secrets from error messages.
    ///
    /// The default implementation delegates to [`resolve_json`] and returns an
    /// empty list (no scrubbing support).
    fn resolve_json_collecting(
        &self,
        value: &Value,
        environment: Option<&str>,
    ) -> Result<(Value, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
        self.resolve_json(value, environment)
            .map(|v| (v, Vec::new()))
    }
}

/// Options controlling a pipeline execution.
/// Callback fired after column lineage edges are persisted.
/// Receives `(pipeline_id, environment, edge_count)`.
pub type ColumnLineageCallback = dyn Fn(&str, &str, usize) + Send + Sync;

pub struct ExecutionOptions {
    /// Environment name for this run (e.g. "dev", "prod").
    pub environment: String,
    /// Optional run store for persisting execution history.
    pub run_store: Option<Arc<dyn RunStorage>>,
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
    /// Runtime variable overrides. These take highest precedence over
    /// pipeline defaults and built-in variables.
    pub variable_overrides: HashMap<String, Value>,
    /// Optional secret resolver for expanding `{{ secret:name }}` references
    /// in connector configs. When `None`, secret references are left unresolved
    /// (which will cause connector errors if the config contains them).
    pub secret_resolver: Option<Arc<dyn SecretResolver>>,
    /// Shared DataFusion session factory with memory pool and spill-to-disk
    /// configuration. When `None`, each node creates an unconfigured
    /// `SessionContext` with default (unbounded) memory.
    pub session_factory: Option<Arc<SessionFactory>>,
    /// Optional incremental-state store. When `None`, sinks with
    /// `read_mode: incremental` behave as first-runs every time (the
    /// pre-pass treats missing state as "no prior run") and no state is
    /// persisted afterwards. Production runs always pass one in.
    pub incremental_state_store: Option<Arc<dyn IncrementalStateStorage>>,
    /// Skip watermark filter injection on incremental sinks for this run,
    /// then advance state at the end as if it were a first run. The CLI
    /// surfaces this as `--full-refresh`; doc 27's "explicit user action,
    /// never an automatic decision" escape hatch.
    pub full_refresh: bool,
    /// Permit incremental sinks whose `first_run` policy is `fail` to
    /// perform their bootstrap load. Surfaced as `--bootstrap-incremental`.
    pub bootstrap_incremental: bool,
    /// Skip sink writes entirely. When `true`, every sink node short-circuits
    /// to an empty-batch success without touching its target, no
    /// `MaterializationReceipt` is produced, and the incremental pre-pass is
    /// not run. Upstream node outputs still flow into `PipelineResult::node_outputs`
    /// so callers can read the staged batches that *would* have been written.
    /// Used by `flux snapshot diff` (doc 28) to drive a true dry-run.
    pub dry_run_no_sinks: bool,
    /// Optional lineage store for recording runtime-observed lineage
    /// (planning doc 31). When `Some`, the executor records an observation
    /// for every successful source read and sink write.
    pub lineage_store: Option<Arc<dyn LineageStorage>>,
    /// Fingerprint function for computing resource identifiers during
    /// lineage observation recording (planning doc 31).
    pub fingerprint_fn: Option<flux_engine::FingerprintFn>,
    /// Pipeline ID string for lineage observation recording.
    pub pipeline_id: Option<String>,
    /// Optional column-level lineage store for persisting lineage edges derived
    /// from SQL transform logical plans (planning doc 35). When `Some`, the
    /// executor derives and persists column-level lineage for every transform.
    pub column_lineage_store: Option<Arc<dyn ColumnLineageStorage>>,
    /// Optional callback fired after column lineage edges are persisted.
    pub on_column_lineage_updated: Option<Arc<ColumnLineageCallback>>,
}

impl Default for ExecutionOptions {
    fn default() -> Self {
        Self {
            environment: "default".to_string(),
            run_store: None,
            cancel: Arc::new(AtomicBool::new(false)),
            environment_resolver: None,
            progress: None,
            variable_overrides: HashMap::new(),
            secret_resolver: None,
            session_factory: None,
            incremental_state_store: None,
            full_refresh: false,
            bootstrap_incremental: false,
            dry_run_no_sinks: false,
            lineage_store: None,
            fingerprint_fn: None,
            pipeline_id: None,
            column_lineage_store: None,
            on_column_lineage_updated: None,
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

        // Load reusable SQL UDFs (planning doc 29, Layer 1). Errors here are
        // surfaced as a node failure on the first SQL transform we can find,
        // since UDFs only affect SQL transforms — that's the most actionable
        // place to land the message. If no SQL transform exists the load is
        // a no-op anyway.
        let udf_registry = match pipeline.udfs_dir.as_deref() {
            Some(dir) => UdfRegistry::load_from_dir(std::path::Path::new(dir)).map_err(|e| {
                let target = pipeline
                    .nodes
                    .iter()
                    .find(|n| {
                        matches!(
                            &n.kind,
                            NodeKind::Transform(cfg) if matches!(cfg.mode, TransformMode::Sql)
                        )
                    })
                    .map(|n| n.id.clone())
                    .unwrap_or_else(|| NodeId::from("<udf-load>"));
                ExecutorError::Node {
                    node_id: target,
                    kind: e.into(),
                }
            })?,
            None => UdfRegistry::new(),
        };
        let udf_registry = Arc::new(udf_registry);

        // Doc 27 pre-pass: walk every incremental sink, load state, and
        // build per-source watermark filters before any I/O. Hard errors
        // here surface as a node failure on the *first* incremental sink
        // (we don't have a real `node_id` for a pipeline-wide error).
        // Skipped under `dry_run_no_sinks` (doc 28 `flux snapshot diff`):
        // sinks won't write anyway, so we don't need state lookups, and
        // pipelines without an `incremental_state_store` configured (e.g.
        // ad-hoc CLI dry-runs) would otherwise hit a "first run" pre-pass
        // failure on `first_run: fail` sinks before we even reach them.
        let incremental_plans = if options.dry_run_no_sinks {
            crate::incremental_coordinator::IncrementalPlans::default()
        } else {
            build_plans(
                pipeline,
                &pipeline.name,
                &options.environment,
                options.incremental_state_store.as_ref(),
                options.full_refresh,
                options.bootstrap_incremental,
            )
            .map_err(|e| {
                // Map to a node-level error attached to the first incremental
                // sink we can find — the error message itself names the
                // offending sink and is what the user actually reads.
                let sink_id = pipeline
                    .nodes
                    .iter()
                    .find(|n| matches!(n.kind, flux_engine::node::NodeKind::Sink(_)))
                    .map(|n| n.id.clone())
                    .unwrap_or_else(|| NodeId::from("<pre-pass>"));
                ExecutorError::Node {
                    node_id: sink_id,
                    kind: e.into(),
                }
            })?
        };

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

        // Resolve pipeline variables: built-ins + defaults + overrides.
        let builtin = BuiltinContext {
            run_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
            run_id: run.id.to_string(),
            pipeline_name: pipeline.name.clone(),
            environment: options.environment.clone(),
        };
        let resolved_vars =
            ResolvedVariables::resolve(pipeline, &options.variable_overrides, &builtin);

        let mut outputs: HashMap<NodeId, Vec<RecordBatch>> = HashMap::new();
        let mut stats: Vec<NodeStats> = Vec::new();
        let mut test_results: Vec<crate::test_assertion::TestNodeResult> = Vec::new();

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
            // Secret values resolved for this node, used to scrub error messages.
            let mut node_secret_values: Vec<String> = Vec::new();
            // Sink-only: populated by `execute_sink` so we can attach the
            // doc-27 materialization receipt to NodeRunStats and broadcast
            // it on the NodeCompleted event.
            let mut sink_receipt: Option<MaterializationReceipt> = None;
            // Sink-only: column names and upstream node IDs captured before
            // the sink consumes the data, used for boundary column lineage.
            let mut sink_boundary_info: Option<(Vec<String>, Vec<NodeId>)> = None;

            let result: Result<Vec<RecordBatch>, NodeErrorKind> = match &node.kind {
                NodeKind::Source(src_cfg) => {
                    let mut interpolated_cfg = src_cfg.clone();
                    // Apply environment override before variable interpolation.
                    if let Some(overrides) = pipeline
                        .environment_overrides
                        .get(&options.environment)
                        .and_then(|env| env.get(&node_id.0))
                    {
                        debug!(node = %node_id, env = %options.environment, "applying environment override to source");
                        merge_override(&mut interpolated_cfg.config, overrides);
                    }
                    // Interpolate variables in source connector config.
                    interpolated_cfg.config =
                        resolved_vars.interpolate_json(&interpolated_cfg.config);
                    // Resolve secret references (collecting values for scrubbing).
                    if let Some(resolver) = &options.secret_resolver {
                        match resolver.resolve_json_collecting(
                            &interpolated_cfg.config,
                            Some(&options.environment),
                        ) {
                            Ok((resolved, values)) => {
                                interpolated_cfg.config = resolved;
                                node_secret_values = values;
                            }
                            Err(e) => {
                                return Err(ExecutorError::Node {
                                    node_id: node_id.clone(),
                                    kind: NodeErrorKind::Source(e),
                                });
                            }
                        }
                    }
                    Self::execute_source(
                        node_id,
                        &interpolated_cfg,
                        registry,
                        options.session_factory.as_deref(),
                        incremental_plans.source_plans.get(node_id),
                    )
                    .await
                }

                NodeKind::Transform(xform_cfg) => {
                    // Resolve code from file or inline.
                    let code =
                        pipeline
                            .resolve_code(xform_cfg)
                            .map_err(|e| NodeErrorKind::CodeFileRead {
                                path: xform_cfg
                                    .code_path
                                    .clone()
                                    .unwrap_or_else(|| "(inline)".into()),
                                source: e,
                            });
                    match code {
                        Err(e) => Err(e),
                        Ok(code) => match xform_cfg.mode {
                            TransformMode::Sql => {
                                let upstream_ids = pipeline.upstream_of(node_id);
                                match Self::gather_upstream(&upstream_ids, &outputs, &mut rows_in) {
                                    Ok(data) => {
                                        let interpolated_sql = resolved_vars.interpolate(&code);
                                        // Derive column lineage when a store is configured.
                                        let derive_lineage = options.column_lineage_store.is_some();
                                        let lineage_node_id =
                                            if derive_lineage { Some(node_id) } else { None };
                                        match Self::execute_sql_transform_with_lineage(
                                            &interpolated_sql,
                                            data,
                                            options.environment_resolver.as_ref(),
                                            options.session_factory.as_deref(),
                                            Some(&udf_registry),
                                            lineage_node_id,
                                        )
                                        .await
                                        {
                                            Ok((batches, lineage)) => {
                                                // Persist column lineage edges if derived.
                                                if let (
                                                    Some(store),
                                                    Some(pid),
                                                    Some(node_lineage),
                                                ) = (
                                                    &options.column_lineage_store,
                                                    &options.pipeline_id,
                                                    lineage,
                                                ) {
                                                    persist_column_lineage(
                                                        store.as_ref(),
                                                        pid,
                                                        &options.environment,
                                                        &node_lineage,
                                                        Some(&run.id),
                                                        options
                                                            .on_column_lineage_updated
                                                            .as_deref(),
                                                    );
                                                }
                                                Ok(batches)
                                            }
                                            Err(e) => Err(e),
                                        }
                                    }
                                    Err(e) => Err(e),
                                }
                            }
                            TransformMode::Python => {
                                let upstream_ids = pipeline.upstream_of(node_id);
                                match Self::gather_upstream(&upstream_ids, &outputs, &mut rows_in) {
                                    Ok(data) => {
                                        let py_config =
                                            crate::python_runtime::PythonConfig::default();
                                        // Capture input columns before the borrow is consumed.
                                        let input_columns_for_lineage: Option<
                                            Vec<(NodeId, Vec<String>)>,
                                        > = if options.column_lineage_store.is_some()
                                            && options.pipeline_id.is_some()
                                        {
                                            Some(
                                                data.iter()
                                                    .map(|(nid, bs)| {
                                                        (nid.clone(), extract_column_names(bs))
                                                    })
                                                    .collect(),
                                            )
                                        } else {
                                            None
                                        };
                                        match crate::python_runtime::execute_python_transform(
                                            &code,
                                            data,
                                            resolved_vars.as_map(),
                                            &py_config,
                                        )
                                        .await
                                        {
                                            Ok(batches) => {
                                                // Persist opaque column lineage for Python transforms.
                                                if let (Some(store), Some(pid), Some(input_cols)) = (
                                                    &options.column_lineage_store,
                                                    &options.pipeline_id,
                                                    input_columns_for_lineage,
                                                ) {
                                                    let output_columns =
                                                        extract_column_names(&batches);
                                                    let opaque_lineage =
                                                        crate::column_lineage::derive_opaque_lineage(
                                                            node_id,
                                                            &input_cols,
                                                            &output_columns,
                                                        );
                                                    persist_column_lineage(
                                                        store.as_ref(),
                                                        pid,
                                                        &options.environment,
                                                        &opaque_lineage,
                                                        Some(&run.id),
                                                        options
                                                            .on_column_lineage_updated
                                                            .as_deref(),
                                                    );
                                                }
                                                Ok(batches)
                                            }
                                            Err(e) => Err(e),
                                        }
                                    }
                                    Err(e) => Err(e),
                                }
                            }
                        },
                    }
                }

                NodeKind::Sink(sink_cfg) => {
                    // Doc 28 dry-run: skip the sink entirely. Upstream
                    // outputs are already in `outputs` so callers (e.g.
                    // `flux snapshot diff`) can read them; we just don't
                    // touch the sink's target.
                    if options.dry_run_no_sinks {
                        Ok(Vec::new())
                    } else {
                        let upstream_ids = pipeline.upstream_of(node_id);
                        match Self::gather_upstream(&upstream_ids, &outputs, &mut rows_in) {
                            Ok(upstream_data) => {
                                let all_batches: Vec<RecordBatch> = upstream_data
                                    .into_values()
                                    .flat_map(|batches| batches.iter().cloned())
                                    .collect();
                                let mut interpolated_cfg = sink_cfg.clone();
                                // Apply environment override before variable interpolation.
                                if let Some(overrides) = pipeline
                                    .environment_overrides
                                    .get(&options.environment)
                                    .and_then(|env| env.get(&node_id.0))
                                {
                                    debug!(node = %node_id, env = %options.environment, "applying environment override to sink");
                                    merge_override(&mut interpolated_cfg.config, overrides);
                                }
                                // Interpolate variables in sink connector config.
                                interpolated_cfg.config =
                                    resolved_vars.interpolate_json(&interpolated_cfg.config);
                                // Resolve secret references (collecting values for scrubbing).
                                if let Some(resolver) = &options.secret_resolver {
                                    match resolver.resolve_json_collecting(
                                        &interpolated_cfg.config,
                                        Some(&options.environment),
                                    ) {
                                        Ok((resolved, values)) => {
                                            interpolated_cfg.config = resolved;
                                            node_secret_values = values;
                                        }
                                        Err(e) => {
                                            return Err(ExecutorError::Node {
                                                node_id: node_id.clone(),
                                                kind: NodeErrorKind::Sink(e),
                                            });
                                        }
                                    }
                                }
                                // Capture sink input columns for boundary lineage before
                                // the data is consumed by execute_sink.
                                if options.column_lineage_store.is_some()
                                    && options.pipeline_id.is_some()
                                    && options.fingerprint_fn.is_some()
                                {
                                    let cols = extract_column_names(&all_batches);
                                    sink_boundary_info = Some((
                                        cols,
                                        upstream_ids.iter().cloned().cloned().collect(),
                                    ));
                                }
                                Self::execute_sink(
                                    node_id,
                                    &interpolated_cfg,
                                    all_batches,
                                    registry,
                                    incremental_plans.sink_plans.get(node_id),
                                    options.incremental_state_store.as_ref(),
                                    &pipeline.name,
                                    &options.environment,
                                    &run.id,
                                    options.full_refresh,
                                )
                                .await
                                .map(|(batches, receipt)| {
                                    sink_receipt = Some(receipt);
                                    batches
                                })
                            }
                            Err(e) => Err(e),
                        }
                    }
                }

                NodeKind::Test(test_cfg) => {
                    let upstream_ids = pipeline.upstream_of(node_id);
                    match Self::gather_upstream(&upstream_ids, &outputs, &mut rows_in) {
                        Ok(data) => {
                            match crate::test_assertion::execute_test(node_id, test_cfg, data).await
                            {
                                Ok((batches, test_result)) => {
                                    emit(
                                        &options.progress,
                                        ExecutionEvent::TestNodePassed {
                                            run_id: run.id.clone(),
                                            node_id: node_id.clone(),
                                            assertions_count: test_result.assertions.len(),
                                        },
                                    );
                                    test_results.push(test_result);
                                    Ok(batches)
                                }
                                Err(NodeErrorKind::TestAssertionFailed {
                                    ref summary,
                                    ref result,
                                }) => {
                                    let failures: Vec<String> = result
                                        .assertions
                                        .iter()
                                        .filter(|a| !a.passed)
                                        .filter_map(|a| a.message.clone())
                                        .collect();
                                    emit(
                                        &options.progress,
                                        ExecutionEvent::TestNodeFailed {
                                            run_id: run.id.clone(),
                                            node_id: node_id.clone(),
                                            severity: result.severity,
                                            failures,
                                        },
                                    );
                                    test_results.push(result.clone());
                                    Err(NodeErrorKind::TestAssertionFailed {
                                        summary: summary.clone(),
                                        result: result.clone(),
                                    })
                                }
                                Err(e) => Err(e),
                            }
                        }
                        Err(e) => Err(e),
                    }
                }

                NodeKind::Snippet(_) => {
                    unreachable!("snippets must be expanded before execution")
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
                        materialization_receipt: sink_receipt.clone(),
                    };
                    if let Some(store) = &options.run_store {
                        let _ = store.save_node_stats(&run.id, &node_run_stats);
                    }
                    run.node_stats.push(node_run_stats);

                    // Doc 31: record lineage observation for source/sink nodes.
                    if let (Some(lineage_store), Some(fp_fn), Some(pid)) = (
                        &options.lineage_store,
                        &options.fingerprint_fn,
                        &options.pipeline_id,
                    ) {
                        let obs = match &node.kind {
                            NodeKind::Source(src) => {
                                fp_fn(&src.connector, &src.config).map(|fp| LineageObservation {
                                    pipeline_id: pid.clone(),
                                    node_id: node_id.0.clone(),
                                    run_id: run.id.0.to_string(),
                                    direction: flux_engine::BindingDirection::Source,
                                    resource_fingerprint: fp,
                                    environment: options.environment.clone(),
                                    observed_at_ms: node_end_wall
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis()
                                        as i64,
                                })
                            }
                            NodeKind::Sink(sink) => {
                                fp_fn(&sink.connector, &sink.config).map(|fp| LineageObservation {
                                    pipeline_id: pid.clone(),
                                    node_id: node_id.0.clone(),
                                    run_id: run.id.0.to_string(),
                                    direction: flux_engine::BindingDirection::Sink,
                                    resource_fingerprint: fp,
                                    environment: options.environment.clone(),
                                    observed_at_ms: node_end_wall
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis()
                                        as i64,
                                })
                            }
                            _ => None,
                        };
                        if let Some(obs) = obs {
                            if let Err(e) = lineage_store.record_observation(&obs) {
                                warn!(node = %node_id, error = %e, "failed to record lineage observation");
                            }
                        }
                    }

                    // Doc 35: derive boundary column lineage for source/sink nodes.
                    if let (Some(col_store), Some(fp_fn), Some(pid)) = (
                        &options.column_lineage_store,
                        &options.fingerprint_fn,
                        &options.pipeline_id,
                    ) {
                        match &node.kind {
                            NodeKind::Source(src) => {
                                if let Some(fp) = fp_fn(&src.connector, &src.config) {
                                    let columns = extract_column_names(&batches);
                                    let boundary =
                                        crate::column_lineage::derive_source_boundary_lineage(
                                            node_id, &fp, &columns,
                                        );
                                    persist_column_lineage(
                                        col_store.as_ref(),
                                        pid,
                                        &options.environment,
                                        &boundary,
                                        Some(&run.id),
                                        options.on_column_lineage_updated.as_deref(),
                                    );
                                }
                            }
                            NodeKind::Sink(sink) => {
                                if let Some(fp) = fp_fn(&sink.connector, &sink.config) {
                                    if let Some((columns, upstream_nodes)) = &sink_boundary_info {
                                        for upstream in upstream_nodes {
                                            let boundary =
                                                crate::column_lineage::derive_sink_boundary_lineage(
                                                    node_id, &fp, columns, upstream,
                                                );
                                            persist_column_lineage(
                                                col_store.as_ref(),
                                                pid,
                                                &options.environment,
                                                &boundary,
                                                Some(&run.id),
                                                options.on_column_lineage_updated.as_deref(),
                                            );
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }

                    emit(
                        &options.progress,
                        ExecutionEvent::NodeCompleted {
                            run_id: run.id.clone(),
                            node_id: node_id.clone(),
                            rows_out,
                            duration_ms: node_end.duration_since(node_start).as_millis() as u64,
                            materialization_receipt: sink_receipt.clone().map(Box::new),
                        },
                    );

                    outputs.insert(node_id.clone(), batches);
                }
                Err(kind) => {
                    // Scrub resolved secret values from the error message so
                    // they never reach logs, the database, WebSocket consumers,
                    // or API responses.
                    let error_msg =
                        flux_secrets::scrub_secrets(&kind.to_string(), &node_secret_values);

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
                        materialization_receipt: None,
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
                    run.test_results = test_results
                        .iter()
                        .map(TestResultSummary::from_test_node_result)
                        .collect();
                    let end_wall = SystemTime::now();
                    run.status = RunStatus::Failed;
                    run.end_time = Some(end_wall);
                    run.error = Some(error_msg);
                    if let Some(store) = &options.run_store {
                        if !run.test_results.is_empty() {
                            let _ = store.save_test_results(&run.id, &run.test_results);
                        }
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
        run.test_results = test_results
            .iter()
            .map(TestResultSummary::from_test_node_result)
            .collect();
        run.status = RunStatus::Success;
        run.end_time = Some(run_end_wall);
        if let Some(store) = &options.run_store {
            if !run.test_results.is_empty() {
                let _ = store.save_test_results(&run.id, &run.test_results);
            }
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
            test_results,
        };

        Ok((pipeline_result, run))
    }

    /// Create a `TableProvider` from the source connector and read all rows.
    ///
    /// When `read_plan` is `Some`, an incremental sink downstream of this
    /// source has declared a watermark filter. The plan is validated
    /// against the source's actual schema (column presence + Arrow type
    /// compatibility) and applied as a `DataFrame::filter` so DataFusion
    /// can push it down into the connector's `TableProvider::scan(filters)`.
    /// Validation failures here are hard errors *before* any rows are
    /// scanned — see doc 27's "no silent fallbacks" rule.
    pub async fn execute_source(
        node_id: &NodeId,
        config: &SourceConfig,
        registry: &ProviderRegistry,
        session_factory: Option<&SessionFactory>,
        read_plan: Option<&IncrementalReadPlan>,
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
        let ctx = match session_factory {
            Some(factory) => factory.create_context(),
            None => SessionContext::new(),
        };

        // Allow connectors to register resources (e.g. cloud object stores)
        // on the execution context before scanning.
        connector
            .configure_session(config, &ctx)
            .map_err(NodeErrorKind::Source)?;

        let provider_schema = table_provider.schema();
        let table_name = node_id.to_string();
        ctx.register_table(TableReference::bare(table_name.as_str()), table_provider)?;
        let mut df = ctx
            .sql(&format!("SELECT * FROM \"{}\"", table_name))
            .await?;

        // Doc 27: inject the watermark filter when an incremental sink
        // downstream of this source has loaded state. Validation against
        // the *actual* source schema happens here so a column rename or
        // type drift fails before any rows are read.
        if let Some(plan) = read_plan {
            let field = provider_schema
                .field_with_name(&plan.column)
                .map_err(|_| {
                    NodeErrorKind::Source(Box::new(std::io::Error::other(format!(
                        "incremental sink `{}`: watermark column `{}` is missing from source `{}` schema",
                        plan.sink_node_id.0, plan.column, node_id
                    ))))
                })?;
            if !watermark_type_matches(field.data_type(), plan.wtype) {
                return Err(NodeErrorKind::Source(Box::new(std::io::Error::other(
                    format!(
                        "incremental sink `{}`: watermark `{}` declared as `{:?}` but source column type is `{}`",
                        plan.sink_node_id.0,
                        plan.column,
                        plan.wtype,
                        field.data_type()
                    ),
                ))));
            }
            if let Some(state) = &plan.state {
                let scalar = stored_to_scalar(
                    &plan.sink_node_id.0,
                    plan.wtype,
                    &state.watermark_value,
                    plan.lookback,
                )
                .map_err(|e| {
                    NodeErrorKind::Source(Box::new(std::io::Error::other(e.to_string())))
                })?;
                let expr = build_filter_expr(&plan.column, scalar);
                df = df.filter(expr)?;
            }
            // No state → first run; the coordinator already enforced the
            // FirstRun policy in build_plans, so reaching here under
            // FirstRun::Full means "read everything, no filter."
        }

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
        session_factory: Option<&SessionFactory>,
        udf_registry: Option<&Arc<UdfRegistry>>,
    ) -> Result<Vec<RecordBatch>, NodeErrorKind> {
        let (batches, _lineage) = Self::execute_sql_transform_with_lineage(
            sql,
            upstream_data,
            resolver,
            session_factory,
            udf_registry,
            None, // no lineage derivation
        )
        .await?;
        Ok(batches)
    }

    /// Execute a SQL transform and optionally derive column-level lineage.
    ///
    /// When `node_id` is `Some`, the DataFusion logical plan is walked to
    /// produce column-level lineage edges (doc 35). When `None`, lineage
    /// derivation is skipped and the second element of the returned tuple is
    /// `None`.
    pub async fn execute_sql_transform_with_lineage(
        sql: &str,
        upstream_data: HashMap<NodeId, &Vec<RecordBatch>>,
        resolver: Option<&Arc<EnvironmentResolver>>,
        session_factory: Option<&SessionFactory>,
        udf_registry: Option<&Arc<UdfRegistry>>,
        node_id: Option<&NodeId>,
    ) -> Result<(Vec<RecordBatch>, Option<flux_engine::NodeColumnLineage>), NodeErrorKind> {
        let ctx = match session_factory {
            Some(factory) => factory.create_context(),
            None => SessionContext::new(),
        };

        // Register the environment resolver if provided, so that SQL queries
        // can reference tables from the active environment's catalog with
        // fallback resolution.
        if let Some(resolver) = resolver {
            ctx.register_catalog_list(resolver.clone());
        }

        // Build schema map for the friendly SQL preprocessor and register tables.
        // Empty result sets must still be registered so downstream transforms
        // can reference them (e.g. joins that return zero rows in preview).
        let mut table_schemas = HashMap::new();
        for (node_id, batches) in &upstream_data {
            if batches.is_empty() {
                continue;
            }
            let (schema, data) = (batches[0].schema(), vec![batches.to_vec()]);
            table_schemas.insert(node_id.to_string(), schema.clone());
            let mem_table = MemTable::try_new(schema, data)?;
            ctx.register_table(
                TableReference::bare(node_id.to_string()),
                Arc::new(mem_table),
            )?;
        }

        // Inline reusable SQL UDFs (planning doc 29, Layer 1) before any
        // other preprocessing — UDF bodies may themselves use friendly SQL
        // constructs, so we want them to flow through `preprocess_sql` next.
        let inlined_sql = match udf_registry {
            Some(reg) if !reg.is_empty() => reg.inline(sql)?,
            _ => sql.to_string(),
        };

        // Preprocess friendly SQL syntax (GROUP BY ALL, EXCLUDE, COLUMNS, bare FROM)
        // into standard SQL that DataFusion understands.
        let processed_sql = crate::friendly_sql::preprocess_sql(&inlined_sql, &table_schemas)?;

        let df = ctx.sql(&processed_sql).await?;

        // Derive column-level lineage from the logical plan before execution.
        let lineage = node_id.map(|nid| {
            let table_to_node: HashMap<String, NodeId> = upstream_data
                .keys()
                .map(|uid| (uid.to_string(), uid.clone()))
                .collect();
            crate::column_lineage::derive_column_lineage(df.logical_plan(), nid, &table_to_node)
        });

        let df_schema = df.schema();
        let schema: Arc<Schema> = Arc::new(df_schema.as_arrow().clone());
        let batches = df.collect().await?;
        // Ensure we always return at least one batch so the schema is preserved
        // for downstream nodes that reference this table (e.g. 0-row join results).
        if batches.is_empty() {
            Ok((vec![RecordBatch::new_empty(schema)], lineage))
        } else {
            Ok((batches, lineage))
        }
    }

    /// Write data through the sink connector, returning the batches and the
    /// structured [`MaterializationReceipt`].
    ///
    /// When `sink_plan` is `Some` this is an incremental sink: the
    /// coordinator computes the new max watermark from the streamed
    /// batches, computes a schema diff against the most recent stored
    /// schema, applies the configured `on_schema_change` policy, calls
    /// the sink, then persists the new state and schema record.
    ///
    /// State persistence is **post-commit** (at-least-once). Wrapping the
    /// sink write and the state save in a single transaction would
    /// require threading a transaction handle through the `PipelineSink`
    /// trait — doc 27 explicitly defers that surgery to a follow-up. The
    /// seam is marked below with a TODO so the future change has an
    /// obvious landing pad.
    #[allow(clippy::too_many_arguments)]
    async fn execute_sink(
        node_id: &NodeId,
        config: &flux_engine::node::SinkConfig,
        batches: Vec<RecordBatch>,
        registry: &ProviderRegistry,
        sink_plan: Option<&IncrementalSinkPlan>,
        state_store: Option<&Arc<dyn IncrementalStateStorage>>,
        pipeline_id: &str,
        environment: &str,
        run_id: &crate::run::RunId,
        full_refresh: bool,
    ) -> Result<(Vec<RecordBatch>, MaterializationReceipt), NodeErrorKind> {
        let sink = registry
            .get_sink(&config.connector)
            .ok_or_else(|| NodeErrorKind::SinkNotFound(config.connector.clone()))?;

        let mut ctx = MaterializationContext::from_policy(config.materialization.as_ref());

        // -- Schema diff (incremental sinks only) ---------------------
        let mut schema_diff = None;
        if let (Some(plan), Some(store)) = (sink_plan, state_store) {
            if let Some(first_batch) = batches.first() {
                let current_schema = first_batch.schema();
                let prev = store
                    .latest_schema(pipeline_id, &node_id.0, environment)
                    .map_err(|e| {
                        NodeErrorKind::Sink(Box::new(std::io::Error::other(e.to_string())))
                    })?;
                if let Some(prev) = prev {
                    if let Some(prev_schema) =
                        crate::schema_diff::deserialize_schema(&prev.schema_json)
                    {
                        let diff = compute_schema_diff(&prev_schema, &current_schema);
                        match apply_policy(&diff, plan.policy.on_schema_change) {
                            SchemaAction::Proceed => {}
                            SchemaAction::ProceedWithAlter => {
                                // Signal sinks that support target-side
                                // schema evolution (PostgresSink under
                                // `append_new_columns`) to introspect and
                                // ALTER before writing. Sinks without
                                // evolution support ignore the flag; for
                                // those, log the seam so the operator can
                                // see why the receipt's schema_diff is
                                // populated but the target wasn't altered.
                                ctx.apply_schema_changes = true;
                                if !sink_supports_schema_evolution(&config.connector) {
                                    warn!(
                                        sink = %node_id,
                                        connector = %config.connector,
                                        "schema diff requires ALTER but this sink does not support target-side evolution; proceeding without altering target"
                                    );
                                }
                            }
                            SchemaAction::Abort(reason) => {
                                return Err(NodeErrorKind::Sink(Box::new(std::io::Error::other(
                                    format!("schema change rejected: {reason}"),
                                ))));
                            }
                        }
                        schema_diff = if diff.is_empty() { None } else { Some(diff) };
                    }
                }
            }
        }

        // -- Capture new max watermark BEFORE write so we can populate
        // -- receipt and persist regardless of what the sink mutates.
        let new_max_scalar = if let Some(plan) = sink_plan {
            let column = &plan
                .policy
                .watermark
                .as_ref()
                .expect("incremental policy guarantees watermark")
                .column;
            let wtype = plan
                .policy
                .watermark
                .as_ref()
                .expect("incremental policy guarantees watermark")
                .watermark_type;
            fold_max_watermark(&batches, column, wtype)
                .map_err(|e| NodeErrorKind::Sink(Box::new(std::io::Error::other(e.to_string()))))?
        } else {
            None
        };

        // -- Sink write -----------------------------------------------
        let mut receipt = sink
            .write(config, batches.clone(), &WriteOptions::default(), &ctx)
            .await
            .map_err(NodeErrorKind::Sink)?;

        // -- Receipt enrichment + state persistence -------------------
        if let Some(plan) = sink_plan {
            let watermark_field = plan
                .policy
                .watermark
                .as_ref()
                .expect("incremental policy guarantees watermark");
            // Watermark before
            receipt.watermark_before = plan.state.as_ref().map(|s| WatermarkValue {
                value: s.watermark_value.clone(),
                r#type: s.watermark_type.clone(),
            });
            // Watermark after — fall back to "before" if no rows seen
            // (pure no-op run), so the receipt is never silently empty.
            let new_value_str = new_max_scalar
                .as_ref()
                .and_then(|s| scalar_to_stored(watermark_field.watermark_type, s));
            let advanced_value = new_value_str
                .clone()
                .or_else(|| plan.state.as_ref().map(|s| s.watermark_value.clone()));
            receipt.watermark_after = advanced_value.clone().map(|v| WatermarkValue {
                value: v,
                r#type: watermark_type_str(watermark_field.watermark_type).into(),
            });
            receipt.schema_diff = schema_diff;

            // Persist new state + schema record. Doc 27: post-commit
            // advance, at-least-once. This is an explicit, documented
            // contract — see "Crash recovery and idempotency" in the user
            // guide. Use `merge` or `delete_insert` for idempotent targets;
            // `append` will duplicate on crash recovery.
            if let Some(store) = state_store {
                if let Some(value) = advanced_value {
                    let new_state = IncrementalState {
                        pipeline_id: pipeline_id.to_string(),
                        node_id: node_id.0.clone(),
                        environment: environment.to_string(),
                        watermark_column: watermark_field.column.clone(),
                        watermark_value: value,
                        watermark_type: watermark_type_str(watermark_field.watermark_type)
                            .to_string(),
                        last_run_at_ms: chrono::Utc::now().timestamp_millis(),
                        last_run_id: run_id.to_string(),
                        rows_processed: receipt.rows_written,
                        schema_fingerprint: batches
                            .first()
                            .map(|b| schema_fingerprint(&b.schema())),
                    };
                    if let Err(e) = store.save_state(&new_state) {
                        warn!(sink = %node_id, error = %e, "failed to persist incremental state");
                    }
                }
                if let Some(first_batch) = batches.first() {
                    let schema = first_batch.schema();
                    let json = crate::schema_diff::serialize_schema(&schema);
                    {
                        let record = IncrementalSchemaRecord {
                            pipeline_id: pipeline_id.to_string(),
                            node_id: node_id.0.clone(),
                            environment: environment.to_string(),
                            run_id: run_id.to_string(),
                            schema_json: json,
                            fingerprint: schema_fingerprint(&schema),
                            recorded_at_ms: chrono::Utc::now().timestamp_millis(),
                        };
                        if let Err(e) = store.record_schema(&record) {
                            warn!(sink = %node_id, error = %e, "failed to persist schema record");
                        }
                    }
                }
            }

            if full_refresh {
                debug!(sink = %node_id, "full_refresh: state advanced as if first run");
            }
        }

        Ok((batches, receipt))
    }
}

/// Stable wire string for a [`WatermarkType`]. Mirrors the JSON spelling
/// used by `materialization.rs`.
/// Sinks that honor `MaterializationContext::apply_schema_changes` by
/// introspecting the target and running an ALTER. Used by the executor to
/// decide whether to log the "diff requires ALTER but sink doesn't support
/// it" WARN seam — sinks listed here read the flag, sinks that don't get a
/// loud notice instead. Doc 27 follow-up: as additional sinks gain
/// evolution support, add them here.
fn sink_supports_schema_evolution(connector: &str) -> bool {
    matches!(connector, "postgresql" | "postgres")
}

fn watermark_type_str(t: flux_engine::materialization::WatermarkType) -> &'static str {
    use flux_engine::materialization::WatermarkType;
    match t {
        WatermarkType::Timestamp => "timestamp",
        WatermarkType::Int64 => "int64",
        WatermarkType::String => "string",
    }
}

/// Shallow-merge an environment override into a node's connector config.
///
/// If the override is a JSON object, its keys are merged into `base` (which
/// must also be a JSON object). Non-object overrides or bases are left
/// unchanged — the override simply replaces the base value.
fn merge_override(base: &mut Value, override_val: &Value) {
    if let (Some(base_map), Some(over_map)) = (base.as_object_mut(), override_val.as_object()) {
        for (k, v) in over_map {
            base_map.insert(k.clone(), v.clone());
        }
    }
}

/// Persist column-level lineage edges for a single node.
///
/// Converts [`NodeColumnLineage`] into [`StoredColumnEdge`] rows and saves
/// them. Errors are logged and swallowed — column lineage is best-effort
/// and must never block execution.
fn persist_column_lineage(
    store: &dyn ColumnLineageStorage,
    pipeline_id: &str,
    environment: &str,
    lineage: &flux_engine::NodeColumnLineage,
    run_id: Option<&RunId>,
    on_updated: Option<&ColumnLineageCallback>,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let edges: Vec<StoredColumnEdge> = lineage
        .edges
        .iter()
        .map(|edge| StoredColumnEdge {
            id: None,
            pipeline_id: pipeline_id.to_string(),
            environment: environment.to_string(),
            edge: edge.clone(),
            derived_at: now.clone(),
            source_run_id: run_id.map(|r| r.0.to_string()),
        })
        .collect();
    let edge_count = edges.len();
    if let Err(e) = store.save_column_edges(pipeline_id, environment, &edges) {
        warn!(
            pipeline = pipeline_id,
            error = %e,
            "failed to persist column lineage edges"
        );
    } else if let Some(cb) = on_updated {
        cb(pipeline_id, environment, edge_count);
    }
}

/// Extract column names from record batches for opaque lineage derivation.
fn extract_column_names(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default()
}

/// Send a progress event if a sender is available. Silently ignores closed channels.
fn emit(sender: &Option<mpsc::UnboundedSender<ExecutionEvent>>, event: ExecutionEvent) {
    if let Some(tx) = sender {
        let _ = tx.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merge_override_merges_keys() {
        let mut base = json!({"path": "/data/dev.csv", "format": "csv"});
        let over = json!({"path": "/data/prod.csv"});
        merge_override(&mut base, &over);
        assert_eq!(base["path"], "/data/prod.csv");
        assert_eq!(base["format"], "csv"); // untouched
    }

    #[test]
    fn merge_override_adds_new_keys() {
        let mut base = json!({"path": "/data/file.csv"});
        let over = json!({"format": "parquet"});
        merge_override(&mut base, &over);
        assert_eq!(base["path"], "/data/file.csv");
        assert_eq!(base["format"], "parquet");
    }

    #[test]
    fn merge_override_noop_for_non_objects() {
        let mut base = json!("scalar");
        let over = json!({"key": "val"});
        merge_override(&mut base, &over);
        assert_eq!(base, json!("scalar")); // unchanged
    }

    #[test]
    fn merge_override_empty_override_is_noop() {
        let mut base = json!({"path": "/data/file.csv"});
        let over = json!({});
        merge_override(&mut base, &over);
        assert_eq!(base["path"], "/data/file.csv");
    }

    /// A test `SecretResolver` that replaces `{{ secret:name }}` with `RESOLVED_<name>`.
    struct FakeSecretResolver;

    impl SecretResolver for FakeSecretResolver {
        fn resolve_json(
            &self,
            value: &Value,
            _environment: Option<&str>,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            let s = serde_json::to_string(value).unwrap();
            // Simple replacement for testing.
            let resolved = s.replace("{{ secret:db_pass }}", "s3cret");
            Ok(serde_json::from_str(&resolved).unwrap())
        }
    }

    #[test]
    fn secret_resolver_resolves_json() {
        let resolver = FakeSecretResolver;
        let input = json!({
            "connection_string": "postgres://user:{{ secret:db_pass }}@localhost/db",
            "table": "users"
        });
        let result = resolver.resolve_json(&input, Some("dev")).unwrap();
        assert_eq!(
            result["connection_string"],
            "postgres://user:s3cret@localhost/db"
        );
        assert_eq!(result["table"], "users"); // untouched
    }

    #[test]
    fn secret_resolver_none_leaves_config_unchanged() {
        // When no resolver is set, secret refs remain as-is.
        let input = json!({
            "connection_string": "postgres://user:{{ secret:db_pass }}@localhost/db"
        });
        let resolver: Option<Arc<dyn SecretResolver>> = None;
        // Simulate the executor logic.
        let result = match &resolver {
            Some(r) => r.resolve_json(&input, None).unwrap(),
            None => input.clone(),
        };
        assert_eq!(result, input);
    }
}
