// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end test for incremental sink materializations (planning doc 27).
//!
//! Drives a two-run pipeline through `PipelineExecutor` with a shared
//! `SqliteRunStore` backing both `RunStorage` and `IncrementalStateStorage`.
//! Asserts:
//!
//! 1. Run 1 (no state) reads everything and persists the max watermark.
//! 2. Run 2 (state present) filters at the source via the injected
//!    DataFusion expression and only the new rows reach the sink.
//! 3. The `MaterializationReceipt` carries `watermark_before/after` and the
//!    sink-side row counts the user actually cares about.
//! 4. `--full-refresh` skips filter injection and re-advances state from a
//!    fresh full scan.

use armillary_datafusion::provider::{
    MaterializationContext, MaterializationReceipt, PipelineSink, ProviderError, ProviderRegistry,
    SourceConnector, WriteOptions, WriteStats,
};
use armillary_datafusion::{
    ExecutionOptions, IncrementalStateStorage, PipelineExecutor, SqliteRunStore,
};
use armillary_engine::edge::Edge;
use armillary_engine::materialization::{
    MaterializationPolicy, OnSchemaChange, ReadMode, Watermark, WatermarkType, WriteStrategy,
};
use armillary_engine::node::*;
use armillary_engine::pipeline::Pipeline;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::{MemTable, TableProvider};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn id_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn batch_with_ids(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(id_schema(), vec![Arc::new(Int64Array::from(ids.to_vec()))]).unwrap()
}

/// Source whose returned batches are swappable between runs.
struct SwitchableSource {
    batches: Arc<Mutex<Vec<RecordBatch>>>,
}

impl SourceConnector for SwitchableSource {
    fn create_table_provider(
        &self,
        _config: &SourceConfig,
    ) -> Result<Arc<dyn TableProvider>, ProviderError> {
        let batches = self.batches.lock().unwrap().clone();
        let table = MemTable::try_new(id_schema(), vec![batches])?;
        Ok(Arc::new(table))
    }
}

/// Sink that captures everything written and reports `rows_written` so the
/// receipt has a real number to advance against.
struct CapturingSink {
    captured: Arc<Mutex<Vec<RecordBatch>>>,
}

#[async_trait]
impl PipelineSink for CapturingSink {
    async fn write(
        &self,
        _config: &SinkConfig,
        data: Vec<RecordBatch>,
        _options: &WriteOptions,
        ctx: &MaterializationContext,
    ) -> Result<MaterializationReceipt, ProviderError> {
        let row_count: u64 = data.iter().map(|b| b.num_rows() as u64).sum();
        self.captured.lock().unwrap().extend(data);
        let stats = WriteStats {
            rows_written: row_count,
            bytes_written: 0,
            duration: Duration::ZERO,
        };
        Ok(MaterializationReceipt::from_write_stats(&stats, ctx))
    }

    fn validate_config(&self, _config: &SinkConfig) -> Result<(), ProviderError> {
        Ok(())
    }
}

fn incremental_pipeline() -> Pipeline {
    Pipeline {
        name: "incr".into(),
        version: 1,
        default_environment: "dev".into(),
        variables: BTreeMap::new(),
        environment_overrides: BTreeMap::new(),
        sample_config: None,
        cache_row_limit: None,
        code_dir: None,
        udfs_dir: None,
        snippets_dir: None,
        snippet: None,
        params: BTreeMap::new(),
        outputs: Vec::new(),
        nodes: vec![
            Node {
                id: NodeId::new("src"),
                name: "src".into(),
                kind: NodeKind::Source(SourceConfig {
                    connector: "mock".into(),
                    config: serde_json::Value::Null,
                    cache_row_limit: None,
                }),
                position: Position::default(),
                pinned_position: false,
                snippet_parent: None,
                snippet_name: None,
            },
            Node {
                id: NodeId::new("sink"),
                name: "sink".into(),
                kind: NodeKind::Sink(SinkConfig {
                    connector: "mock".into(),
                    config: serde_json::Value::Null,
                    materialization: Some(MaterializationPolicy {
                        read_mode: ReadMode::Incremental,
                        write_strategy: WriteStrategy::Append,
                        watermark: Some(Watermark {
                            column: "id".into(),
                            watermark_type: WatermarkType::Int64,
                        }),
                        ..Default::default()
                    }),
                }),
                position: Position::default(),
                pinned_position: false,
                snippet_parent: None,
                snippet_name: None,
            },
        ],
        edges: vec![Edge::new("src", "sink")],
    }
}

#[tokio::test]
async fn two_runs_advance_watermark_and_filter_source() {
    let source_batches = Arc::new(Mutex::new(vec![batch_with_ids(&[1, 2, 3, 4, 5])]));
    let sink_capture: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));

    let mut registry = ProviderRegistry::new();
    registry.register_source(
        "mock",
        Arc::new(SwitchableSource {
            batches: Arc::clone(&source_batches),
        }),
    );
    registry.register_sink(
        "mock",
        Arc::new(CapturingSink {
            captured: Arc::clone(&sink_capture),
        }),
    );

    let store = Arc::new(SqliteRunStore::open_in_memory().unwrap());
    let state_store: Arc<dyn IncrementalStateStorage> = store.clone();

    let pipeline = incremental_pipeline();

    // -- Run 1: no state, full read of 5 rows. ------------------------
    let opts1 = ExecutionOptions {
        environment: "dev".into(),
        run_store: Some(store.clone()),
        incremental_state_store: Some(Arc::clone(&state_store)),
        ..ExecutionOptions::default()
    };
    let (_result, run1) = PipelineExecutor::execute(&pipeline, &registry, &opts1)
        .await
        .expect("run 1 should succeed");

    let receipt1 = run1
        .node_stats
        .iter()
        .find(|s| s.node_id == NodeId::new("sink"))
        .and_then(|s| s.materialization_receipt.as_ref())
        .expect("sink should produce a receipt");
    assert_eq!(receipt1.rows_written, 5);
    assert!(
        receipt1.watermark_before.is_none(),
        "first run has no prior watermark"
    );
    let after1 = receipt1
        .watermark_after
        .as_ref()
        .expect("first run advances watermark");
    assert_eq!(after1.value, "5");
    assert_eq!(after1.r#type, "int64");

    // State row exists in the metadata store.
    let stored = state_store
        .load_state("incr", "sink", "dev")
        .unwrap()
        .expect("state should be persisted");
    assert_eq!(stored.watermark_value, "5");

    // -- Run 2: source now has 10 rows; filter `id > 5` keeps 6..10. ---
    *source_batches.lock().unwrap() = vec![batch_with_ids(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])];
    sink_capture.lock().unwrap().clear();

    let opts2 = ExecutionOptions {
        environment: "dev".into(),
        run_store: Some(store.clone()),
        incremental_state_store: Some(Arc::clone(&state_store)),
        ..ExecutionOptions::default()
    };
    let (_result, run2) = PipelineExecutor::execute(&pipeline, &registry, &opts2)
        .await
        .expect("run 2 should succeed");

    let receipt2 = run2
        .node_stats
        .iter()
        .find(|s| s.node_id == NodeId::new("sink"))
        .and_then(|s| s.materialization_receipt.as_ref())
        .expect("sink should produce a receipt");
    assert_eq!(receipt2.rows_written, 5, "filter `id > 5` keeps rows 6..10");
    assert_eq!(receipt2.watermark_before.as_ref().unwrap().value, "5");
    assert_eq!(receipt2.watermark_after.as_ref().unwrap().value, "10");

    // Verify the captured rows on the sink side are exactly the new ones.
    let captured_total: usize = sink_capture
        .lock()
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(captured_total, 5);

    // -- Run 3: --full-refresh re-reads everything and advances state. -
    sink_capture.lock().unwrap().clear();
    let opts3 = ExecutionOptions {
        environment: "dev".into(),
        run_store: Some(store.clone()),
        incremental_state_store: Some(Arc::clone(&state_store)),
        full_refresh: true,
        ..ExecutionOptions::default()
    };
    let (_, run3) = PipelineExecutor::execute(&pipeline, &registry, &opts3)
        .await
        .expect("run 3 should succeed");
    let receipt3 = run3
        .node_stats
        .iter()
        .find(|s| s.node_id == NodeId::new("sink"))
        .and_then(|s| s.materialization_receipt.as_ref())
        .expect("sink should produce a receipt");
    // No filter applied → all 10 rows reach the sink.
    assert_eq!(receipt3.rows_written, 10);
    assert_eq!(receipt3.watermark_after.as_ref().unwrap().value, "10");
}

// ---------------------------------------------------------------------------
// Helpers + tests for schema-change handling, late-arriving data via lookback,
// and crash recovery (planning doc 27 — testing section).
// ---------------------------------------------------------------------------

/// Source whose schema is derived from whatever batches are currently set.
/// Lets a test swap in batches with an evolved schema between runs.
struct DynSchemaSource {
    batches: Arc<Mutex<Vec<RecordBatch>>>,
}

impl SourceConnector for DynSchemaSource {
    fn create_table_provider(
        &self,
        _config: &SourceConfig,
    ) -> Result<Arc<dyn TableProvider>, ProviderError> {
        let batches = self.batches.lock().unwrap().clone();
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(id_schema);
        let table = MemTable::try_new(schema, vec![batches])?;
        Ok(Arc::new(table))
    }
}

/// Sink that captures rows AND the materialization context it received, and
/// can be flipped to fail on demand to simulate a mid-write crash.
struct FlexibleSink {
    captured: Arc<Mutex<Vec<RecordBatch>>>,
    last_ctx: Arc<Mutex<Option<MaterializationContext>>>,
    fail: Arc<AtomicBool>,
}

#[async_trait]
impl PipelineSink for FlexibleSink {
    async fn write(
        &self,
        _config: &SinkConfig,
        data: Vec<RecordBatch>,
        _options: &WriteOptions,
        ctx: &MaterializationContext,
    ) -> Result<MaterializationReceipt, ProviderError> {
        *self.last_ctx.lock().unwrap() = Some(ctx.clone());
        if self.fail.load(Ordering::SeqCst) {
            return Err("simulated sink failure".into());
        }
        let row_count: u64 = data.iter().map(|b| b.num_rows() as u64).sum();
        self.captured.lock().unwrap().extend(data);
        let stats = WriteStats {
            rows_written: row_count,
            bytes_written: 0,
            duration: Duration::ZERO,
        };
        Ok(MaterializationReceipt::from_write_stats(&stats, ctx))
    }

    fn validate_config(&self, _config: &SinkConfig) -> Result<(), ProviderError> {
        Ok(())
    }
}

fn pipeline_with_policy(policy: MaterializationPolicy) -> Pipeline {
    Pipeline {
        name: "incr".into(),
        version: 1,
        default_environment: "dev".into(),
        variables: BTreeMap::new(),
        environment_overrides: BTreeMap::new(),
        sample_config: None,
        cache_row_limit: None,
        code_dir: None,
        udfs_dir: None,
        snippets_dir: None,
        snippet: None,
        params: BTreeMap::new(),
        outputs: Vec::new(),
        nodes: vec![
            Node {
                id: NodeId::new("src"),
                name: "src".into(),
                kind: NodeKind::Source(SourceConfig {
                    connector: "mock".into(),
                    config: serde_json::Value::Null,
                    cache_row_limit: None,
                }),
                position: Position::default(),
                pinned_position: false,
                snippet_parent: None,
                snippet_name: None,
            },
            Node {
                id: NodeId::new("sink"),
                name: "sink".into(),
                kind: NodeKind::Sink(SinkConfig {
                    connector: "mock".into(),
                    config: serde_json::Value::Null,
                    materialization: Some(policy),
                }),
                position: Position::default(),
                pinned_position: false,
                snippet_parent: None,
                snippet_name: None,
            },
        ],
        edges: vec![Edge::new("src", "sink")],
    }
}

fn int64_id_policy(on_schema_change: OnSchemaChange) -> MaterializationPolicy {
    MaterializationPolicy {
        read_mode: ReadMode::Incremental,
        write_strategy: WriteStrategy::Append,
        watermark: Some(Watermark {
            column: "id".into(),
            watermark_type: WatermarkType::Int64,
        }),
        on_schema_change,
        ..Default::default()
    }
}

fn id_name_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn batch_id_name(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        id_name_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

fn build_registry(
    source_batches: Arc<Mutex<Vec<RecordBatch>>>,
    sink: Arc<FlexibleSink>,
) -> ProviderRegistry {
    let mut registry = ProviderRegistry::new();
    registry.register_source(
        "mock",
        Arc::new(DynSchemaSource {
            batches: source_batches,
        }),
    );
    registry.register_sink("mock", sink);
    registry
}

/// Run 1 streams `{id}` only, run 2 streams `{id, name}` (a new column).
/// Under `on_schema_change: append_new_columns`, the run must succeed, the
/// receipt's `schema_diff.added` must list `name`, and the executor must
/// signal `apply_schema_changes = true` on the materialization context so
/// schema-evolution-aware sinks (PostgresSink) can ALTER the target.
#[tokio::test]
async fn schema_change_append_new_columns_proceeds_with_alter_signal() {
    let source_batches = Arc::new(Mutex::new(vec![batch_with_ids(&[1, 2, 3])]));
    let captured: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
    let last_ctx: Arc<Mutex<Option<MaterializationContext>>> = Arc::new(Mutex::new(None));
    let sink = Arc::new(FlexibleSink {
        captured: Arc::clone(&captured),
        last_ctx: Arc::clone(&last_ctx),
        fail: Arc::new(AtomicBool::new(false)),
    });
    let registry = build_registry(Arc::clone(&source_batches), Arc::clone(&sink));

    let store = Arc::new(SqliteRunStore::open_in_memory().unwrap());
    let state_store: Arc<dyn IncrementalStateStorage> = store.clone();
    let pipeline = pipeline_with_policy(int64_id_policy(OnSchemaChange::AppendNewColumns));

    let opts = || ExecutionOptions {
        environment: "dev".into(),
        run_store: Some(store.clone()),
        incremental_state_store: Some(Arc::clone(&state_store)),
        ..ExecutionOptions::default()
    };

    // Run 1: schema = {id}
    let (_r, run1) = PipelineExecutor::execute(&pipeline, &registry, &opts())
        .await
        .expect("run 1 should succeed");
    let receipt1 = run1
        .node_stats
        .iter()
        .find(|s| s.node_id == NodeId::new("sink"))
        .and_then(|s| s.materialization_receipt.as_ref())
        .unwrap();
    assert!(receipt1.schema_diff.is_none(), "no prior schema → no diff");
    // First-run ctx should not signal an ALTER (nothing to compare against).
    assert!(
        !last_ctx
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .apply_schema_changes
    );

    // Run 2: schema = {id, name} — new `name` column.
    *source_batches.lock().unwrap() = vec![batch_id_name(&[4, 5], &["alice", "bob"])];
    let (_r, run2) = PipelineExecutor::execute(&pipeline, &registry, &opts())
        .await
        .expect("run 2 with added column should succeed under append_new_columns");

    let receipt2 = run2
        .node_stats
        .iter()
        .find(|s| s.node_id == NodeId::new("sink"))
        .and_then(|s| s.materialization_receipt.as_ref())
        .expect("sink should produce a receipt");
    let diff = receipt2
        .schema_diff
        .as_ref()
        .expect("schema added a column → diff must be populated");
    assert_eq!(diff.added.len(), 1);
    assert_eq!(diff.added[0].name, "name");
    assert!(diff.removed.is_empty());
    assert!(diff.type_changed.is_empty());

    // Executor must have signalled apply_schema_changes to the sink so
    // schema-evolution-aware connectors (PostgresSink) can ALTER the target.
    let ctx = last_ctx.lock().unwrap().clone().unwrap();
    assert!(
        ctx.apply_schema_changes,
        "ProceedWithAlter must surface as ctx.apply_schema_changes = true"
    );
}

/// Same setup, but `on_schema_change: fail` aborts run 2 with a clear diff
/// in the error message and leaves the persisted state at run 1's value.
#[tokio::test]
async fn schema_change_fail_policy_aborts_and_preserves_state() {
    let source_batches = Arc::new(Mutex::new(vec![batch_with_ids(&[1, 2, 3])]));
    let sink = Arc::new(FlexibleSink {
        captured: Arc::new(Mutex::new(Vec::new())),
        last_ctx: Arc::new(Mutex::new(None)),
        fail: Arc::new(AtomicBool::new(false)),
    });
    let registry = build_registry(Arc::clone(&source_batches), Arc::clone(&sink));

    let store = Arc::new(SqliteRunStore::open_in_memory().unwrap());
    let state_store: Arc<dyn IncrementalStateStorage> = store.clone();
    let pipeline = pipeline_with_policy(int64_id_policy(OnSchemaChange::Fail));

    let opts = || ExecutionOptions {
        environment: "dev".into(),
        run_store: Some(store.clone()),
        incremental_state_store: Some(Arc::clone(&state_store)),
        ..ExecutionOptions::default()
    };

    // Run 1: bootstrap with {id} only.
    PipelineExecutor::execute(&pipeline, &registry, &opts())
        .await
        .expect("run 1 should succeed");
    let state_after_run1 = state_store
        .load_state("incr", "sink", "dev")
        .unwrap()
        .unwrap();
    assert_eq!(state_after_run1.watermark_value, "3");

    // Run 2: schema changes — must abort under Fail.
    *source_batches.lock().unwrap() = vec![batch_id_name(&[4, 5], &["a", "b"])];
    let result = PipelineExecutor::execute(&pipeline, &registry, &opts()).await;
    assert!(
        result.is_err(),
        "Fail policy must abort the run when schema changes"
    );
    let err_msg = format!("{:?}", result.err().unwrap());
    assert!(
        err_msg.contains("schema change") || err_msg.contains("added"),
        "error should describe the schema change, got: {err_msg}"
    );

    // State must NOT have advanced past run 1's watermark.
    let state_after_fail = state_store
        .load_state("incr", "sink", "dev")
        .unwrap()
        .unwrap();
    assert_eq!(
        state_after_fail.watermark_value, "3",
        "aborted run must not advance watermark"
    );
}

/// If the sink fails mid-write, the executor must propagate the error and
/// must NOT advance incremental state. Next run replays the same rows
/// (at-least-once) — this is the contract doc 27 promises so users picking
/// `merge` get idempotency for free.
#[tokio::test]
async fn sink_failure_does_not_advance_watermark_state() {
    let source_batches = Arc::new(Mutex::new(vec![batch_with_ids(&[1, 2, 3, 4, 5])]));
    let fail = Arc::new(AtomicBool::new(false));
    let sink = Arc::new(FlexibleSink {
        captured: Arc::new(Mutex::new(Vec::new())),
        last_ctx: Arc::new(Mutex::new(None)),
        fail: Arc::clone(&fail),
    });
    let registry = build_registry(Arc::clone(&source_batches), Arc::clone(&sink));

    let store = Arc::new(SqliteRunStore::open_in_memory().unwrap());
    let state_store: Arc<dyn IncrementalStateStorage> = store.clone();
    let pipeline = pipeline_with_policy(int64_id_policy(OnSchemaChange::AppendNewColumns));

    let opts = || ExecutionOptions {
        environment: "dev".into(),
        run_store: Some(store.clone()),
        incremental_state_store: Some(Arc::clone(&state_store)),
        ..ExecutionOptions::default()
    };

    // Run 1 succeeds → watermark = 5.
    PipelineExecutor::execute(&pipeline, &registry, &opts())
        .await
        .expect("run 1 should succeed");
    let after_ok = state_store
        .load_state("incr", "sink", "dev")
        .unwrap()
        .unwrap();
    assert_eq!(after_ok.watermark_value, "5");

    // Run 2: source has new rows 6..10, but the sink fails.
    *source_batches.lock().unwrap() = vec![batch_with_ids(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])];
    fail.store(true, Ordering::SeqCst);
    let result = PipelineExecutor::execute(&pipeline, &registry, &opts()).await;
    assert!(result.is_err(), "sink failure must surface as a run error");

    // State must remain at the run 1 value — the sink crash leaves rows 6..10
    // available for the next attempt. This is at-least-once by construction.
    let after_fail = state_store
        .load_state("incr", "sink", "dev")
        .unwrap()
        .unwrap();
    assert_eq!(
        after_fail.watermark_value, "5",
        "sink failure must not advance watermark"
    );

    // Recovery: clear the fail flag and re-run; state should now advance to 10.
    fail.store(false, Ordering::SeqCst);
    PipelineExecutor::execute(&pipeline, &registry, &opts())
        .await
        .expect("recovery run should succeed");
    let after_recovery = state_store
        .load_state("incr", "sink", "dev")
        .unwrap()
        .unwrap();
    assert_eq!(after_recovery.watermark_value, "10");
}
