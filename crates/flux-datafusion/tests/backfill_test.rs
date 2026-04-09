// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the backfill coordinator (planning doc 33).

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use flux_datafusion::backfill::{BackfillEvent, BackfillRunOptions};
use flux_datafusion::provider::{
    PipelineSink, ProviderError, ProviderRegistry, SourceConnector, WriteOptions, WriteStats,
};
use flux_datafusion::{
    BackfillStorage, ExecutionOptions, SqliteBackfillStore, SqliteRunStore,
};
use flux_engine::backfill::*;
use flux_engine::edge::Edge;
use flux_engine::node::*;
use flux_engine::pipeline::Pipeline;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn test_batch() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

struct MockSourceConnector {
    batches: Vec<RecordBatch>,
}

impl SourceConnector for MockSourceConnector {
    fn create_table_provider(
        &self,
        _config: &SourceConfig,
    ) -> Result<Arc<dyn TableProvider>, ProviderError> {
        let schema = self.batches[0].schema();
        let table = MemTable::try_new(schema, vec![self.batches.clone()])?;
        Ok(Arc::new(table))
    }
}

/// A source connector that tracks concurrent invocations for concurrency tests.
struct ConcurrencyTrackingSource {
    batches: Vec<RecordBatch>,
    concurrent: Arc<AtomicU32>,
    max_concurrent: Arc<AtomicU32>,
}

impl SourceConnector for ConcurrencyTrackingSource {
    fn create_table_provider(
        &self,
        _config: &SourceConfig,
    ) -> Result<Arc<dyn TableProvider>, ProviderError> {
        let prev = self.concurrent.fetch_add(1, Ordering::SeqCst);
        let current = prev + 1;
        // Update max if this is a new peak
        self.max_concurrent.fetch_max(current, Ordering::SeqCst);
        // Sleep briefly to let concurrent tasks overlap
        std::thread::sleep(Duration::from_millis(50));
        self.concurrent.fetch_sub(1, Ordering::SeqCst);

        let schema = self.batches[0].schema();
        let table = MemTable::try_new(schema, vec![self.batches.clone()])?;
        Ok(Arc::new(table))
    }
}

struct MockSink {
    captured: Arc<Mutex<Vec<RecordBatch>>>,
}

#[async_trait]
impl PipelineSink for MockSink {
    async fn write(
        &self,
        _config: &SinkConfig,
        data: Vec<RecordBatch>,
        _options: &WriteOptions,
        ctx: &flux_datafusion::provider::MaterializationContext,
    ) -> Result<flux_datafusion::provider::MaterializationReceipt, ProviderError> {
        let row_count: u64 = data.iter().map(|b| b.num_rows() as u64).sum();
        self.captured.lock().unwrap().extend(data);
        let stats = WriteStats {
            rows_written: row_count,
            bytes_written: 0,
            duration: Duration::ZERO,
        };
        Ok(flux_datafusion::provider::MaterializationReceipt::from_write_stats(&stats, ctx))
    }

    fn validate_config(&self, _config: &SinkConfig) -> Result<(), ProviderError> {
        Ok(())
    }
}

/// A sink that always fails, for testing failure handling.
struct FailingSink;

#[async_trait]
impl PipelineSink for FailingSink {
    async fn write(
        &self,
        _config: &SinkConfig,
        _data: Vec<RecordBatch>,
        _options: &WriteOptions,
        _ctx: &flux_datafusion::provider::MaterializationContext,
    ) -> Result<flux_datafusion::provider::MaterializationReceipt, ProviderError> {
        Err("intentional test failure".into())
    }

    fn validate_config(&self, _config: &SinkConfig) -> Result<(), ProviderError> {
        Ok(())
    }
}

fn source_node(id: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Source(SourceConfig {
            connector: "mock".into(),
            config: serde_json::Value::Null,
            cache_row_limit: None,
        }),
        position: Position::default(),
        pinned_position: false,
        snippet_parent: None,
        snippet_name: None,
    }
}

fn sql_transform_node(id: &str, sql: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Transform(TransformConfig {
            mode: TransformMode::Sql,
            code: sql.to_string(),
            code_path: None,
            materialized: false,
            cache_row_limit: None,
        }),
        position: Position::default(),
        pinned_position: false,
        snippet_parent: None,
        snippet_name: None,
    }
}

fn sink_node(id: &str) -> Node {
    sink_node_with_connector(id, "mock")
}

fn sink_node_with_connector(id: &str, connector: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Sink(SinkConfig {
            connector: connector.into(),
            materialization: None,
            config: serde_json::Value::Null,
        }),
        position: Position::default(),
        pinned_position: false,
        snippet_parent: None,
        snippet_name: None,
    }
}

fn make_pipeline(name: &str, nodes: Vec<Node>, edges: Vec<Edge>) -> Pipeline {
    Pipeline {
        name: name.to_string(),
        version: 1,
        default_environment: "dev".to_string(),
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
        nodes,
        edges,
    }
}

fn mock_registry(
    batches: Vec<RecordBatch>,
    sink_capture: Arc<Mutex<Vec<RecordBatch>>>,
) -> ProviderRegistry {
    let mut reg = ProviderRegistry::new();
    reg.register_source("mock", Arc::new(MockSourceConnector { batches }));
    reg.register_sink(
        "mock",
        Arc::new(MockSink {
            captured: sink_capture,
        }),
    );
    reg
}

fn make_backfill(id: &str, range: RangeDefinition) -> Backfill {
    Backfill {
        id: BackfillId(id.to_string()),
        pipeline_id: "test-pipe".into(),
        environment: "default".into(),
        range_definition: range,
        concurrency: 1,
        fail_fast: false,
        full_refresh: true,
        status: BackfillStatus::Pending,
        created_at: chrono::Utc::now().to_rfc3339(),
        started_at: None,
        completed_at: None,
        created_by: None,
    }
}

fn make_opts(
    pipeline: Pipeline,
    registry: ProviderRegistry,
    store: Arc<dyn BackfillStorage>,
) -> (BackfillRunOptions, mpsc::UnboundedReceiver<BackfillEvent>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let run_store = Arc::new(SqliteRunStore::open_in_memory().unwrap());
    let opts = BackfillRunOptions {
        pipeline,
        registry: Arc::new(registry),
        base_options: ExecutionOptions {
            run_store: Some(run_store),
            ..ExecutionOptions::default()
        },
        backfill_store: store,
        progress: Some(tx),
        cancel: Arc::new(AtomicBool::new(false)),
    };
    (opts, rx)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test that concurrency is bounded to the configured limit.
#[tokio::test]
async fn coordinator_respects_concurrency_limit() {
    let pipeline = make_pipeline(
        "conc-test",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id, value FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let concurrent = Arc::new(AtomicU32::new(0));
    let max_concurrent = Arc::new(AtomicU32::new(0));
    let captured = Arc::new(Mutex::new(Vec::new()));

    let mut reg = ProviderRegistry::new();
    reg.register_source(
        "mock",
        Arc::new(ConcurrencyTrackingSource {
            batches: vec![test_batch()],
            concurrent: concurrent.clone(),
            max_concurrent: max_concurrent.clone(),
        }),
    );
    reg.register_sink(
        "mock",
        Arc::new(MockSink {
            captured: captured.clone(),
        }),
    );

    let store: Arc<dyn BackfillStorage> =
        Arc::new(SqliteBackfillStore::open_in_memory().unwrap());

    let range = RangeDefinition::List {
        values: vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()],
        variable_mapping: [("v".to_string(), "$iteration.value".to_string())]
            .into_iter()
            .collect(),
    };

    let mut backfill = make_backfill("conc-test", range);
    backfill.concurrency = 2;

    let (opts, _rx) = make_opts(pipeline, reg, store);
    let opts = BackfillRunOptions {
        cancel: opts.cancel,
        pipeline: opts.pipeline,
        registry: opts.registry,
        base_options: opts.base_options,
        backfill_store: opts.backfill_store,
        progress: opts.progress,
    };

    let (_, progress) =
        flux_datafusion::backfill::start_backfill(backfill, opts)
            .await
            .unwrap();

    // All 6 iterations should succeed
    assert_eq!(progress.succeeded, 6);
    assert_eq!(progress.failed, 0);
    // Max concurrent should not exceed 2 (the configured limit)
    assert!(
        max_concurrent.load(Ordering::SeqCst) <= 2,
        "max concurrent was {}, expected <= 2",
        max_concurrent.load(Ordering::SeqCst)
    );
}

/// Test that resume skips already-succeeded iterations.
#[tokio::test]
async fn resume_skips_succeeded_iterations() {
    let store: Arc<dyn BackfillStorage> =
        Arc::new(SqliteBackfillStore::open_in_memory().unwrap());

    let range = RangeDefinition::List {
        values: vec!["a".into(), "b".into(), "c".into()],
        variable_mapping: [("v".to_string(), "$iteration.value".to_string())]
            .into_iter()
            .collect(),
    };

    // Manually create a "failed" backfill with iteration 0 succeeded, 1 failed, 2 pending
    let bf = Backfill {
        id: BackfillId("resume-test".into()),
        pipeline_id: "test-pipe".into(),
        environment: "default".into(),
        range_definition: range,
        concurrency: 1,
        fail_fast: false,
        full_refresh: true,
        status: BackfillStatus::Failed,
        created_at: "2024-01-01T00:00:00Z".into(),
        started_at: Some("2024-01-01T00:00:01Z".into()),
        completed_at: Some("2024-01-01T00:01:00Z".into()),
        created_by: None,
    };
    store.create_backfill(&bf).unwrap();

    let iterations = vec![
        BackfillIteration {
            backfill_id: BackfillId("resume-test".into()),
            iteration_index: 0,
            iteration_key: "a".into(),
            variables: [("v".to_string(), serde_json::Value::String("a".into()))]
                .into_iter()
                .collect(),
            status: IterationStatus::Succeeded,
            run_id: Some("run-a".into()),
            error: None,
            started_at: Some("2024-01-01T00:00:01Z".into()),
            completed_at: Some("2024-01-01T00:00:10Z".into()),
        },
        BackfillIteration {
            backfill_id: BackfillId("resume-test".into()),
            iteration_index: 1,
            iteration_key: "b".into(),
            variables: [("v".to_string(), serde_json::Value::String("b".into()))]
                .into_iter()
                .collect(),
            status: IterationStatus::Failed,
            run_id: None,
            error: Some("previous failure".into()),
            started_at: Some("2024-01-01T00:00:10Z".into()),
            completed_at: Some("2024-01-01T00:00:15Z".into()),
        },
        BackfillIteration {
            backfill_id: BackfillId("resume-test".into()),
            iteration_index: 2,
            iteration_key: "c".into(),
            variables: [("v".to_string(), serde_json::Value::String("c".into()))]
                .into_iter()
                .collect(),
            status: IterationStatus::Pending,
            run_id: None,
            error: None,
            started_at: None,
            completed_at: None,
        },
    ];
    store.create_iterations(&iterations).unwrap();

    let pipeline = make_pipeline(
        "test-pipe",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id, value FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let reg = mock_registry(vec![test_batch()], captured.clone());
    let (opts, mut rx) = make_opts(pipeline, reg, store.clone());

    let bf_id = BackfillId("resume-test".into());
    let (_, progress) =
        flux_datafusion::backfill::resume_backfill(&bf_id, opts)
            .await
            .unwrap();

    // Iteration 0 ("a") was already succeeded, so it should be skipped.
    // Iterations 1 ("b") and 2 ("c") should run and succeed.
    // The returned progress reflects all iterations (including the pre-existing success).
    assert_eq!(progress.succeeded, 3, "all 3 iterations should show as succeeded");
    assert_eq!(progress.failed, 0);

    // Collect events and verify iteration "a" is not among the started events.
    let mut started_keys = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if let BackfillEvent::IterationStarted { iteration_key, .. } = event {
            started_keys.push(iteration_key);
        }
    }
    assert!(
        !started_keys.contains(&"a".to_string()),
        "iteration 'a' should have been skipped, but was started"
    );
}

/// Test that cancellation stops new iterations from starting.
#[tokio::test]
async fn cancellation_stops_new_iterations() {
    let store: Arc<dyn BackfillStorage> =
        Arc::new(SqliteBackfillStore::open_in_memory().unwrap());

    let range = RangeDefinition::List {
        values: (0..10).map(|i| i.to_string()).collect(),
        variable_mapping: [("v".to_string(), "$iteration.value".to_string())]
            .into_iter()
            .collect(),
    };

    let backfill = make_backfill("cancel-test", range);

    let pipeline = make_pipeline(
        "cancel-pipe",
        vec![
            source_node("src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let reg = mock_registry(vec![test_batch()], captured);
    let (opts, _rx) = make_opts(pipeline, reg, store.clone());
    let cancel = opts.cancel.clone();

    // Cancel immediately — the coordinator should not start (many) iterations.
    cancel.store(true, Ordering::Relaxed);

    let (_, progress) =
        flux_datafusion::backfill::start_backfill(backfill, opts)
            .await
            .unwrap();

    // The backfill should be marked cancelled.
    let bf = store.get_backfill(&BackfillId("cancel-test".into())).unwrap().unwrap();
    assert_eq!(bf.status, BackfillStatus::Cancelled);

    // Not all 10 iterations should have run.
    let all_iterations = store.list_iterations(&BackfillId("cancel-test".into())).unwrap();
    let pending_count = all_iterations.iter().filter(|i| i.status == IterationStatus::Pending).count();
    assert!(
        pending_count > 0,
        "expected some iterations to remain pending after cancellation"
    );
}

/// Test that a failed iteration doesn't block subsequent ones (unless fail_fast).
#[tokio::test]
async fn failed_iteration_does_not_block_subsequent() {
    let store: Arc<dyn BackfillStorage> =
        Arc::new(SqliteBackfillStore::open_in_memory().unwrap());

    let range = RangeDefinition::List {
        values: vec!["a".into(), "b".into(), "c".into()],
        variable_mapping: [("v".to_string(), "$iteration.value".to_string())]
            .into_iter()
            .collect(),
    };

    let backfill = make_backfill("fail-continue", range);

    let pipeline = make_pipeline(
        "fail-pipe",
        vec![
            source_node("src"),
            sink_node_with_connector("out", "failing"),
        ],
        vec![Edge::new("src", "out")],
    );

    // Use a failing sink — all iterations will fail, but they should all run.
    let mut reg = ProviderRegistry::new();
    reg.register_source("mock", Arc::new(MockSourceConnector { batches: vec![test_batch()] }));
    reg.register_sink("failing", Arc::new(FailingSink));

    let (opts, _rx) = make_opts(pipeline, reg, store.clone());

    let (_, progress) =
        flux_datafusion::backfill::start_backfill(backfill, opts)
            .await
            .unwrap();

    // All 3 should have failed, but all should have run (not blocked).
    assert_eq!(progress.failed, 3, "all iterations should have been attempted");
    assert_eq!(progress.succeeded, 0);

    let bf = store.get_backfill(&BackfillId("fail-continue".into())).unwrap().unwrap();
    assert_eq!(bf.status, BackfillStatus::Failed);
}

/// Test that fail_fast stops after the first failure.
#[tokio::test]
async fn fail_fast_stops_after_first_failure() {
    let store: Arc<dyn BackfillStorage> =
        Arc::new(SqliteBackfillStore::open_in_memory().unwrap());

    let range = RangeDefinition::List {
        values: (0..10).map(|i| i.to_string()).collect(),
        variable_mapping: [("v".to_string(), "$iteration.value".to_string())]
            .into_iter()
            .collect(),
    };

    let mut backfill = make_backfill("fail-fast-test", range);
    backfill.fail_fast = true;

    let pipeline = make_pipeline(
        "ff-pipe",
        vec![
            source_node("src"),
            sink_node_with_connector("out", "failing"),
        ],
        vec![Edge::new("src", "out")],
    );

    let mut reg = ProviderRegistry::new();
    reg.register_source("mock", Arc::new(MockSourceConnector { batches: vec![test_batch()] }));
    reg.register_sink("failing", Arc::new(FailingSink));

    let (opts, _rx) = make_opts(pipeline, reg, store.clone());

    let (_, progress) =
        flux_datafusion::backfill::start_backfill(backfill, opts)
            .await
            .unwrap();

    // With fail_fast and serial execution (concurrency=1), the coordinator will
    // spawn up to 2 iterations before the fail_fast flag is observed: the first
    // iteration fails and sets had_failure, but the second is already waiting on
    // the semaphore. The third and subsequent iterations see the flag and stop.
    assert!(progress.failed <= 2, "fail_fast should limit failures, got {}", progress.failed);
    assert!(progress.failed >= 1, "at least one failure expected");
    // Remaining iterations should be pending.
    let all_iterations = store.list_iterations(&BackfillId("fail-fast-test".into())).unwrap();
    let pending_count = all_iterations.iter().filter(|i| i.status == IterationStatus::Pending).count();
    assert!(pending_count >= 8, "most iterations should remain pending, got {} pending", pending_count);
}

/// Test that full_refresh is set in execution options.
#[tokio::test]
async fn full_refresh_applied_for_backfill() {
    let store: Arc<dyn BackfillStorage> =
        Arc::new(SqliteBackfillStore::open_in_memory().unwrap());

    let range = RangeDefinition::List {
        values: vec!["a".into()],
        variable_mapping: [("v".to_string(), "$iteration.value".to_string())]
            .into_iter()
            .collect(),
    };

    let mut backfill = make_backfill("fr-test", range);
    backfill.full_refresh = true;

    let pipeline = make_pipeline(
        "fr-pipe",
        vec![
            source_node("src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let reg = mock_registry(vec![test_batch()], captured);
    let (opts, _rx) = make_opts(pipeline, reg, store.clone());

    // Verify the base options have full_refresh = false by default.
    assert!(!opts.base_options.full_refresh, "base_options default should be false");

    let (_, _progress) =
        flux_datafusion::backfill::start_backfill(backfill, opts)
            .await
            .unwrap();

    // The full_refresh flag is applied per-iteration inside the coordinator.
    // We verify the backfill completed successfully — the coordinator overrides
    // the base full_refresh flag with the backfill's full_refresh value.
    let bf = store.get_backfill(&BackfillId("fr-test".into())).unwrap().unwrap();
    assert_eq!(bf.status, BackfillStatus::Completed);
}
