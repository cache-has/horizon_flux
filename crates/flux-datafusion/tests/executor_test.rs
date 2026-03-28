// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for `PipelineExecutor`.

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use flux_datafusion::error::{ExecutorError, NodeErrorKind};
use flux_datafusion::provider::{
    PipelineSink, ProviderError, ProviderRegistry, SourceConnector, WriteOptions, WriteStats,
};
use flux_datafusion::{ExecutionOptions, PipelineExecutor, RunStatus, RunStore};
use flux_engine::edge::Edge;
use flux_engine::node::*;
use flux_engine::pipeline::Pipeline;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

fn second_batch() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["d", "e"])),
        ],
    )
    .unwrap()
}

fn default_opts() -> ExecutionOptions {
    ExecutionOptions::default()
}

// ---------------------------------------------------------------------------
// Mock connectors
// ---------------------------------------------------------------------------

/// A source connector that wraps fixed batches in a MemTable TableProvider.
struct MockSourceConnector {
    batches: Vec<RecordBatch>,
}

impl SourceConnector for MockSourceConnector {
    fn create_table_provider(
        &self,
        _config: &SourceConfig,
    ) -> Result<Arc<dyn TableProvider>, ProviderError> {
        if self.batches.is_empty() {
            let schema = test_schema();
            let table = MemTable::try_new(schema, vec![vec![]])?;
            return Ok(Arc::new(table));
        }
        let schema = self.batches[0].schema();
        let table = MemTable::try_new(schema, vec![self.batches.clone()])?;
        Ok(Arc::new(table))
    }
}

/// A source connector that sleeps briefly (for cancellation tests).
struct SlowSourceConnector {
    batches: Vec<RecordBatch>,
}

impl SourceConnector for SlowSourceConnector {
    fn create_table_provider(
        &self,
        _config: &SourceConfig,
    ) -> Result<Arc<dyn TableProvider>, ProviderError> {
        // The sleep was in the old SourceProvider::read. With TableProvider,
        // the "slowness" happens at query time. For cancellation tests we just
        // need the source to produce data — the cancel flag is checked between
        // nodes, not during a single node's execution.
        let schema = self.batches[0].schema();
        let table = MemTable::try_new(schema, vec![self.batches.clone()])?;
        Ok(Arc::new(table))
    }
}

/// A sink that captures written batches for assertions.
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
    ) -> Result<WriteStats, ProviderError> {
        let row_count: u64 = data.iter().map(|b| b.num_rows() as u64).sum();
        self.captured.lock().unwrap().extend(data);
        Ok(WriteStats {
            rows_written: row_count,
            bytes_written: 0,
            duration: Duration::ZERO,
        })
    }

    fn validate_config(&self, _config: &SinkConfig) -> Result<(), ProviderError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Pipeline builders
// ---------------------------------------------------------------------------

fn source_node(id: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Source(SourceConfig {
            connector: "mock".into(),
            config: serde_json::Value::Null,
        }),
        position: Position::default(),
        pinned_position: false,
    }
}

fn sql_transform_node(id: &str, sql: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Transform(TransformConfig {
            mode: TransformMode::Sql,
            code: sql.to_string(),
            materialized: false,
        }),
        position: Position::default(),
        pinned_position: false,
    }
}

fn python_transform_node(id: &str, code: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Transform(TransformConfig {
            mode: TransformMode::Python,
            code: code.to_string(),
            materialized: false,
        }),
        position: Position::default(),
        pinned_position: false,
    }
}

fn sink_node(id: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Sink(SinkConfig {
            connector: "mock".into(),
            config: serde_json::Value::Null,
        }),
        position: Position::default(),
        pinned_position: false,
    }
}

fn make_pipeline(name: &str, nodes: Vec<Node>, edges: Vec<Edge>) -> Pipeline {
    Pipeline {
        name: name.to_string(),
        version: 1,
        default_environment: "dev".to_string(),
        variables: HashMap::new(),
        environment_overrides: HashMap::new(),
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

// ---------------------------------------------------------------------------
// Existing executor tests (updated for new API)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn linear_pipeline_source_transform_sink() {
    let pipeline = make_pipeline(
        "linear",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id, value FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let registry = mock_registry(vec![test_batch()], Arc::clone(&captured));

    let (result, run) = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .expect("pipeline should succeed");

    // 3 nodes executed
    assert_eq!(result.node_stats.len(), 3);

    // Source produced 3 rows
    assert_eq!(result.node_stats[0].rows_out, 3);
    assert_eq!(result.node_stats[0].rows_in, 0);

    // Transform consumed 3, produced 3
    assert_eq!(result.node_stats[1].rows_in, 3);
    assert_eq!(result.node_stats[1].rows_out, 3);

    // Sink consumed 3, produced 3
    assert_eq!(result.node_stats[2].rows_in, 3);
    assert_eq!(result.node_stats[2].rows_out, 3);

    // Sink captured the data
    let sink_data = captured.lock().unwrap();
    assert_eq!(sink_data.len(), 1);
    assert_eq!(sink_data[0].num_rows(), 3);

    // PipelineRun should be Success
    assert_eq!(run.status, RunStatus::Success);
    assert!(run.start_time.is_some());
    assert!(run.end_time.is_some());
    assert_eq!(run.node_stats.len(), 3);
}

#[tokio::test]
async fn sql_transform_filters_rows() {
    let pipeline = make_pipeline(
        "filter",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id, value FROM src WHERE id > 1"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let registry = mock_registry(vec![test_batch()], Arc::clone(&captured));

    let (result, _run) = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .expect("pipeline should succeed");

    // Transform should have filtered to 2 rows (id=2, id=3)
    assert_eq!(result.node_stats[1].rows_out, 2);

    let sink_data = captured.lock().unwrap();
    assert_eq!(sink_data[0].num_rows(), 2);
}

#[tokio::test]
async fn multi_input_join_transform() {
    let mut registry = ProviderRegistry::new();
    registry.register_source(
        "mock",
        Arc::new(MockSourceConnector {
            batches: vec![test_batch()],
        }),
    );

    let pipeline = make_pipeline(
        "join",
        vec![
            source_node("src_a"),
            {
                let mut n = source_node("src_b");
                if let NodeKind::Source(ref mut cfg) = n.kind {
                    cfg.connector = "mock_b".to_string();
                }
                n
            },
            sql_transform_node(
                "joined",
                "SELECT a.id, a.value, b.value AS value_b \
                 FROM src_a a JOIN src_b b ON a.id = b.id",
            ),
            sink_node("out"),
        ],
        vec![
            Edge::new("src_a", "joined"),
            Edge::new("src_b", "joined"),
            Edge::new("joined", "out"),
        ],
    );

    // src_b has ids [4,5] - no overlap with src_a [1,2,3], so inner join yields 0 rows
    registry.register_source(
        "mock_b",
        Arc::new(MockSourceConnector {
            batches: vec![second_batch()],
        }),
    );
    let captured = Arc::new(Mutex::new(Vec::new()));
    registry.register_sink(
        "mock",
        Arc::new(MockSink {
            captured: Arc::clone(&captured),
        }),
    );

    let (result, _run) = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .expect("pipeline should succeed");

    // Inner join: src_a [1,2,3] x src_b [4,5] -> 0 rows
    assert_eq!(result.node_stats[2].rows_out, 0);
}

#[tokio::test]
async fn python_transform_passthrough() {
    let code = r#"
import polars as pl

def transform(inputs, params):
    return inputs["src"]
"#;
    let pipeline = make_pipeline(
        "python_passthrough",
        vec![
            source_node("src"),
            python_transform_node("py", code),
            sink_node("out"),
        ],
        vec![Edge::new("src", "py"), Edge::new("py", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let registry = mock_registry(vec![test_batch()], captured.clone());

    let (result, run) = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .expect("pipeline should succeed");

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(result.node_stats[1].rows_out, 3); // passthrough: 3 rows
    let written = captured.lock().unwrap();
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].num_rows(), 3);
}

#[tokio::test]
async fn python_transform_filter() {
    let code = r#"
import polars as pl

def transform(inputs, params):
    df = inputs["src"]
    return df.filter(pl.col("id") > 1)
"#;
    let pipeline = make_pipeline(
        "python_filter",
        vec![
            source_node("src"),
            python_transform_node("py", code),
            sink_node("out"),
        ],
        vec![Edge::new("src", "py"), Edge::new("py", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let registry = mock_registry(vec![test_batch()], captured.clone());

    let (result, _run) = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .expect("pipeline should succeed");

    // test_batch has ids [1,2,3], filter > 1 keeps [2,3]
    assert_eq!(result.node_stats[1].rows_out, 2);
}

#[tokio::test]
async fn python_transform_syntax_error() {
    let code = "def transform(inputs, params)\n    return inputs['src']"; // missing colon
    let pipeline = make_pipeline(
        "python_syntax_err",
        vec![
            source_node("src"),
            python_transform_node("py", code),
            sink_node("out"),
        ],
        vec![Edge::new("src", "py"), Edge::new("py", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let registry = mock_registry(vec![test_batch()], captured);

    let err = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .unwrap_err();

    match err {
        ExecutorError::Node {
            ref node_id,
            ref kind,
        } => {
            assert_eq!(node_id.0, "py");
            let msg = kind.to_string();
            assert!(
                msg.contains("SyntaxError"),
                "expected SyntaxError, got: {msg}"
            );
        }
        other => panic!("expected Node error, got: {other}"),
    }
}

#[tokio::test]
async fn python_transform_missing_function() {
    let code = "x = 42"; // no transform function defined
    let pipeline = make_pipeline(
        "python_no_fn",
        vec![
            source_node("src"),
            python_transform_node("py", code),
            sink_node("out"),
        ],
        vec![Edge::new("src", "py"), Edge::new("py", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let registry = mock_registry(vec![test_batch()], captured);

    let err = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .unwrap_err();

    match err {
        ExecutorError::Node {
            ref node_id,
            ref kind,
        } => {
            assert_eq!(node_id.0, "py");
            let msg = kind.to_string();
            assert!(
                msg.contains("transform"),
                "expected missing-function message, got: {msg}"
            );
        }
        other => panic!("expected Node error, got: {other}"),
    }
}

#[tokio::test]
async fn python_transform_wrong_return_type() {
    let code = r#"
def transform(inputs, params):
    return "not a dataframe"
"#;
    let pipeline = make_pipeline(
        "python_bad_return",
        vec![
            source_node("src"),
            python_transform_node("py", code),
            sink_node("out"),
        ],
        vec![Edge::new("src", "py"), Edge::new("py", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let registry = mock_registry(vec![test_batch()], captured);

    let err = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .unwrap_err();

    match err {
        ExecutorError::Node {
            ref node_id,
            ref kind,
        } => {
            assert_eq!(node_id.0, "py");
            let msg = kind.to_string();
            assert!(
                msg.contains("DataFrame"),
                "expected wrong-type message, got: {msg}"
            );
        }
        other => panic!("expected Node error, got: {other}"),
    }
}

#[tokio::test]
async fn missing_source_provider() {
    let pipeline = make_pipeline(
        "missing",
        vec![
            {
                let mut n = source_node("src");
                if let NodeKind::Source(ref mut cfg) = n.kind {
                    cfg.connector = "nonexistent".to_string();
                }
                n
            },
            sql_transform_node("xform", "SELECT * FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let registry = ProviderRegistry::new();

    let err = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .unwrap_err();

    match err {
        ExecutorError::Node { ref kind, .. } => {
            assert!(matches!(kind, NodeErrorKind::SourceProviderNotFound(c) if c == "nonexistent"));
        }
        other => panic!("expected SourceProviderNotFound, got: {other}"),
    }
}

#[tokio::test]
async fn invalid_sql_returns_datafusion_error() {
    let pipeline = make_pipeline(
        "bad_sql",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECTTTT broken garbage"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let registry = mock_registry(vec![test_batch()], captured);

    let err = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .unwrap_err();

    match err {
        ExecutorError::Node { ref kind, .. } => {
            assert!(
                matches!(
                    kind,
                    NodeErrorKind::DataFusion(_) | NodeErrorKind::Preprocess(_)
                ),
                "expected DataFusion or Preprocess error, got: {kind}"
            );
        }
        other => panic!("expected node error, got: {other}"),
    }
}

#[tokio::test]
async fn empty_pipeline_returns_validation_error() {
    let pipeline = make_pipeline("empty", vec![], vec![]);
    let registry = ProviderRegistry::new();

    let err = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .unwrap_err();

    assert!(matches!(err, ExecutorError::Validation(_)));
}

#[tokio::test]
async fn execution_stats_track_duration() {
    let pipeline = make_pipeline(
        "timing",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id, value FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let registry = mock_registry(vec![test_batch()], captured);

    let (result, _run) = PipelineExecutor::execute(&pipeline, &registry, &default_opts())
        .await
        .expect("pipeline should succeed");

    // All nodes should have non-negative durations and no errors
    for stat in &result.node_stats {
        assert!(stat.end_time >= stat.start_time);
        assert!(stat.error.is_none());
    }

    // Overall pipeline duration should be non-negative
    assert!(result.end_time >= result.start_time);
}

// ---------------------------------------------------------------------------
// Run store tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_with_run_store_persists_history() {
    let store = Arc::new(RunStore::open_in_memory().unwrap());

    let pipeline = make_pipeline(
        "persisted",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id, value FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let registry = mock_registry(vec![test_batch()], Arc::clone(&captured));

    let opts = ExecutionOptions {
        environment: "test".to_string(),
        run_store: Some(Arc::clone(&store)),
        cancel: Arc::new(AtomicBool::new(false)),
        environment_resolver: None,
    };

    let (_result, run) = PipelineExecutor::execute(&pipeline, &registry, &opts)
        .await
        .expect("pipeline should succeed");

    // Verify persisted run
    let loaded = store.get_run(&run.id).unwrap().expect("run should exist");
    assert_eq!(loaded.status, RunStatus::Success);
    assert_eq!(loaded.pipeline_name, "persisted");
    assert_eq!(loaded.environment, "test");
    assert!(loaded.start_time.is_some());
    assert!(loaded.end_time.is_some());
    assert_eq!(loaded.node_stats.len(), 3);
    assert!(loaded.error.is_none());

    // Verify list_runs
    let runs = store.list_runs(Some("persisted"), 10).unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].id, run.id);
}

#[tokio::test]
async fn failed_run_persists_error() {
    let store = Arc::new(RunStore::open_in_memory().unwrap());

    let pipeline = make_pipeline(
        "failing",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECTTTT broken"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let registry = mock_registry(vec![test_batch()], captured);

    let opts = ExecutionOptions {
        environment: "test".to_string(),
        run_store: Some(Arc::clone(&store)),
        cancel: Arc::new(AtomicBool::new(false)),
        environment_resolver: None,
    };

    let err = PipelineExecutor::execute(&pipeline, &registry, &opts)
        .await
        .unwrap_err();
    assert!(matches!(err, ExecutorError::Node { .. }));

    // Run should be persisted as Failed
    let runs = store.list_runs(Some("failing"), 10).unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, RunStatus::Failed);
    assert!(runs[0].error.is_some());
    // Source node succeeded, transform failed — both should have stats
    assert_eq!(runs[0].node_stats.len(), 2);
}

#[tokio::test]
async fn cancellation_stops_execution() {
    let cancel = Arc::new(AtomicBool::new(false));

    // Build a pipeline with two source nodes: first one is slow,
    // and we cancel before the second node runs.
    let mut registry = ProviderRegistry::new();
    registry.register_source(
        "slow",
        Arc::new(SlowSourceConnector {
            batches: vec![test_batch()],
        }),
    );
    registry.register_source(
        "mock",
        Arc::new(MockSourceConnector {
            batches: vec![test_batch()],
        }),
    );
    let captured = Arc::new(Mutex::new(Vec::new()));
    registry.register_sink(
        "mock",
        Arc::new(MockSink {
            captured: Arc::clone(&captured),
        }),
    );

    let pipeline = make_pipeline(
        "cancel_test",
        vec![
            {
                let mut n = source_node("slow_src");
                if let NodeKind::Source(ref mut cfg) = n.kind {
                    cfg.connector = "slow".to_string();
                }
                n
            },
            sql_transform_node("xform", "SELECT id, value FROM slow_src"),
            sink_node("out"),
        ],
        vec![Edge::new("slow_src", "xform"), Edge::new("xform", "out")],
    );

    let cancel_clone = Arc::clone(&cancel);
    let opts = ExecutionOptions {
        environment: "test".to_string(),
        run_store: None,
        cancel: cancel_clone,
        environment_resolver: None,
    };

    // Set cancel before execution so it triggers after the first node.
    // The first node (slow_src) will execute, then cancellation is checked
    // before the second node.
    cancel.store(true, Ordering::Relaxed);

    let err = PipelineExecutor::execute(&pipeline, &registry, &opts)
        .await
        .unwrap_err();

    assert!(matches!(err, ExecutorError::Cancelled));

    // Sink should NOT have received any data
    let sink_data = captured.lock().unwrap();
    assert!(sink_data.is_empty());
}

// ---------------------------------------------------------------------------
// RunStore unit tests
// ---------------------------------------------------------------------------

#[test]
fn run_store_create_and_get() {
    let store = RunStore::open_in_memory().unwrap();
    let run = store.create_run("my_pipeline", "dev").unwrap();

    assert_eq!(run.pipeline_name, "my_pipeline");
    assert_eq!(run.environment, "dev");
    assert_eq!(run.status, RunStatus::Pending);

    let loaded = store.get_run(&run.id).unwrap().unwrap();
    assert_eq!(loaded.pipeline_name, "my_pipeline");
    assert_eq!(loaded.status, RunStatus::Pending);
}

#[test]
fn run_store_lifecycle() {
    use std::time::SystemTime;

    let store = RunStore::open_in_memory().unwrap();
    let run = store.create_run("lifecycle", "prod").unwrap();

    let start = SystemTime::now();
    store.set_running(&run.id, start).unwrap();

    let loaded = store.get_run(&run.id).unwrap().unwrap();
    assert_eq!(loaded.status, RunStatus::Running);
    assert!(loaded.start_time.is_some());

    let end = SystemTime::now();
    store
        .finish_run(&run.id, RunStatus::Success, end, None)
        .unwrap();

    let loaded = store.get_run(&run.id).unwrap().unwrap();
    assert_eq!(loaded.status, RunStatus::Success);
    assert!(loaded.end_time.is_some());
}

#[test]
fn run_store_list_filters_by_pipeline() {
    let store = RunStore::open_in_memory().unwrap();
    store.create_run("alpha", "dev").unwrap();
    store.create_run("beta", "dev").unwrap();
    store.create_run("alpha", "prod").unwrap();

    let alpha_runs = store.list_runs(Some("alpha"), 10).unwrap();
    assert_eq!(alpha_runs.len(), 2);

    let beta_runs = store.list_runs(Some("beta"), 10).unwrap();
    assert_eq!(beta_runs.len(), 1);

    let all_runs = store.list_runs(None, 10).unwrap();
    assert_eq!(all_runs.len(), 3);
}
