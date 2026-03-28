// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for preview execution.

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use flux_datafusion::error::ExecutorError;
use flux_datafusion::provider::{
    PipelineSink, ProviderError, ProviderRegistry, SourceConnector, WriteOptions, WriteStats,
};
use flux_datafusion::{PipelineExecutor, PreviewOptions};
use flux_engine::edge::Edge;
use flux_engine::node::*;
use flux_engine::pipeline::Pipeline;
use flux_engine::sample::SampleConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Test helpers (shared patterns from executor_test.rs)
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

fn large_batch() -> RecordBatch {
    let ids: Vec<i32> = (1..=200).collect();
    let vals: Vec<String> = (1..=200).map(|i| format!("val_{i}")).collect();
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(vals)),
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

/// A sink that records whether it was called (should NOT be called in preview).
struct SpySink {
    called: Arc<AtomicBool>,
}

#[async_trait]
impl PipelineSink for SpySink {
    async fn write(
        &self,
        _config: &SinkConfig,
        data: Vec<RecordBatch>,
        _options: &WriteOptions,
    ) -> Result<WriteStats, ProviderError> {
        self.called.store(true, Ordering::Relaxed);
        let rows: u64 = data.iter().map(|b| b.num_rows() as u64).sum();
        Ok(WriteStats {
            rows_written: rows,
            bytes_written: 0,
            duration: Duration::ZERO,
        })
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

// ---------------------------------------------------------------------------
// Preview tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn preview_returns_per_node_outputs() {
    let mut registry = ProviderRegistry::new();
    registry.register_source(
        "mock",
        Arc::new(MockSourceConnector {
            batches: vec![test_batch()],
        }),
    );
    let sink_called = Arc::new(AtomicBool::new(false));
    registry.register_sink(
        "mock",
        Arc::new(SpySink {
            called: Arc::clone(&sink_called),
        }),
    );

    let pipeline = make_pipeline(
        "preview_basic",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id, value FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let opts = PreviewOptions::default();
    let result = PipelineExecutor::preview(&pipeline, &registry, &opts)
        .await
        .expect("preview should succeed");

    // All 3 nodes should have results.
    assert_eq!(result.nodes.len(), 3);

    // Source: 3 rows.
    let src = result.node_output(&"src".into()).unwrap();
    assert_eq!(src.row_count, 3);
    assert_eq!(src.schema.fields().len(), 2);

    // Transform: 3 rows.
    let xform = result.node_output(&"xform".into()).unwrap();
    assert_eq!(xform.row_count, 3);

    // Sink: shows what would be written (3 rows), but sink was NOT called.
    let out = result.node_output(&"out".into()).unwrap();
    assert_eq!(out.row_count, 3);
    assert!(
        !sink_called.load(Ordering::Relaxed),
        "sink should not be invoked during preview"
    );
}

#[tokio::test]
async fn preview_samples_source_with_first_n() {
    let mut registry = ProviderRegistry::new();
    registry.register_source(
        "mock",
        Arc::new(MockSourceConnector {
            batches: vec![large_batch()],
        }),
    );
    registry.register_sink(
        "mock",
        Arc::new(SpySink {
            called: Arc::new(AtomicBool::new(false)),
        }),
    );

    let pipeline = make_pipeline(
        "preview_sample",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id, value FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let opts = PreviewOptions {
        sample: SampleConfig::FirstN { count: 10 },
        ..PreviewOptions::default()
    };

    let result = PipelineExecutor::preview(&pipeline, &registry, &opts)
        .await
        .expect("preview should succeed");

    // Source should be sampled to 10 rows.
    let src = result.node_output(&"src".into()).unwrap();
    assert_eq!(src.row_count, 10);

    // Transform operates on sampled data.
    let xform = result.node_output(&"xform".into()).unwrap();
    assert_eq!(xform.row_count, 10);
}

#[tokio::test]
async fn preview_full_sample_passes_all_rows() {
    let mut registry = ProviderRegistry::new();
    registry.register_source(
        "mock",
        Arc::new(MockSourceConnector {
            batches: vec![large_batch()],
        }),
    );
    registry.register_sink(
        "mock",
        Arc::new(SpySink {
            called: Arc::new(AtomicBool::new(false)),
        }),
    );

    let pipeline = make_pipeline(
        "preview_full",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id, value FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let opts = PreviewOptions {
        sample: SampleConfig::Full,
        ..PreviewOptions::default()
    };

    let result = PipelineExecutor::preview(&pipeline, &registry, &opts)
        .await
        .expect("preview should succeed");

    let src = result.node_output(&"src".into()).unwrap();
    assert_eq!(src.row_count, 200);
}

#[tokio::test]
async fn preview_random_sample_is_deterministic() {
    let mut registry = ProviderRegistry::new();
    registry.register_source(
        "mock",
        Arc::new(MockSourceConnector {
            batches: vec![large_batch()],
        }),
    );
    registry.register_sink(
        "mock",
        Arc::new(SpySink {
            called: Arc::new(AtomicBool::new(false)),
        }),
    );

    let pipeline = make_pipeline(
        "preview_random",
        vec![source_node("src"), sink_node("out")],
        vec![Edge::new("src", "out")],
    );

    let opts = PreviewOptions {
        sample: SampleConfig::Random {
            count: 15,
            seed: 42,
        },
        ..PreviewOptions::default()
    };

    let r1 = PipelineExecutor::preview(&pipeline, &registry, &opts)
        .await
        .unwrap();
    let r2 = PipelineExecutor::preview(&pipeline, &registry, &opts)
        .await
        .unwrap();

    let src1 = r1.node_output(&"src".into()).unwrap();
    let src2 = r2.node_output(&"src".into()).unwrap();
    assert_eq!(src1.row_count, 15);
    assert_eq!(src2.row_count, 15);
}

#[tokio::test]
async fn preview_cancellation() {
    let mut registry = ProviderRegistry::new();
    registry.register_source(
        "mock",
        Arc::new(MockSourceConnector {
            batches: vec![test_batch()],
        }),
    );
    registry.register_sink(
        "mock",
        Arc::new(SpySink {
            called: Arc::new(AtomicBool::new(false)),
        }),
    );

    let pipeline = make_pipeline(
        "preview_cancel",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id, value FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let cancel = Arc::new(AtomicBool::new(true)); // pre-cancelled
    let opts = PreviewOptions {
        sample: SampleConfig::default(),
        cancel,
        progress: None,
    };

    let err = PipelineExecutor::preview(&pipeline, &registry, &opts)
        .await
        .unwrap_err();

    assert!(matches!(err, ExecutorError::Cancelled));
}

#[tokio::test]
async fn preview_invalid_sql_reports_error() {
    let mut registry = ProviderRegistry::new();
    registry.register_source(
        "mock",
        Arc::new(MockSourceConnector {
            batches: vec![test_batch()],
        }),
    );
    registry.register_sink(
        "mock",
        Arc::new(SpySink {
            called: Arc::new(AtomicBool::new(false)),
        }),
    );

    let pipeline = make_pipeline(
        "preview_bad_sql",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECTTTTT broken"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let err = PipelineExecutor::preview(&pipeline, &registry, &PreviewOptions::default())
        .await
        .unwrap_err();

    assert!(matches!(err, ExecutorError::Node { .. }));
}

#[tokio::test]
async fn preview_empty_pipeline_returns_validation_error() {
    let pipeline = make_pipeline("empty", vec![], vec![]);
    let registry = ProviderRegistry::new();

    let err = PipelineExecutor::preview(&pipeline, &registry, &PreviewOptions::default())
        .await
        .unwrap_err();

    assert!(matches!(err, ExecutorError::Validation(_)));
}

#[tokio::test]
async fn preview_schema_includes_column_info() {
    let mut registry = ProviderRegistry::new();
    registry.register_source(
        "mock",
        Arc::new(MockSourceConnector {
            batches: vec![test_batch()],
        }),
    );
    registry.register_sink(
        "mock",
        Arc::new(SpySink {
            called: Arc::new(AtomicBool::new(false)),
        }),
    );

    let pipeline = make_pipeline(
        "preview_schema",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id AS renamed_id FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let result = PipelineExecutor::preview(&pipeline, &registry, &PreviewOptions::default())
        .await
        .unwrap();

    // Source schema: id (Int32), value (Utf8)
    let src_schema = &result.node_output(&"src".into()).unwrap().schema;
    assert_eq!(src_schema.fields().len(), 2);
    assert_eq!(src_schema.field(0).name(), "id");

    // Transform schema: renamed_id (Int32)
    let xform_schema = &result.node_output(&"xform".into()).unwrap().schema;
    assert_eq!(xform_schema.fields().len(), 1);
    assert_eq!(xform_schema.field(0).name(), "renamed_id");
}
