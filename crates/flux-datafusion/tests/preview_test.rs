// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for cache-based preview.

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use flux_datafusion::error::ExecutorError;
use flux_datafusion::output_cache::OutputCache;
use flux_datafusion::provider::{
    PipelineSink, ProviderError, ProviderRegistry, SourceConnector, WriteOptions, WriteStats,
};
use flux_datafusion::{PipelineExecutor, PreviewOptions, PreviewStatus};
use flux_engine::edge::Edge;
use flux_engine::node::*;
use flux_engine::pipeline::Pipeline;
use flux_engine::sample::SampleConfig;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
        ctx: &flux_datafusion::provider::MaterializationContext,
    ) -> Result<flux_datafusion::provider::MaterializationReceipt, ProviderError> {
        self.called.store(true, Ordering::Relaxed);
        let rows: u64 = data.iter().map(|b| b.num_rows() as u64).sum();
        let stats = WriteStats {
            rows_written: rows,
            bytes_written: 0,
            duration: Duration::ZERO,
        };
        Ok(flux_datafusion::provider::MaterializationReceipt::from_write_stats(&stats, ctx))
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
            lineage_annotations: None,
        }),
        position: Position::default(),
        pinned_position: false,
        snippet_parent: None,
        snippet_name: None,
    }
}

fn sink_node(id: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Sink(SinkConfig {
            connector: "mock".into(),
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

fn test_cache() -> (tempfile::TempDir, OutputCache) {
    let dir = tempfile::tempdir().unwrap();
    let cache = OutputCache::new(dir.path());
    (dir, cache)
}

fn mock_registry() -> ProviderRegistry {
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
    registry
}

// ---------------------------------------------------------------------------
// Preview tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn preview_loads_cached_data() {
    let (_dir, cache) = test_cache();
    let registry = mock_registry();

    let pipeline = make_pipeline(
        "cached_preview",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id, value FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    // Populate cache for src and xform.
    cache
        .write_node("cached_preview", "src", &[test_batch()], 10_000)
        .unwrap();
    cache
        .write_node("cached_preview", "xform", &[test_batch()], 10_000)
        .unwrap();

    let result =
        PipelineExecutor::preview(&pipeline, &cache, &registry, &PreviewOptions::default())
            .await
            .expect("preview should succeed");

    // All 3 nodes should have results.
    assert_eq!(result.nodes.len(), 3);

    // Source: cached with 3 rows.
    let src = result.node_output(&"src".into()).unwrap();
    assert_eq!(src.row_count, 3);
    assert_eq!(src.status, PreviewStatus::Cached);

    // Transform: cached with 3 rows.
    let xform = result.node_output(&"xform".into()).unwrap();
    assert_eq!(xform.row_count, 3);
    assert_eq!(xform.status, PreviewStatus::Cached);

    // Sink: always skipped.
    let out = result.node_output(&"out".into()).unwrap();
    assert_eq!(out.status, PreviewStatus::Skipped);
    assert_eq!(out.row_count, 0);
}

#[tokio::test]
async fn preview_reports_no_cache_for_uncached_nodes() {
    let (_dir, cache) = test_cache();
    let registry = mock_registry();

    let pipeline = make_pipeline(
        "no_cache",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    // Don't populate cache — all nodes should report NoCache.
    let result =
        PipelineExecutor::preview(&pipeline, &cache, &registry, &PreviewOptions::default())
            .await
            .expect("preview should succeed");

    let src = result.node_output(&"src".into()).unwrap();
    assert_eq!(src.status, PreviewStatus::NoCache);
    assert_eq!(src.row_count, 0);

    let xform = result.node_output(&"xform".into()).unwrap();
    assert_eq!(xform.status, PreviewStatus::NoCache);
}

#[tokio::test]
async fn preview_samples_cached_data() {
    let (_dir, cache) = test_cache();
    let registry = mock_registry();

    let pipeline = make_pipeline(
        "sample_cache",
        vec![source_node("src"), sink_node("out")],
        vec![Edge::new("src", "out")],
    );

    // Cache 200 rows.
    cache
        .write_node("sample_cache", "src", &[large_batch()], 10_000)
        .unwrap();

    let opts = PreviewOptions {
        sample: SampleConfig::FirstN { count: 10 },
        ..PreviewOptions::default()
    };

    let result = PipelineExecutor::preview(&pipeline, &cache, &registry, &opts)
        .await
        .expect("preview should succeed");

    let src = result.node_output(&"src".into()).unwrap();
    assert_eq!(src.row_count, 10);
    assert_eq!(src.status, PreviewStatus::Cached);
}

#[tokio::test]
async fn preview_re_execute_transform() {
    let (_dir, cache) = test_cache();
    let registry = mock_registry();

    let pipeline = make_pipeline(
        "re_exec",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id * 2 AS doubled FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    // Cache upstream (src) data.
    cache
        .write_node("re_exec", "src", &[test_batch()], 10_000)
        .unwrap();

    let opts = PreviewOptions {
        re_execute_node: Some(NodeId::new("xform")),
        ..PreviewOptions::default()
    };

    let result = PipelineExecutor::preview(&pipeline, &cache, &registry, &opts)
        .await
        .expect("preview should succeed");

    let xform = result.node_output(&"xform".into()).unwrap();
    assert_eq!(xform.status, PreviewStatus::ReExecuted);
    assert_eq!(xform.row_count, 3);
    // Verify schema has the "doubled" column from the SQL.
    assert_eq!(xform.schema.field(0).name(), "doubled");
}

#[tokio::test]
async fn preview_re_execute_without_upstream_cache_returns_no_cache() {
    let (_dir, cache) = test_cache();
    let registry = mock_registry();

    let pipeline = make_pipeline(
        "re_exec_no_upstream",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    // Don't cache src — re-execute should fail gracefully.
    let opts = PreviewOptions {
        re_execute_node: Some(NodeId::new("xform")),
        ..PreviewOptions::default()
    };

    let result = PipelineExecutor::preview(&pipeline, &cache, &registry, &opts)
        .await
        .expect("preview should succeed");

    let xform = result.node_output(&"xform".into()).unwrap();
    assert_eq!(xform.status, PreviewStatus::NoCache);
}

#[tokio::test]
async fn preview_cancellation() {
    let (_dir, cache) = test_cache();
    let registry = mock_registry();

    let pipeline = make_pipeline(
        "cancel",
        vec![
            source_node("src"),
            sql_transform_node("xform", "SELECT id FROM src"),
            sink_node("out"),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "out")],
    );

    let cancel = Arc::new(AtomicBool::new(true)); // pre-cancelled
    let opts = PreviewOptions {
        sample: SampleConfig::default(),
        cancel,
        progress: None,
        variable_overrides: std::collections::HashMap::new(),
        re_execute_node: None,
    };

    let err = PipelineExecutor::preview(&pipeline, &cache, &registry, &opts)
        .await
        .unwrap_err();

    assert!(matches!(err, ExecutorError::Cancelled));
}

#[tokio::test]
async fn preview_empty_pipeline_returns_validation_error() {
    let (_dir, cache) = test_cache();
    let registry = ProviderRegistry::new();

    let pipeline = make_pipeline("empty", vec![], vec![]);

    let err = PipelineExecutor::preview(&pipeline, &cache, &registry, &PreviewOptions::default())
        .await
        .unwrap_err();

    assert!(matches!(err, ExecutorError::Validation(_)));
}

#[tokio::test]
async fn preview_schema_from_cached_data() {
    let (_dir, cache) = test_cache();
    let registry = mock_registry();

    let pipeline = make_pipeline(
        "schema_test",
        vec![source_node("src"), sink_node("out")],
        vec![Edge::new("src", "out")],
    );

    cache
        .write_node("schema_test", "src", &[test_batch()], 10_000)
        .unwrap();

    let result =
        PipelineExecutor::preview(&pipeline, &cache, &registry, &PreviewOptions::default())
            .await
            .unwrap();

    let src_schema = &result.node_output(&"src".into()).unwrap().schema;
    assert_eq!(src_schema.fields().len(), 2);
    assert_eq!(src_schema.field(0).name(), "id");
    assert_eq!(src_schema.field(1).name(), "value");
}
