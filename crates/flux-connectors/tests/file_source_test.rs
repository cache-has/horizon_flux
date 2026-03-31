// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the file source connector (CSV and Parquet).

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use flux_connectors::FileSource;
use flux_datafusion::provider::SourceConnector;
use flux_engine::node::SourceConfig;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper: create a SourceConfig with the given connector name and JSON config.
fn source_config(connector: &str, config: serde_json::Value) -> SourceConfig {
    SourceConfig {
        connector: connector.to_string(),
        config,
        cache_row_limit: None,
    }
}

/// Helper: write a CSV file with test data and return its path.
fn write_test_csv(dir: &TempDir, filename: &str, content: &str) -> String {
    let path = dir.path().join(filename);
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(content.as_bytes()).unwrap();
    path.to_str().unwrap().to_string()
}

/// Helper: write a Parquet file with test data and return its path.
fn write_test_parquet(dir: &TempDir, filename: &str) -> String {
    let path = dir.path().join(filename);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
            Arc::new(Float64Array::from(vec![95.5, 87.3, 92.1])),
        ],
    )
    .unwrap();

    let file = std::fs::File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    path.to_str().unwrap().to_string()
}

// ---------------------------------------------------------------------------
// CSV tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_source_reads_basic_file() {
    let dir = TempDir::new().unwrap();
    let path = write_test_csv(
        &dir,
        "data.csv",
        "id,name,score\n1,Alice,95.5\n2,Bob,87.3\n3,Carol,92.1\n",
    );

    let source = FileSource::new();
    let config = source_config(
        "csv",
        serde_json::json!({
            "path": path,
            "format": "csv",
        }),
    );

    let provider = source.create_table_provider(&config).unwrap();

    // Use a session to query the table provider
    let ctx = SessionContext::new();
    ctx.register_table("test", provider).unwrap();
    let batches = ctx
        .sql("SELECT * FROM test")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    // Verify schema has 3 columns
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(2).name(), "score");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_source_with_custom_delimiter() {
    let dir = TempDir::new().unwrap();
    let path = write_test_csv(&dir, "data.tsv", "id\tname\n1\tAlice\n2\tBob\n");

    let source = FileSource::new();
    let config = source_config(
        "csv",
        serde_json::json!({
            "path": path,
            "format": "csv",
            "options": { "delimiter": "\t" }
        }),
    );

    let provider = source.create_table_provider(&config).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("test", provider).unwrap();
    let batches = ctx
        .sql("SELECT * FROM test")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_source_projection_pushdown() {
    let dir = TempDir::new().unwrap();
    let path = write_test_csv(
        &dir,
        "data.csv",
        "id,name,score\n1,Alice,95.5\n2,Bob,87.3\n",
    );

    let source = FileSource::new();
    let config = source_config(
        "csv",
        serde_json::json!({
            "path": path,
            "format": "csv",
        }),
    );

    let provider = source.create_table_provider(&config).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("test", provider).unwrap();

    // Only select one column — projection pushdown should limit reading
    let batches = ctx
        .sql("SELECT name FROM test")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].schema().fields().len(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "name");
}

// ---------------------------------------------------------------------------
// Parquet tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_source_reads_basic_file() {
    let dir = TempDir::new().unwrap();
    let path = write_test_parquet(&dir, "data.parquet");

    let source = FileSource::new();
    let config = source_config(
        "parquet",
        serde_json::json!({
            "path": path,
            "format": "parquet",
        }),
    );

    let provider = source.create_table_provider(&config).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("test", provider).unwrap();
    let batches = ctx
        .sql("SELECT * FROM test")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(2).name(), "score");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_source_filter_pushdown() {
    let dir = TempDir::new().unwrap();
    let path = write_test_parquet(&dir, "data.parquet");

    let source = FileSource::new();
    let config = source_config(
        "parquet",
        serde_json::json!({
            "path": path,
            "format": "parquet",
        }),
    );

    let provider = source.create_table_provider(&config).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("test", provider).unwrap();

    // Filter pushdown — only rows where score > 90
    let batches = ctx
        .sql("SELECT name, score FROM test WHERE score > 90")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2); // Alice (95.5) and Carol (92.1)
}

// ---------------------------------------------------------------------------
// Glob pattern tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_source_glob_pattern_reads_multiple_files() {
    let dir = TempDir::new().unwrap();
    write_test_csv(&dir, "part1.csv", "id,name\n1,Alice\n2,Bob\n");
    write_test_csv(&dir, "part2.csv", "id,name\n3,Carol\n4,Dave\n");

    let glob_path = dir.path().join("*.csv");

    let source = FileSource::new();
    let config = source_config(
        "csv",
        serde_json::json!({
            "path": glob_path.to_str().unwrap(),
            "format": "csv",
        }),
    );

    let provider = source.create_table_provider(&config).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("test", provider).unwrap();
    let batches = ctx
        .sql("SELECT * FROM test")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

// ---------------------------------------------------------------------------
// Error handling tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_not_found_returns_error() {
    let source = FileSource::new();
    let config = source_config(
        "csv",
        serde_json::json!({
            "path": "/nonexistent/path/data.csv",
            "format": "csv",
        }),
    );

    // Schema inference should fail for a nonexistent path.
    let result = source.create_table_provider(&config);
    assert!(result.is_err(), "nonexistent file should produce an error");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("failed") || msg.contains("schema") || msg.contains("not found"),
        "error should be descriptive: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_config_returns_error() {
    let source = FileSource::new();
    let config = source_config(
        "csv",
        serde_json::json!({
            "not_a_valid_field": true,
        }),
    );

    let result = source.create_table_provider(&config);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// default_registry tests
// ---------------------------------------------------------------------------

#[test]
fn default_registry_has_file_connectors() {
    let registry = flux_connectors::default_registry();
    let names = registry.source_names();
    assert!(
        names.contains(&"csv"),
        "registry should contain 'csv' source"
    );
    assert!(
        names.contains(&"parquet"),
        "registry should contain 'parquet' source"
    );
    assert!(
        names.contains(&"file"),
        "registry should contain 'file' source"
    );
}
