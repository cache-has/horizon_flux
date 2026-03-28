// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the file sink connector (CSV and Parquet).

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use flux_connectors::FileSink;
use flux_datafusion::provider::{PipelineSink, WriteOptions};
use flux_engine::node::SinkConfig;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper: create a SinkConfig with the given connector name and JSON config.
fn sink_config(connector: &str, config: serde_json::Value) -> SinkConfig {
    SinkConfig {
        connector: connector.to_string(),
        config,
    }
}

/// Helper: create test record batches.
fn test_batches() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));

    vec![
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
                Arc::new(Float64Array::from(vec![95.5, 87.3, 92.1])),
            ],
        )
        .unwrap(),
    ]
}

// ---------------------------------------------------------------------------
// CSV sink tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_sink_writes_basic_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("output.csv");

    let sink = FileSink::new();
    let config = sink_config(
        "csv",
        serde_json::json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
        }),
    );

    let stats = sink
        .write(&config, test_batches(), &WriteOptions::default())
        .await
        .unwrap();

    assert_eq!(stats.rows_written, 3);
    assert!(stats.bytes_written > 0);

    let content = std::fs::read_to_string(&path).unwrap();
    assert!(content.starts_with("id,name,score\n"));
    assert!(content.contains("Alice"));
    assert!(content.contains("Bob"));
    assert!(content.contains("Carol"));
    // 3 data rows + 1 header = 4 lines
    assert_eq!(content.lines().count(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_sink_with_custom_delimiter() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("output.tsv");

    let sink = FileSink::new();
    let config = sink_config(
        "csv",
        serde_json::json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
            "options": { "delimiter": "\t" }
        }),
    );

    sink.write(&config, test_batches(), &WriteOptions::default())
        .await
        .unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    assert!(content.contains("id\tname\tscore"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_sink_no_header() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("output.csv");

    let sink = FileSink::new();
    let config = sink_config(
        "csv",
        serde_json::json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
            "options": { "has_header": false }
        }),
    );

    sink.write(&config, test_batches(), &WriteOptions::default())
        .await
        .unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    // No header line — should start with data directly.
    assert!(!content.starts_with("id"));
    assert_eq!(content.lines().count(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_sink_append_mode() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("output.csv");

    let sink = FileSink::new();

    // First write — overwrite (default).
    let config = sink_config(
        "csv",
        serde_json::json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
        }),
    );
    sink.write(&config, test_batches(), &WriteOptions::default())
        .await
        .unwrap();

    // Second write — append.
    let append_config = sink_config(
        "csv",
        serde_json::json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
            "options": { "write_mode": "append" }
        }),
    );
    sink.write(&append_config, test_batches(), &WriteOptions::default())
        .await
        .unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    // 1 header + 3 original rows + 3 appended rows = 7 lines
    assert_eq!(content.lines().count(), 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_sink_overwrite_replaces_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("output.csv");

    let sink = FileSink::new();
    let config = sink_config(
        "csv",
        serde_json::json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
        }),
    );

    // Write twice with default (overwrite) mode.
    sink.write(&config, test_batches(), &WriteOptions::default())
        .await
        .unwrap();
    sink.write(&config, test_batches(), &WriteOptions::default())
        .await
        .unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    // Should still be just 4 lines (overwritten, not appended).
    assert_eq!(content.lines().count(), 4);
}

// ---------------------------------------------------------------------------
// Parquet sink tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_sink_writes_basic_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("output.parquet");

    let sink = FileSink::new();
    let config = sink_config(
        "parquet",
        serde_json::json!({
            "path": path.to_str().unwrap(),
            "format": "parquet",
        }),
    );

    let stats = sink
        .write(&config, test_batches(), &WriteOptions::default())
        .await
        .unwrap();

    assert_eq!(stats.rows_written, 3);
    assert!(stats.bytes_written > 0);
    assert!(path.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_sink_with_compression() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("output.parquet");

    let sink = FileSink::new();
    let config = sink_config(
        "parquet",
        serde_json::json!({
            "path": path.to_str().unwrap(),
            "format": "parquet",
            "options": { "compression": "zstd" }
        }),
    );

    let stats = sink
        .write(&config, test_batches(), &WriteOptions::default())
        .await
        .unwrap();

    assert_eq!(stats.rows_written, 3);
    assert!(path.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_sink_append_fails() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("output.parquet");

    // Create the file first.
    std::fs::write(&path, b"dummy").unwrap();

    let sink = FileSink::new();
    let config = sink_config(
        "parquet",
        serde_json::json!({
            "path": path.to_str().unwrap(),
            "format": "parquet",
            "options": { "write_mode": "append" }
        }),
    );

    let result = sink
        .write(&config, test_batches(), &WriteOptions::default())
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("append"));
}

// ---------------------------------------------------------------------------
// Parent directory creation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_creates_parent_directories() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("nested").join("deep").join("output.csv");

    let sink = FileSink::new();
    let config = sink_config(
        "csv",
        serde_json::json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
        }),
    );

    sink.write(&config, test_batches(), &WriteOptions::default())
        .await
        .unwrap();

    assert!(path.exists());
}

// ---------------------------------------------------------------------------
// Empty data
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_handles_empty_data() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("output.csv");

    let sink = FileSink::new();
    let config = sink_config(
        "csv",
        serde_json::json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
        }),
    );

    let stats = sink
        .write(&config, vec![], &WriteOptions::default())
        .await
        .unwrap();

    assert_eq!(stats.rows_written, 0);
    // File should not be created for empty data.
    assert!(!path.exists());
}

// ---------------------------------------------------------------------------
// Validation tests
// ---------------------------------------------------------------------------

#[test]
fn validate_rejects_empty_path() {
    let sink = FileSink::new();
    let config = sink_config(
        "csv",
        serde_json::json!({
            "path": "",
            "format": "csv",
        }),
    );
    assert!(sink.validate_config(&config).is_err());
}

#[test]
fn validate_rejects_glob_patterns() {
    let sink = FileSink::new();
    let config = sink_config(
        "csv",
        serde_json::json!({
            "path": "/data/*.csv",
            "format": "csv",
        }),
    );
    assert!(sink.validate_config(&config).is_err());
}

#[test]
fn validate_rejects_invalid_compression() {
    let sink = FileSink::new();
    let config = sink_config(
        "parquet",
        serde_json::json!({
            "path": "/data/output.parquet",
            "format": "parquet",
            "options": { "compression": "brotli" }
        }),
    );
    assert!(sink.validate_config(&config).is_err());
}

#[test]
fn validate_accepts_valid_config() {
    let sink = FileSink::new();
    let config = sink_config(
        "csv",
        serde_json::json!({
            "path": "/data/output.csv",
            "format": "csv",
        }),
    );
    assert!(sink.validate_config(&config).is_ok());
}

// ---------------------------------------------------------------------------
// Registry tests
// ---------------------------------------------------------------------------

#[test]
fn default_registry_has_file_sink() {
    let registry = flux_connectors::default_registry();
    let names = registry.sink_names();
    assert!(names.contains(&"csv"), "registry should contain 'csv' sink");
    assert!(
        names.contains(&"parquet"),
        "registry should contain 'parquet' sink"
    );
    assert!(
        names.contains(&"file"),
        "registry should contain 'file' sink"
    );
}
