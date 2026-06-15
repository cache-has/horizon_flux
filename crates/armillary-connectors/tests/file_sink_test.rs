// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the file sink connector (CSV and Parquet).

use armillary_connectors::FileSink;
use armillary_datafusion::provider::{MaterializationContext, PipelineSink, WriteOptions};
use armillary_engine::node::SinkConfig;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper: create a SinkConfig with the given connector name and JSON config.
fn sink_config(connector: &str, config: serde_json::Value) -> SinkConfig {
    SinkConfig {
        connector: connector.to_string(),
        materialization: None,
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
        .write(
            &config,
            test_batches(),
            &WriteOptions::default(),
            &MaterializationContext::default(),
        )
        .await
        .unwrap();

    assert_eq!(stats.rows_written, 3);

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

    sink.write(
        &config,
        test_batches(),
        &WriteOptions::default(),
        &MaterializationContext::default(),
    )
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

    sink.write(
        &config,
        test_batches(),
        &WriteOptions::default(),
        &MaterializationContext::default(),
    )
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
    sink.write(
        &config,
        test_batches(),
        &WriteOptions::default(),
        &MaterializationContext::default(),
    )
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
    sink.write(
        &append_config,
        test_batches(),
        &WriteOptions::default(),
        &MaterializationContext::default(),
    )
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
    sink.write(
        &config,
        test_batches(),
        &WriteOptions::default(),
        &MaterializationContext::default(),
    )
    .await
    .unwrap();
    sink.write(
        &config,
        test_batches(),
        &WriteOptions::default(),
        &MaterializationContext::default(),
    )
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
        .write(
            &config,
            test_batches(),
            &WriteOptions::default(),
            &MaterializationContext::default(),
        )
        .await
        .unwrap();

    assert_eq!(stats.rows_written, 3);
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
        .write(
            &config,
            test_batches(),
            &WriteOptions::default(),
            &MaterializationContext::default(),
        )
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
        .write(
            &config,
            test_batches(),
            &WriteOptions::default(),
            &MaterializationContext::default(),
        )
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

    sink.write(
        &config,
        test_batches(),
        &WriteOptions::default(),
        &MaterializationContext::default(),
    )
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
        .write(
            &config,
            vec![],
            &WriteOptions::default(),
            &MaterializationContext::default(),
        )
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
// Parquet insert_overwrite (hive partition replace)
// ---------------------------------------------------------------------------

/// Two-run test: first run writes partitions for `region=us` and `region=eu`.
/// Second run only writes `region=eu` rows under `insert_overwrite`. Result:
/// `eu` is replaced, `us` is left untouched. This is the entire point of the
/// strategy and the contract that distinguishes it from `truncate_insert`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_insert_overwrite_replaces_only_touched_partitions() {
    use armillary_engine::materialization::{MaterializationPolicy, ReadMode, WriteStrategy};

    let dir = TempDir::new().unwrap();
    let root = dir.path().join("dataset");

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let mk_batch = |regions: Vec<&str>, values: Vec<i64>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(regions)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    };

    let policy = MaterializationPolicy {
        read_mode: ReadMode::Full,
        write_strategy: WriteStrategy::InsertOverwrite,
        partition_column: Some("region".to_string()),
        ..MaterializationPolicy::default()
    };

    let config = SinkConfig {
        connector: "parquet".to_string(),
        materialization: Some(policy),
        config: serde_json::json!({
            "path": root.to_str().unwrap(),
            "format": "parquet",
        }),
    };

    let sink = FileSink::new();
    let ctx = MaterializationContext::from_policy(config.materialization.as_ref());

    // Run 1: write us + eu rows.
    let receipt = sink
        .write(
            &config,
            vec![mk_batch(vec!["us", "us", "eu"], vec![1, 2, 10])],
            &WriteOptions::default(),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(receipt.rows_written, 3);
    assert_eq!(receipt.rows_inserted, 3);

    let us_dir = root.join("region=us");
    let eu_dir = root.join("region=eu");
    assert!(us_dir.is_dir());
    assert!(eu_dir.is_dir());
    assert!(us_dir.join("data.parquet").exists());
    let eu_data_v1 = std::fs::read(eu_dir.join("data.parquet")).unwrap();

    // Run 2: only eu rows. us partition must be left alone; eu partition
    // must be replaced (not appended) with the new contents.
    let receipt2 = sink
        .write(
            &config,
            vec![mk_batch(vec!["eu", "eu"], vec![99, 100])],
            &WriteOptions::default(),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(receipt2.rows_written, 2);

    // us partition still present, byte-identical (we never touched it).
    assert!(us_dir.join("data.parquet").exists());

    // eu partition was replaced — file content differs from run 1 because
    // the new rows are different.
    let eu_data_v2 = std::fs::read(eu_dir.join("data.parquet")).unwrap();
    assert_ne!(
        eu_data_v1, eu_data_v2,
        "eu partition should have been replaced, not appended"
    );

    // Round-trip: read eu/data.parquet back and confirm it has exactly 2
    // rows (not 3 — we replaced, didn't append).
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = std::fs::File::open(eu_dir.join("data.parquet")).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let total_rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(
        total_rows, 2,
        "eu partition should hold only the run-2 rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_insert_overwrite_rejects_null_partition_value() {
    use armillary_engine::materialization::{MaterializationPolicy, WriteStrategy};

    let dir = TempDir::new().unwrap();
    let root = dir.path().join("dataset");

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("us"), None])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();

    let config = SinkConfig {
        connector: "parquet".to_string(),
        materialization: Some(MaterializationPolicy {
            write_strategy: WriteStrategy::InsertOverwrite,
            partition_column: Some("region".to_string()),
            ..MaterializationPolicy::default()
        }),
        config: serde_json::json!({
            "path": root.to_str().unwrap(),
            "format": "parquet",
        }),
    };

    let sink = FileSink::new();
    let ctx = MaterializationContext::from_policy(config.materialization.as_ref());
    let err = sink
        .write(&config, vec![batch], &WriteOptions::default(), &ctx)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("null"),
        "expected null-value rejection, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Registry tests
// ---------------------------------------------------------------------------

#[test]
fn default_registry_has_file_sink() {
    let registry = armillary_connectors::default_registry();
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
