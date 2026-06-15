// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end SCD2 test for the Parquet file sink. Drives 4 sequential runs
//! through `FileSink` against a real on-disk `.parquet` file and reads it
//! back via `parquet::arrow::arrow_reader` to assert SCD2 invariants.

use armillary_connectors::FileSink;
use armillary_datafusion::provider::{MaterializationContext, PipelineSink, WriteOptions};
use armillary_engine::materialization::{
    ChangeDetection, HardDeletes, MaterializationPolicy, ReadMode, SnapshotPolicy, WriteStrategy,
};
use armillary_engine::node::SinkConfig;
use arrow::array::{
    Array, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;
use tempfile::TempDir;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("plan", DataType::Utf8, false),
    ]))
}

fn batch(ids: &[i64], emails: &[&str], plans: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(emails.to_vec())),
            Arc::new(StringArray::from(plans.to_vec())),
        ],
    )
    .unwrap()
}

fn snapshot_policy(hard_deletes: HardDeletes) -> MaterializationPolicy {
    MaterializationPolicy {
        read_mode: ReadMode::Full,
        write_strategy: WriteStrategy::Snapshot,
        watermark: None,
        unique_keys: Some(vec!["customer_id".into()]),
        partition_column: None,
        on_schema_change: Default::default(),
        first_run: Default::default(),
        lookback: "PT0S".into(),
        snapshot: Some(SnapshotPolicy {
            change_detection: ChangeDetection::Check,
            check_columns: Some(vec!["email".into(), "plan".into()]),
            updated_at_column: None,
            hard_deletes,
        }),
    }
}

fn config_with_policy(path: &std::path::Path, policy: MaterializationPolicy) -> SinkConfig {
    SinkConfig {
        connector: "parquet".to_string(),
        materialization: Some(policy),
        config: serde_json::json!({ "path": path.to_str().unwrap(), "format": "parquet" }),
    }
}

async fn run_snapshot(
    sink: &FileSink,
    path: &std::path::Path,
    policy: MaterializationPolicy,
    data: Vec<RecordBatch>,
) -> armillary_datafusion::provider::MaterializationReceipt {
    let cfg = config_with_policy(path, policy.clone());
    let ctx = MaterializationContext::from_policy(Some(&policy));
    sink.write(&cfg, data, &WriteOptions::default(), &ctx)
        .await
        .unwrap()
}

fn read_target(path: &std::path::Path) -> RecordBatch {
    let file = std::fs::File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let target_schema = builder.schema().clone();
    let reader = builder.build().unwrap();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    arrow::compute::concat_batches(&target_schema, &batches).unwrap()
}

fn col_i64<'a>(b: &'a RecordBatch, name: &str) -> &'a Int64Array {
    b.column(b.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}
fn col_str<'a>(b: &'a RecordBatch, name: &str) -> &'a StringArray {
    b.column(b.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}
fn col_bool<'a>(b: &'a RecordBatch, name: &str) -> &'a BooleanArray {
    b.column(b.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
}
fn col_ts<'a>(b: &'a RecordBatch, name: &str) -> &'a TimestampMicrosecondArray {
    b.column(b.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_snapshot_lifecycle_full_check() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("customers.parquet");
    let sink = FileSink::new();

    // ---- Run 1: 3 fresh customers ----
    let r1 = run_snapshot(
        &sink,
        &path,
        snapshot_policy(HardDeletes::Ignore),
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@x.com", "c@x.com"],
            &["free", "pro", "free"],
        )],
    )
    .await;
    assert_eq!(r1.rows_inserted, 3, "first run opens 3 versions");
    assert_eq!(r1.rows_updated, 0);
    assert_eq!(r1.rows_deleted, 0);

    let t1 = read_target(&path);
    assert_eq!(t1.num_rows(), 3);
    // All armillary_is_current = true, all armillary_valid_to = NULL.
    let is_cur = col_bool(&t1, "armillary_is_current");
    let vt = col_ts(&t1, "armillary_valid_to");
    for i in 0..t1.num_rows() {
        assert!(is_cur.value(i));
        assert!(vt.is_null(i));
    }
    // SCD ids unique.
    let ids: std::collections::HashSet<&str> = (0..t1.num_rows())
        .map(|i| col_str(&t1, "armillary_scd_id").value(i))
        .collect();
    assert_eq!(ids.len(), 3);

    // ---- Run 2: idempotent rerun (same data) ----
    let r2 = run_snapshot(
        &sink,
        &path,
        snapshot_policy(HardDeletes::Ignore),
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@x.com", "c@x.com"],
            &["free", "pro", "free"],
        )],
    )
    .await;
    assert_eq!(r2.rows_inserted, 0, "idempotent rerun opens nothing");
    assert_eq!(r2.rows_updated, 0);

    let t2 = read_target(&path);
    assert_eq!(t2.num_rows(), 3);
    let cur_count = (0..t2.num_rows())
        .filter(|i| col_bool(&t2, "armillary_is_current").value(*i))
        .count();
    assert_eq!(cur_count, 3);

    // ---- Run 3: customer 2's plan changes ----
    let r3 = run_snapshot(
        &sink,
        &path,
        snapshot_policy(HardDeletes::Ignore),
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@x.com", "c@x.com"],
            &["free", "enterprise", "free"], // 2 changed
        )],
    )
    .await;
    assert_eq!(r3.rows_inserted, 1, "one new version opened");
    assert_eq!(r3.rows_updated, 1, "one current version closed");

    let t3 = read_target(&path);
    assert_eq!(t3.num_rows(), 4, "3 originals + 1 new version");
    // Customer 2 should now have two versions: one closed, one current.
    let mut closed_for_2 = 0;
    let mut current_for_2 = 0;
    for i in 0..t3.num_rows() {
        if col_i64(&t3, "customer_id").value(i) != 2 {
            continue;
        }
        if col_bool(&t3, "armillary_is_current").value(i) {
            current_for_2 += 1;
            assert_eq!(col_str(&t3, "plan").value(i), "enterprise");
        } else {
            closed_for_2 += 1;
            assert!(!col_ts(&t3, "armillary_valid_to").is_null(i));
            assert_eq!(col_str(&t3, "plan").value(i), "pro");
        }
    }
    assert_eq!(closed_for_2, 1);
    assert_eq!(current_for_2, 1);

    // ---- Run 4: customer 3 disappears, hard_deletes = Invalidate ----
    let r4 = run_snapshot(
        &sink,
        &path,
        snapshot_policy(HardDeletes::Invalidate),
        vec![batch(
            &[1, 2],
            &["a@x.com", "b@x.com"],
            &["free", "enterprise"],
        )],
    )
    .await;
    assert_eq!(r4.rows_inserted, 0);
    assert_eq!(r4.rows_updated, 1, "customer 3 invalidated");
    assert_eq!(r4.rows_deleted, 0, "invalidate is not a physical delete");

    let t4 = read_target(&path);
    // Same row count — invalidate just flips metadata on customer 3's current row.
    assert_eq!(t4.num_rows(), 4);
    // Customer 3 must have zero current versions.
    let three_current = (0..t4.num_rows())
        .filter(|i| {
            col_i64(&t4, "customer_id").value(*i) == 3
                && col_bool(&t4, "armillary_is_current").value(*i)
        })
        .count();
    assert_eq!(
        three_current, 0,
        "customer 3 has no current version after invalidate"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_snapshot_hard_delete_drops_all_versions() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("customers.parquet");
    let sink = FileSink::new();

    // Open + change customer 2 so it has both a historical and a current version.
    run_snapshot(
        &sink,
        &path,
        snapshot_policy(HardDeletes::Ignore),
        vec![batch(&[1, 2], &["a@x.com", "b@x.com"], &["free", "pro"])],
    )
    .await;
    run_snapshot(
        &sink,
        &path,
        snapshot_policy(HardDeletes::Ignore),
        vec![batch(
            &[1, 2],
            &["a@x.com", "b@x.com"],
            &["free", "enterprise"],
        )],
    )
    .await;
    let mid = read_target(&path);
    assert_eq!(mid.num_rows(), 3);

    // Now drop customer 2 with HardDeletes::Delete — both versions must vanish.
    let r = run_snapshot(
        &sink,
        &path,
        snapshot_policy(HardDeletes::Delete),
        vec![batch(&[1], &["a@x.com"], &["free"])],
    )
    .await;
    assert_eq!(r.rows_deleted, 1);

    let after = read_target(&path);
    let twos = (0..after.num_rows())
        .filter(|i| col_i64(&after, "customer_id").value(*i) == 2)
        .count();
    assert_eq!(twos, 0, "all versions of deleted key are gone");
    assert_eq!(after.num_rows(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_snapshot_rejects_csv_format() {
    // CSV+snapshot must be rejected at validate_config time so users see the
    // error during pipeline import, not at first write.
    let sink = FileSink::new();
    let cfg = SinkConfig {
        connector: "csv".to_string(),
        materialization: Some(snapshot_policy(HardDeletes::Ignore)),
        config: serde_json::json!({ "path": "/tmp/x.csv", "format": "csv" }),
    };
    let err = sink.validate_config(&cfg).unwrap_err().to_string();
    assert!(err.to_lowercase().contains("snapshot") || err.contains("SCD2"));
    assert!(err.contains("parquet"));
}
