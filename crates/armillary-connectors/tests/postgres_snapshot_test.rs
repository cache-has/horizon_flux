// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Live-PostgreSQL integration tests for `WriteStrategy::Snapshot`.
//!
//! These tests require a running PostgreSQL instance. Set
//! `ARMILLARY_TEST_PG_URL` to a connection string, or they default to
//! `postgresql://localhost:5432/postgres`. If PostgreSQL is unreachable
//! the tests early-return rather than failing — matching the convention
//! in `crates/armillary-postgres/tests/postgres_test.rs`.
//!
//! Each test creates its own isolated schema (`hf_snap_<uuid>`), runs the
//! Postgres sink against tables inside it, and drops the schema on
//! teardown.
//!
//! Coverage spans the doc 28 success criteria:
//! - first run inserts every row as a current version,
//! - re-running unchanged data is idempotent (zero new versions),
//! - changing a tracked column closes the prior version and opens a new one,
//! - `HardDeletes::Invalidate` closes the current version of a missing key,
//! - `HardDeletes::Delete` removes every historical version of a missing key,
//! - insert-only workloads accrete one current row per key across runs,
//! - mutating a non-tracked column does not open a new SCD2 version,
//! - `HardDeletes::Ignore` leaves a missing key's current row untouched.

use armillary_connectors::PostgresSink;
use armillary_datafusion::provider::{
    MaterializationContext, MaterializationReceipt, PipelineSink, WriteOptions,
};
use armillary_engine::materialization::{
    ChangeDetection, HardDeletes, MaterializationPolicy, ReadMode, SnapshotPolicy, WriteStrategy,
};
use armillary_engine::node::SinkConfig;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};

const DEFAULT_URL: &str = "postgresql://localhost:5432/postgres";

fn base_url() -> String {
    std::env::var("ARMILLARY_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

/// Build a schema-scoped connection string by appending the
/// `options=-csearch_path%3D{schema}` URL parameter, exactly the same
/// trick `crates/armillary-postgres/tests/postgres_test.rs` uses.
fn schema_url(schema: &str) -> String {
    let base = base_url();
    let sep = if base.contains('?') { '&' } else { '?' };
    format!("{base}{sep}options=-csearch_path%3D{schema}")
}

/// Open a raw client and create an isolated schema. Returns
/// `(client, schema_name, schema_scoped_url)`. Returns `None` if
/// Postgres is unreachable so the test can early-return.
async fn setup() -> Option<(Client, String, String)> {
    let url = base_url();
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await.ok()?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("setup connection error: {e}");
        }
    });
    let schema = format!("hf_snap_{}", uuid::Uuid::new_v4().simple());
    client
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .ok()?;
    let scoped = schema_url(&schema);

    // Open a fresh client on the scoped URL so subsequent helper queries
    // see the search_path. The setup client (default search_path) is
    // discarded — we keep it only long enough to create the schema.
    let (scoped_client, scoped_conn) = tokio_postgres::connect(&scoped, NoTls).await.ok()?;
    tokio::spawn(async move {
        let _ = scoped_conn.await;
    });
    Some((scoped_client, schema, scoped))
}

async fn teardown(schema: &str) {
    let url = base_url();
    if let Ok((client, conn)) = tokio_postgres::connect(&url, NoTls).await {
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let _ = client
            .batch_execute(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
            .await;
    }
}

fn customer_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("plan", DataType::Utf8, false),
    ]))
}

fn batch(ids: &[i64], emails: &[&str], plans: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        customer_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(emails.to_vec())),
            Arc::new(StringArray::from(plans.to_vec())),
        ],
    )
    .unwrap()
}

fn snapshot_sink_config(
    connection_string: &str,
    table: &str,
    hard_deletes: HardDeletes,
) -> SinkConfig {
    SinkConfig {
        connector: "postgresql".to_string(),
        materialization: Some(MaterializationPolicy {
            read_mode: ReadMode::Full,
            write_strategy: WriteStrategy::Snapshot,
            unique_keys: Some(vec!["id".into()]),
            partition_column: None,
            watermark: None,
            on_schema_change: Default::default(),
            first_run: Default::default(),
            lookback: "PT0S".into(),
            snapshot: Some(SnapshotPolicy {
                change_detection: ChangeDetection::Check,
                check_columns: Some(vec!["email".into(), "plan".into()]),
                updated_at_column: None,
                hard_deletes,
            }),
        }),
        config: serde_json::json!({
            "connection_string": connection_string,
            "table": table,
        }),
    }
}

fn snapshot_ctx() -> MaterializationContext {
    MaterializationContext {
        read_mode: ReadMode::Full,
        write_strategy: WriteStrategy::Snapshot,
        unique_keys: vec!["id".into()],
        partition_column: None,
        watermark_column: None,
        apply_schema_changes: false,
    }
}

async fn run_snapshot(
    sink: &PostgresSink,
    cfg: &SinkConfig,
    batches: Vec<RecordBatch>,
) -> MaterializationReceipt {
    sink.write(cfg, batches, &WriteOptions::default(), &snapshot_ctx())
        .await
        .expect("snapshot write")
}

async fn current_count(client: &Client, table: &str) -> i64 {
    let row = client
        .query_one(
            &format!("SELECT COUNT(*) FROM \"{table}\" WHERE armillary_is_current"),
            &[],
        )
        .await
        .expect("count current");
    row.get(0)
}

async fn total_count(client: &Client, table: &str) -> i64 {
    let row = client
        .query_one(&format!("SELECT COUNT(*) FROM \"{table}\""), &[])
        .await
        .expect("count total");
    row.get(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_first_run_inserts_all_as_current_versions() {
    let Some((client, schema, scoped_url)) = setup().await else {
        eprintln!("postgres unreachable, skipping");
        return;
    };
    let table = "customers_first_run";
    let cfg = snapshot_sink_config(&scoped_url, table, HardDeletes::Ignore);

    let receipt = run_snapshot(
        &PostgresSink::new(),
        &cfg,
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@x.com", "c@x.com"],
            &["pro", "free", "pro"],
        )],
    )
    .await;

    assert_eq!(current_count(&client, table).await, 3);
    assert_eq!(total_count(&client, table).await, 3);
    assert_eq!(receipt.rows_inserted, 3, "all 3 are new versions");
    assert_eq!(receipt.rows_updated, 0);
    assert_eq!(receipt.rows_deleted, 0);

    // Surrogate keys are unique per row.
    let row = client
        .query_one(
            &format!("SELECT COUNT(DISTINCT armillary_scd_id) FROM \"{table}\""),
            &[],
        )
        .await
        .unwrap();
    let distinct: i64 = row.get(0);
    assert_eq!(distinct, 3);

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_second_run_unchanged_is_idempotent() {
    let Some((client, schema, scoped_url)) = setup().await else {
        eprintln!("postgres unreachable, skipping");
        return;
    };
    let table = "customers_idem";
    let cfg = snapshot_sink_config(&scoped_url, table, HardDeletes::Ignore);
    let sink = PostgresSink::new();

    let initial = vec![batch(
        &[1, 2, 3],
        &["a@x.com", "b@x.com", "c@x.com"],
        &["pro", "free", "pro"],
    )];

    let _ = run_snapshot(&sink, &cfg, initial.clone()).await;
    // Capture the initial scd_ids so we can confirm they're untouched.
    let before: Vec<String> = client
        .query(
            &format!("SELECT armillary_scd_id FROM \"{table}\" ORDER BY id"),
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect();

    let receipt = run_snapshot(&sink, &cfg, initial).await;

    // Doc 28 success criterion: idempotent re-runs produce zero new versions.
    assert_eq!(receipt.rows_inserted, 0);
    assert_eq!(receipt.rows_updated, 0);
    assert_eq!(current_count(&client, table).await, 3);
    assert_eq!(total_count(&client, table).await, 3);

    let after: Vec<String> = client
        .query(
            &format!("SELECT armillary_scd_id FROM \"{table}\" ORDER BY id"),
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert_eq!(
        before, after,
        "surrogate keys unchanged across idempotent rerun"
    );

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_changed_row_closes_old_and_opens_new() {
    let Some((client, schema, scoped_url)) = setup().await else {
        eprintln!("postgres unreachable, skipping");
        return;
    };
    let table = "customers_changed";
    let cfg = snapshot_sink_config(&scoped_url, table, HardDeletes::Ignore);
    let sink = PostgresSink::new();

    let _ = run_snapshot(
        &sink,
        &cfg,
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@x.com", "c@x.com"],
            &["pro", "free", "pro"],
        )],
    )
    .await;

    // Customer 2 changes their email; everyone else identical.
    let receipt = run_snapshot(
        &sink,
        &cfg,
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@new.com", "c@x.com"],
            &["pro", "free", "pro"],
        )],
    )
    .await;

    assert_eq!(
        receipt.rows_inserted, 1,
        "one new version opened (changed row)"
    );
    assert_eq!(receipt.rows_updated, 1, "one current version closed");
    assert_eq!(receipt.rows_deleted, 0);

    assert_eq!(current_count(&client, table).await, 3);
    assert_eq!(total_count(&client, table).await, 4, "old version retained");

    // Customer 2 has exactly two history rows: one closed (b@x.com),
    // one current (b@new.com).
    let rows = client
        .query(
            &format!(
                "SELECT email, armillary_is_current, armillary_valid_to IS NULL AS open_ended \
                 FROM \"{table}\" WHERE id = 2 ORDER BY armillary_valid_from"
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let old: String = rows[0].get(0);
    let old_current: bool = rows[0].get(1);
    let old_open: bool = rows[0].get(2);
    assert_eq!(old, "b@x.com");
    assert!(!old_current);
    assert!(!old_open, "old version's armillary_valid_to must be set");
    let new: String = rows[1].get(0);
    let new_current: bool = rows[1].get(1);
    assert_eq!(new, "b@new.com");
    assert!(new_current);

    // Doc 28 success criterion: historical query returns the right
    // value at a given timestamp. Right now, "now" returns the new email.
    let row = client
        .query_one(
            &format!(
                "SELECT email FROM \"{table}\" \
                 WHERE id = 2 \
                   AND now() >= armillary_valid_from \
                   AND (armillary_valid_to IS NULL OR now() < armillary_valid_to)"
            ),
            &[],
        )
        .await
        .unwrap();
    let email: String = row.get(0);
    assert_eq!(email, "b@new.com");

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_hard_deletes_invalidate_closes_missing_rows() {
    let Some((client, schema, scoped_url)) = setup().await else {
        eprintln!("postgres unreachable, skipping");
        return;
    };
    let table = "customers_invalidate";
    let cfg = snapshot_sink_config(&scoped_url, table, HardDeletes::Invalidate);
    let sink = PostgresSink::new();

    let _ = run_snapshot(
        &sink,
        &cfg,
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@x.com", "c@x.com"],
            &["pro", "free", "pro"],
        )],
    )
    .await;

    // Customer 3 disappears from the source.
    let receipt = run_snapshot(
        &sink,
        &cfg,
        vec![batch(&[1, 2], &["a@x.com", "b@x.com"], &["pro", "free"])],
    )
    .await;

    assert_eq!(
        receipt.rows_inserted, 0,
        "nothing changed for the surviving rows"
    );
    assert_eq!(
        receipt.rows_updated, 1,
        "the missing customer's current version is closed"
    );
    assert_eq!(receipt.rows_deleted, 0, "invalidate, not delete");

    assert_eq!(current_count(&client, table).await, 2);
    // Customer 3's history row is retained but no longer current.
    let row = client
        .query_one(
            &format!(
                "SELECT armillary_is_current, armillary_valid_to IS NOT NULL \
                 FROM \"{table}\" WHERE id = 3"
            ),
            &[],
        )
        .await
        .unwrap();
    let is_current: bool = row.get(0);
    let valid_to_set: bool = row.get(1);
    assert!(!is_current);
    assert!(valid_to_set);

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_hard_deletes_delete_removes_all_history() {
    let Some((client, schema, scoped_url)) = setup().await else {
        eprintln!("postgres unreachable, skipping");
        return;
    };
    let table = "customers_delete";
    let cfg = snapshot_sink_config(&scoped_url, table, HardDeletes::Delete);
    let sink = PostgresSink::new();

    // Run twice to give customer 3 a history (close + reopen via a tracked-column change).
    let _ = run_snapshot(
        &sink,
        &cfg,
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@x.com", "c@x.com"],
            &["pro", "free", "pro"],
        )],
    )
    .await;
    let _ = run_snapshot(
        &sink,
        &cfg,
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@x.com", "c@new.com"],
            &["pro", "free", "pro"],
        )],
    )
    .await;
    // Customer 3 now has 2 historical rows.
    let row3_count: i64 = client
        .query_one(
            &format!("SELECT COUNT(*) FROM \"{table}\" WHERE id = 3"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(row3_count, 2);

    // Customer 3 disappears.
    let receipt = run_snapshot(
        &sink,
        &cfg,
        vec![batch(&[1, 2], &["a@x.com", "b@x.com"], &["pro", "free"])],
    )
    .await;

    assert_eq!(
        receipt.rows_deleted, 1,
        "one gone key → physical_deletes counted by key, not by row"
    );
    let remaining: i64 = client
        .query_one(
            &format!("SELECT COUNT(*) FROM \"{table}\" WHERE id = 3"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 0, "every historical version of id=3 is gone");
    assert_eq!(current_count(&client, table).await, 2);

    teardown(&schema).await;
}

/// Build a snapshot config that tracks an arbitrary subset of columns.
/// Used by the non-tracked-column test below.
fn snapshot_sink_config_tracking(
    connection_string: &str,
    table: &str,
    check_columns: Vec<String>,
) -> SinkConfig {
    SinkConfig {
        connector: "postgresql".to_string(),
        materialization: Some(MaterializationPolicy {
            read_mode: ReadMode::Full,
            write_strategy: WriteStrategy::Snapshot,
            unique_keys: Some(vec!["id".into()]),
            partition_column: None,
            watermark: None,
            on_schema_change: Default::default(),
            first_run: Default::default(),
            lookback: "PT0S".into(),
            snapshot: Some(SnapshotPolicy {
                change_detection: ChangeDetection::Check,
                check_columns: Some(check_columns),
                updated_at_column: None,
                hard_deletes: HardDeletes::Ignore,
            }),
        }),
        config: serde_json::json!({
            "connection_string": connection_string,
            "table": table,
        }),
    }
}

/// Insert-only workload: each run brings entirely fresh keys. Every run
/// must open exactly N new current versions and never close anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_insert_only_multiple_runs_open_new_versions() {
    let Some((client, schema, scoped_url)) = setup().await else {
        eprintln!("postgres unreachable, skipping");
        return;
    };
    let table = "customers_insert_only";
    let cfg = snapshot_sink_config(&scoped_url, table, HardDeletes::Ignore);
    let sink = PostgresSink::new();

    let r1 = run_snapshot(
        &sink,
        &cfg,
        vec![batch(&[1, 2], &["a@x.com", "b@x.com"], &["pro", "free"])],
    )
    .await;
    assert_eq!(r1.rows_inserted, 2);
    assert_eq!(r1.rows_updated, 0);

    // Run 2: two brand-new keys; the first run's keys are *also* still
    // present (snapshot is a state-of-the-world write, not a delta).
    let r2 = run_snapshot(
        &sink,
        &cfg,
        vec![batch(
            &[1, 2, 3, 4],
            &["a@x.com", "b@x.com", "c@x.com", "d@x.com"],
            &["pro", "free", "pro", "free"],
        )],
    )
    .await;
    assert_eq!(r2.rows_inserted, 2, "only the two new keys are inserted");
    assert_eq!(r2.rows_updated, 0);
    assert_eq!(r2.rows_deleted, 0);

    // Run 3: yet another fresh key, plus the existing four.
    let r3 = run_snapshot(
        &sink,
        &cfg,
        vec![batch(
            &[1, 2, 3, 4, 5],
            &["a@x.com", "b@x.com", "c@x.com", "d@x.com", "e@x.com"],
            &["pro", "free", "pro", "free", "pro"],
        )],
    )
    .await;
    assert_eq!(r3.rows_inserted, 1);
    assert_eq!(r3.rows_updated, 0);

    assert_eq!(current_count(&client, table).await, 5);
    assert_eq!(
        total_count(&client, table).await,
        5,
        "no historical versions accumulated when nothing changed"
    );

    teardown(&schema).await;
}

/// A column that is not in `check_columns` mutates between runs. The row
/// must remain a single current version: snapshot semantics ignore
/// non-tracked metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_non_tracked_column_change_does_not_open_new_version() {
    let Some((client, schema, scoped_url)) = setup().await else {
        eprintln!("postgres unreachable, skipping");
        return;
    };
    let table = "customers_non_tracked";
    // Only `email` is tracked. `plan` is mutable metadata.
    let cfg = snapshot_sink_config_tracking(&scoped_url, table, vec!["email".into()]);
    let sink = PostgresSink::new();

    let _ = run_snapshot(
        &sink,
        &cfg,
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@x.com", "c@x.com"],
            &["pro", "free", "pro"],
        )],
    )
    .await;

    // Customer 2's `plan` flips free→pro. `email` is unchanged.
    let receipt = run_snapshot(
        &sink,
        &cfg,
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@x.com", "c@x.com"],
            &["pro", "pro", "pro"],
        )],
    )
    .await;

    assert_eq!(
        receipt.rows_inserted, 0,
        "non-tracked column change must not open a new version"
    );
    assert_eq!(receipt.rows_updated, 0);
    assert_eq!(receipt.rows_deleted, 0);
    assert_eq!(current_count(&client, table).await, 3);
    assert_eq!(
        total_count(&client, table).await,
        3,
        "history depth unchanged"
    );

    teardown(&schema).await;
}

/// `HardDeletes::Ignore`: a key disappearing from the source must NOT
/// close the existing current version. The row stays open-ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_hard_deletes_ignore_keeps_missing_row_current() {
    let Some((client, schema, scoped_url)) = setup().await else {
        eprintln!("postgres unreachable, skipping");
        return;
    };
    let table = "customers_ignore_missing";
    let cfg = snapshot_sink_config(&scoped_url, table, HardDeletes::Ignore);
    let sink = PostgresSink::new();

    let _ = run_snapshot(
        &sink,
        &cfg,
        vec![batch(
            &[1, 2, 3],
            &["a@x.com", "b@x.com", "c@x.com"],
            &["pro", "free", "pro"],
        )],
    )
    .await;

    // Customer 3 disappears from the source.
    let receipt = run_snapshot(
        &sink,
        &cfg,
        vec![batch(&[1, 2], &["a@x.com", "b@x.com"], &["pro", "free"])],
    )
    .await;

    assert_eq!(receipt.rows_inserted, 0);
    assert_eq!(
        receipt.rows_updated, 0,
        "Ignore must not close the missing row"
    );
    assert_eq!(receipt.rows_deleted, 0);

    // Customer 3 is still current and still open-ended.
    assert_eq!(current_count(&client, table).await, 3);
    assert_eq!(total_count(&client, table).await, 3);
    let row = client
        .query_one(
            &format!(
                "SELECT armillary_is_current, armillary_valid_to IS NULL \
                 FROM \"{table}\" WHERE id = 3"
            ),
            &[],
        )
        .await
        .unwrap();
    let is_current: bool = row.get(0);
    let still_open: bool = row.get(1);
    assert!(is_current);
    assert!(still_open);

    teardown(&schema).await;
}
