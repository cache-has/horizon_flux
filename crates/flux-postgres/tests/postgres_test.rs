// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for PostgreSQL storage backends.
//!
//! These tests require a running PostgreSQL instance. Set
//! `HORIZON_FLUX_TEST_PG_URL` to a connection string, or they will default
//! to `postgresql://localhost:5432/postgres`.
//!
//! Each test creates its own schema (via `SET search_path`) to avoid
//! interference between concurrent runs.

use deadpool_postgres::{Config, Pool, Runtime};
use flux_datafusion::incremental_state::{IncrementalSchemaRecord, IncrementalState};
use flux_datafusion::run::{NodeRunStats, RunStatus};
use flux_datafusion::storage::{EnvironmentStorage, IncrementalStateStorage, RunStorage};
use flux_engine::NodeId;
use flux_engine::pipeline::Pipeline;
use flux_engine::storage::PipelineStorage;
use flux_postgres::{PostgresEnvironmentStore, PostgresPipelineStore, PostgresRunStore};
use std::time::{Duration, SystemTime};
use tokio_postgres::NoTls;

/// Default test connection URL.
const DEFAULT_URL: &str = "postgresql://localhost:5432/postgres";

fn test_url() -> String {
    std::env::var("HORIZON_FLUX_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

/// Create a pool and an isolated schema for one test. Returns the pool and
/// schema name so the caller can clean up.
async fn setup_pool() -> Option<(Pool, String)> {
    let url = test_url();
    let mut cfg = Config::new();
    cfg.url = Some(url);
    let pool = match cfg.create_pool(Some(Runtime::Tokio1), NoTls) {
        Ok(p) => p,
        Err(_) => return None,
    };

    // Verify connectivity.
    let client = match pool.get().await {
        Ok(c) => c,
        Err(_) => return None,
    };

    // Create a unique schema for isolation.
    let schema = format!(
        "hf_test_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")
    );
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; SET search_path TO {schema};"
        ))
        .await
        .ok()?;

    // Reconfigure pool to use the test schema by appending options.
    // Easier: just set search_path on each connection via an init statement.
    // deadpool-postgres doesn't support per-connection init, so we create a
    // new pool with the schema in the URL options.
    let base_url = test_url();
    let sep = if base_url.contains('?') { '&' } else { '?' };
    let schema_url = format!("{base_url}{sep}options=-csearch_path%3D{schema}");

    let mut cfg2 = Config::new();
    cfg2.url = Some(schema_url);
    let pool = cfg2.create_pool(Some(Runtime::Tokio1), NoTls).ok()?;

    // Run schema creation.
    flux_postgres::ensure_schema(&pool).await.ok()?;

    Some((pool, schema))
}

/// Drop the test schema on cleanup.
async fn teardown(schema: &str) {
    let url = test_url();
    let mut cfg = Config::new();
    cfg.url = Some(url);
    if let Ok(pool) = cfg.create_pool(Some(Runtime::Tokio1), NoTls) {
        if let Ok(client) = pool.get().await {
            let _ = client
                .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                .await;
        }
    }
}

fn test_pipeline(name: &str) -> Pipeline {
    Pipeline {
        name: name.to_string(),
        version: 1,
        default_environment: "dev".to_string(),
        variables: Default::default(),
        environment_overrides: Default::default(),
        sample_config: None,
        cache_row_limit: None,
        code_dir: None,
        nodes: vec![],
        edges: vec![],
    }
}

// ---------------------------------------------------------------------------
// Schema / migration tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn schema_creation_is_idempotent() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    // Running ensure_schema again should not fail.
    flux_postgres::ensure_schema(&pool)
        .await
        .expect("second ensure_schema should succeed");

    // A third time for good measure.
    flux_postgres::ensure_schema(&pool)
        .await
        .expect("third ensure_schema should succeed");

    // Verify that the schema_version table has entries.
    let client = pool.get().await.unwrap();
    let row = client
        .query_one("SELECT COUNT(*)::INTEGER FROM schema_version", &[])
        .await
        .unwrap();
    let count: i32 = row.get(0);
    // First call inserts version, subsequent calls skip (version already current).
    assert!(count >= 1, "schema_version should have at least one row");

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_creates_all_tables() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let client = pool.get().await.unwrap();
    let expected_tables = [
        "schema_version",
        "pipelines",
        "pipeline_versions",
        "pipeline_runs",
        "node_run_stats",
        "environments",
        "table_overrides",
    ];
    for table in &expected_tables {
        let row = client
            .query_one(
                "SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )",
                &[&schema, table],
            )
            .await
            .unwrap();
        let exists: bool = row.get(0);
        assert!(exists, "table {table} should exist in schema {schema}");
    }

    teardown(&schema).await;
}

// ---------------------------------------------------------------------------
// Pipeline store tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_create_and_get() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresPipelineStore::new(pool);
    let record = store.create(test_pipeline("test")).unwrap();
    assert_eq!(record.pipeline.name, "test");
    assert_eq!(record.pipeline.version, 1);
    assert_eq!(record.run_count, 0);
    assert!(record.last_run_at.is_none());

    let fetched = store.get(&record.id).unwrap().unwrap();
    assert_eq!(fetched.id, record.id);
    assert_eq!(fetched.pipeline.name, "test");

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_get_by_name() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresPipelineStore::new(pool);
    let record = store.create(test_pipeline("lookup")).unwrap();

    let found = store.get_by_name("lookup").unwrap().unwrap();
    assert_eq!(found.id, record.id);
    assert!(store.get_by_name("nope").unwrap().is_none());

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_name_conflict() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresPipelineStore::new(pool);
    store.create(test_pipeline("dup")).unwrap();
    let err = store.create(test_pipeline("dup")).unwrap_err();
    assert!(
        matches!(
            err,
            flux_engine::pipeline_store::PipelineStoreError::NameConflict(_)
        ),
        "expected NameConflict, got: {err:?}"
    );

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_list_and_count() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresPipelineStore::new(pool);
    store.create(test_pipeline("b")).unwrap();
    store.create(test_pipeline("a")).unwrap();

    let all = store.list(100, 0).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].pipeline.name, "a");
    assert_eq!(all[1].pipeline.name, "b");
    assert_eq!(store.count().unwrap(), 2);

    // Pagination.
    let page = store.list(1, 0).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].pipeline.name, "a");

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_update() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresPipelineStore::new(pool);
    let record = store.create(test_pipeline("old")).unwrap();

    let updated = store.update(&record.id, test_pipeline("new")).unwrap();
    assert_eq!(updated.pipeline.name, "new");
    assert_eq!(updated.id, record.id);
    assert_eq!(updated.pipeline.version, 2);

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_delete() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresPipelineStore::new(pool);
    let record = store.create(test_pipeline("doomed")).unwrap();
    store.delete(&record.id).unwrap();
    assert!(store.get(&record.id).unwrap().is_none());

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_version_history() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresPipelineStore::new(pool);
    let record = store.create(test_pipeline("versioned")).unwrap();
    assert_eq!(store.count_versions(&record.id).unwrap(), 1);

    store
        .update(&record.id, test_pipeline("versioned"))
        .unwrap();
    store
        .update(&record.id, test_pipeline("versioned"))
        .unwrap();
    assert_eq!(store.count_versions(&record.id).unwrap(), 3);

    let versions = store.list_versions(&record.id, 100, 0).unwrap();
    assert_eq!(versions.len(), 3);
    assert_eq!(versions[0].version, 3); // newest first
    assert_eq!(versions[2].version, 1);

    let v1 = store.get_version(&record.id, 1).unwrap().unwrap();
    assert_eq!(v1.version, 1);
    assert!(store.get_version(&record.id, 99).unwrap().is_none());

    // Delete cascades versions.
    store.delete(&record.id).unwrap();
    assert_eq!(store.count_versions(&record.id).unwrap(), 0);

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_record_run() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresPipelineStore::new(pool);
    let record = store.create(test_pipeline("runner")).unwrap();

    store.record_run(&record.id).unwrap();
    let after = store.get(&record.id).unwrap().unwrap();
    assert_eq!(after.run_count, 1);
    assert!(after.last_run_at.is_some());

    store.record_run(&record.id).unwrap();
    let after2 = store.get(&record.id).unwrap().unwrap();
    assert_eq!(after2.run_count, 2);

    teardown(&schema).await;
}

// ---------------------------------------------------------------------------
// Run store tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn run_create_and_get() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresRunStore::new(pool);
    let run = store.create_run("my-pipeline", "dev").unwrap();
    assert_eq!(run.pipeline_name, "my-pipeline");
    assert_eq!(run.environment, "dev");
    assert_eq!(run.status, RunStatus::Pending);
    assert!(run.start_time.is_none());

    let fetched = store.get_run(&run.id).unwrap().unwrap();
    assert_eq!(fetched.id, run.id);
    assert_eq!(fetched.pipeline_name, "my-pipeline");

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_lifecycle() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresRunStore::new(pool);
    let run = store.create_run("pipe", "prod").unwrap();

    // Pending → Running.
    let start = SystemTime::now();
    store.set_running(&run.id, start).unwrap();
    let running = store.get_run(&run.id).unwrap().unwrap();
    assert_eq!(running.status, RunStatus::Running);
    assert!(running.start_time.is_some());

    // Save node stats.
    let stats = NodeRunStats {
        node_id: NodeId::new("node-1"),
        start_time: start,
        end_time: start + Duration::from_millis(500),
        rows_in: 100,
        rows_out: 80,
        error: None,
        materialization_receipt: None,
    };
    store.save_node_stats(&run.id, &stats).unwrap();

    // Running → Success.
    let end = start + Duration::from_secs(1);
    store
        .finish_run(&run.id, RunStatus::Success, end, None)
        .unwrap();
    let finished = store.get_run(&run.id).unwrap().unwrap();
    assert_eq!(finished.status, RunStatus::Success);
    assert!(finished.end_time.is_some());
    assert_eq!(finished.node_stats.len(), 1);
    assert_eq!(finished.node_stats[0].rows_in, 100);

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_failed_with_error() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresRunStore::new(pool);
    let run = store.create_run("pipe", "dev").unwrap();
    let start = SystemTime::now();
    store.set_running(&run.id, start).unwrap();

    let end = start + Duration::from_millis(200);
    store
        .finish_run(&run.id, RunStatus::Failed, end, Some("boom"))
        .unwrap();

    let failed = store.get_run(&run.id).unwrap().unwrap();
    assert_eq!(failed.status, RunStatus::Failed);
    assert_eq!(failed.error.as_deref(), Some("boom"));

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_list_and_filter() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresRunStore::new(pool);
    let now = SystemTime::now();

    // Create runs for different pipelines.
    let r1 = store.create_run("alpha", "dev").unwrap();
    store.set_running(&r1.id, now).unwrap();

    let r2 = store.create_run("alpha", "prod").unwrap();
    store
        .set_running(&r2.id, now + Duration::from_secs(1))
        .unwrap();

    let _r3 = store.create_run("beta", "dev").unwrap();

    // Unfiltered: all 3.
    let all = store.list_runs(None, 100).unwrap();
    assert_eq!(all.len(), 3);

    // Filtered by pipeline.
    let alpha = store.list_runs(Some("alpha"), 100).unwrap();
    assert_eq!(alpha.len(), 2);
    // Most recent first.
    assert_eq!(alpha[0].id, r2.id);

    let beta = store.list_runs(Some("beta"), 100).unwrap();
    assert_eq!(beta.len(), 1);

    teardown(&schema).await;
}

// ---------------------------------------------------------------------------
// Environment store tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn environment_defaults() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresEnvironmentStore::new(pool).unwrap();

    // Default `prod` and `dev` should exist.
    let prod = store.get("prod").unwrap().unwrap();
    assert!(prod.fallback.is_none());

    let dev = store.get("dev").unwrap().unwrap();
    assert_eq!(dev.fallback.as_deref(), Some("prod"));

    let all = store.list().unwrap();
    assert!(all.len() >= 2);

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_create_and_delete() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresEnvironmentStore::new(pool).unwrap();

    let staging = store.create("staging", Some("prod")).unwrap();
    assert_eq!(staging.name, "staging");
    assert_eq!(staging.fallback.as_deref(), Some("prod"));

    // Duplicate.
    let err = store.create("staging", None).unwrap_err();
    assert!(
        matches!(
            err,
            flux_datafusion::error::EnvironmentError::AlreadyExists(_)
        ),
        "expected AlreadyExists, got: {err:?}"
    );

    // Delete.
    store.delete("staging").unwrap();
    assert!(store.get("staging").unwrap().is_none());

    // Cannot delete prod.
    let err = store.delete("prod").unwrap_err();
    assert!(matches!(
        err,
        flux_datafusion::error::EnvironmentError::CannotDeleteProd
    ));

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_fallback_chain() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresEnvironmentStore::new(pool).unwrap();
    store.create("staging", Some("prod")).unwrap();
    store.create("feature", Some("staging")).unwrap();

    let chain = store.fallback_chain("feature").unwrap();
    assert_eq!(chain, vec!["feature", "staging", "prod"]);

    let chain_dev = store.fallback_chain("dev").unwrap();
    assert_eq!(chain_dev, vec!["dev", "prod"]);

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_update_fallback() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresEnvironmentStore::new(pool).unwrap();
    store.create("staging", Some("prod")).unwrap();

    store.update_fallback("staging", Some("dev")).unwrap();
    let updated = store.get("staging").unwrap().unwrap();
    assert_eq!(updated.fallback.as_deref(), Some("dev"));

    // Self-referential fallback.
    let err = store
        .update_fallback("staging", Some("staging"))
        .unwrap_err();
    assert!(matches!(
        err,
        flux_datafusion::error::EnvironmentError::CyclicFallback
    ));

    // prod cannot have fallback.
    let err = store.update_fallback("prod", Some("dev")).unwrap_err();
    assert!(matches!(
        err,
        flux_datafusion::error::EnvironmentError::ProdCannotHaveFallback
    ));

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_table_overrides() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresEnvironmentStore::new(pool).unwrap();

    store
        .register_table_override("dev", "public", "users")
        .unwrap();
    store
        .register_table_override("dev", "public", "orders")
        .unwrap();

    let overrides = store.list_table_overrides("dev").unwrap();
    assert_eq!(overrides.len(), 2);
    assert_eq!(overrides[0].table_name, "orders");
    assert_eq!(overrides[1].table_name, "users");

    // Idempotent register.
    store
        .register_table_override("dev", "public", "users")
        .unwrap();
    assert_eq!(store.list_table_overrides("dev").unwrap().len(), 2);

    // Deregister.
    let removed = store
        .deregister_table_override("dev", "public", "users")
        .unwrap();
    assert!(removed);
    assert_eq!(store.list_table_overrides("dev").unwrap().len(), 1);

    // Deregister non-existent.
    let removed = store
        .deregister_table_override("dev", "public", "users")
        .unwrap();
    assert!(!removed);

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_delete_cascades_overrides() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let store = PostgresEnvironmentStore::new(pool).unwrap();
    store.create("staging", Some("prod")).unwrap();
    store
        .register_table_override("staging", "public", "users")
        .unwrap();

    store.delete("staging").unwrap();
    // Overrides should be gone with the environment.
    // Re-create to verify no leftover overrides.
    store.create("staging", Some("prod")).unwrap();
    let overrides = store.list_table_overrides("staging").unwrap();
    assert!(overrides.is_empty());

    teardown(&schema).await;
}

// ---------------------------------------------------------------------------
// Full lifecycle integration test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn full_pipeline_lifecycle() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let pipeline_store = PostgresPipelineStore::new(pool.clone());
    let run_store = PostgresRunStore::new(pool.clone());
    let env_store = PostgresEnvironmentStore::new(pool).unwrap();

    // 1. Verify default environments.
    assert!(env_store.get("dev").unwrap().is_some());

    // 2. Create a pipeline.
    let record = pipeline_store.create(test_pipeline("etl-job")).unwrap();
    assert_eq!(pipeline_store.count().unwrap(), 1);

    // 3. Update the pipeline (creates version 2).
    let updated = pipeline_store
        .update(&record.id, test_pipeline("etl-job"))
        .unwrap();
    assert_eq!(updated.pipeline.version, 2);

    // 4. Create a run.
    let run = run_store.create_run("etl-job", "dev").unwrap();
    assert_eq!(run.status, RunStatus::Pending);

    // 5. Mark running.
    let start = SystemTime::now();
    run_store.set_running(&run.id, start).unwrap();

    // 6. Save node stats.
    run_store
        .save_node_stats(
            &run.id,
            &NodeRunStats {
                node_id: NodeId::new("source-csv"),
                start_time: start,
                end_time: start + Duration::from_millis(300),
                rows_in: 0,
                rows_out: 1000,
                error: None,
                materialization_receipt: None,
            },
        )
        .unwrap();
    run_store
        .save_node_stats(
            &run.id,
            &NodeRunStats {
                node_id: NodeId::new("transform-sql"),
                start_time: start + Duration::from_millis(300),
                end_time: start + Duration::from_millis(700),
                rows_in: 1000,
                rows_out: 500,
                error: None,
                materialization_receipt: None,
            },
        )
        .unwrap();

    // 7. Finish the run.
    let end = start + Duration::from_secs(1);
    run_store
        .finish_run(&run.id, RunStatus::Success, end, None)
        .unwrap();

    // 8. Record the run on the pipeline.
    pipeline_store.record_run(&record.id).unwrap();

    // 9. Verify run state.
    let completed_run = run_store.get_run(&run.id).unwrap().unwrap();
    assert_eq!(completed_run.status, RunStatus::Success);
    assert_eq!(completed_run.node_stats.len(), 2);

    // 10. List runs filtered by pipeline.
    let runs = run_store.list_runs(Some("etl-job"), 100).unwrap();
    assert_eq!(runs.len(), 1);

    // 11. Verify pipeline metadata updated.
    let final_record = pipeline_store.get(&record.id).unwrap().unwrap();
    assert_eq!(final_record.run_count, 1);
    assert!(final_record.last_run_at.is_some());

    // 12. Delete the pipeline.
    pipeline_store.delete(&record.id).unwrap();
    assert_eq!(pipeline_store.count().unwrap(), 0);
    assert_eq!(pipeline_store.count_versions(&record.id).unwrap(), 0);

    teardown(&schema).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_state_round_trip_postgres() {
    let Some((pool, schema)) = setup_pool().await else {
        eprintln!("skipping: PostgreSQL not available");
        return;
    };

    let pipeline_store = PostgresPipelineStore::new(pool.clone());
    let run_store = PostgresRunStore::new(pool.clone());

    // Pipeline must exist first because of the FK on incremental_state.pipeline_id.
    let record = pipeline_store
        .create(test_pipeline("inc-state-pipe"))
        .unwrap();
    let pid = record.id.to_string();

    // load on empty -> None
    assert!(run_store.load_state(&pid, "sink", "dev").unwrap().is_none());

    let s1 = IncrementalState {
        pipeline_id: pid.clone(),
        node_id: "sink".into(),
        environment: "dev".into(),
        watermark_column: "updated_at".into(),
        watermark_value: "2026-04-08T00:00:00.000000000Z".into(),
        watermark_type: "timestamp".into(),
        last_run_at_ms: 1_700_000_000_000,
        last_run_id: "run-1".into(),
        rows_processed: 42,
        schema_fingerprint: Some("abc123".into()),
    };
    run_store.save_state(&s1).unwrap();
    assert_eq!(
        run_store.load_state(&pid, "sink", "dev").unwrap().unwrap(),
        s1
    );

    // upsert (advance watermark)
    let s2 = IncrementalState {
        watermark_value: "2026-04-09T00:00:00.000000000Z".into(),
        last_run_at_ms: 1_700_000_100_000,
        last_run_id: "run-2".into(),
        rows_processed: 7,
        ..s1.clone()
    };
    run_store.save_state(&s2).unwrap();
    assert_eq!(
        run_store.load_state(&pid, "sink", "dev").unwrap().unwrap(),
        s2
    );

    // env isolation + listing
    let mut prod = s1.clone();
    prod.environment = "prod".into();
    run_store.save_state(&prod).unwrap();
    assert_eq!(run_store.list_states(Some("dev")).unwrap().len(), 1);
    assert_eq!(run_store.list_states(Some("prod")).unwrap().len(), 1);
    assert_eq!(run_store.list_states(None).unwrap().len(), 2);

    // schema history
    let r1 = IncrementalSchemaRecord {
        pipeline_id: pid.clone(),
        node_id: "sink".into(),
        environment: "dev".into(),
        run_id: "run-1".into(),
        schema_json: "{\"fields\":[]}".into(),
        fingerprint: "f1".into(),
        recorded_at_ms: 1_700_000_000_000,
    };
    let r2 = IncrementalSchemaRecord {
        run_id: "run-2".into(),
        fingerprint: "f2".into(),
        recorded_at_ms: 1_700_000_100_000,
        ..r1.clone()
    };
    run_store.record_schema(&r1).unwrap();
    run_store.record_schema(&r2).unwrap();
    let latest = run_store
        .latest_schema(&pid, "sink", "dev")
        .unwrap()
        .unwrap();
    assert_eq!(latest.fingerprint, "f2");

    // reset
    assert!(run_store.reset_state(&pid, "sink", "dev").unwrap());
    assert!(!run_store.reset_state(&pid, "sink", "dev").unwrap());
    assert!(run_store.load_state(&pid, "sink", "dev").unwrap().is_none());

    // import is idempotent
    run_store.import_state(&s1).unwrap();
    run_store.import_state(&s1).unwrap();
    assert_eq!(run_store.list_states(Some("dev")).unwrap().len(), 1);
    run_store.import_schema_record(&r1).unwrap();
    run_store.import_schema_record(&r1).unwrap();

    // FK cascade: deleting the pipeline must remove its incremental state.
    pipeline_store.delete(&record.id).unwrap();
    assert!(run_store.list_states(None).unwrap().is_empty());

    teardown(&schema).await;
}
