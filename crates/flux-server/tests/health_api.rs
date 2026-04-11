// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the cross-pipeline health dashboard API
//! (planning doc 37, sub-feature 4).

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_connectors::ConnectorRegistry;
use flux_datafusion::run::RunStatus;
use flux_datafusion::{SqliteBackfillStore, SqliteEnvironmentStore, SqliteRunStore};
use flux_engine::SqlitePipelineStore;
use flux_server::AppState;
use http_body_util::BodyExt;
use serde_json::Value;
use std::sync::Arc;
use std::time::SystemTime;
use tower::ServiceExt;

fn test_state() -> AppState {
    let pipelines_dir = tempfile::tempdir().unwrap().keep();
    let run_store = Arc::new(SqliteRunStore::open_in_memory().unwrap());
    AppState {
        pipeline_store: Arc::new(SqlitePipelineStore::open_in_memory(&pipelines_dir).unwrap()),
        run_store: run_store.clone(),
        incremental_state_store: run_store.clone(),
        lineage_store: run_store.clone(),
        connector_registry: Arc::new(ConnectorRegistry::new()),
        environment_store: Arc::new(SqliteEnvironmentStore::open_in_memory().unwrap()),
        secret_session: Arc::new(std::sync::Mutex::new(
            flux_server::state::SecretSession::new(std::env::temp_dir().join("unused-secrets.db")),
        )),
        event_tx: AppState::new_event_channel(),
        plugin_event_tx: AppState::new_plugin_event_channel(),
        output_cache: Arc::new(flux_datafusion::OutputCache::new(std::env::temp_dir())),
        session_factory: None,
        metadata_info: flux_server::state::MetadataInfo {
            backend: "sqlite".to_string(),
            data_dir: std::env::temp_dir(),
            connection_string: None,
            config_source: "default".to_string(),
        },
        plugin_registry: Arc::new(std::sync::RwLock::new(Arc::new(
            flux_plugin_host::PluginRegistry::default(),
        ))),
        backfill_store: Arc::new(SqliteBackfillStore::open_in_memory().unwrap()),
        trigger_store: Arc::new(flux_scheduler::SqliteTriggerStore::open_in_memory().unwrap()),
        scheduler: None,
        plugin_cwd: std::env::temp_dir(),
        plugin_scan_roots: Some(Vec::new()),
        metadata_dir: None,
        catalog_event_tx: AppState::new_catalog_event_channel(),
        column_lineage_store: None,
        column_lineage_event_tx: AppState::new_column_lineage_event_channel(),
        openlineage_client: None,
        sla_store: Some(run_store),
    }
}

fn test_router(state: AppState) -> Router {
    Router::new()
        .nest("/api/health", flux_server::api::health::router())
        .with_state(state)
}

async fn body_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn overview_empty() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::get("/api/health/overview")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;

    assert_eq!(body["window"], "24h");
    assert_eq!(body["run_summary"]["total"], 0);
    assert_eq!(body["trigger_health"]["total"], 0);
    assert_eq!(body["sla_summary"]["total"], 0);
    assert!(body["notable_events"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn overview_with_runs() {
    let state = test_state();
    let run_store = &state.run_store;

    // Create some runs with different statuses.
    let now = SystemTime::now();

    let run1 = run_store.create_run("etl_orders", "prod").unwrap();
    run_store
        .set_running(&run1.id, now, Some("cron:1h"))
        .unwrap();
    run_store
        .finish_run(&run1.id, RunStatus::Success, now, None)
        .unwrap();

    let run2 = run_store.create_run("etl_orders", "prod").unwrap();
    run_store
        .set_running(&run2.id, now, Some("cron:1h"))
        .unwrap();
    run_store
        .finish_run(
            &run2.id,
            RunStatus::Failed,
            now,
            Some("connection refused"),
        )
        .unwrap();

    let run3 = run_store.create_run("etl_users", "staging").unwrap();
    run_store.set_running(&run3.id, now, None).unwrap();
    run_store
        .finish_run(&run3.id, RunStatus::Success, now, None)
        .unwrap();

    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::get("/api/health/overview?window=24h")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;

    assert_eq!(body["run_summary"]["total"], 3);
    assert_eq!(body["run_summary"]["success"], 2);
    assert_eq!(body["run_summary"]["failed"], 1);

    // Check environment breakdown.
    assert_eq!(body["run_summary"]["by_environment"]["prod"]["total"], 2);
    assert_eq!(
        body["run_summary"]["by_environment"]["staging"]["total"],
        1
    );

    // Top failing pipeline should be etl_orders.
    let top_failing = body["top_failing_pipelines"].as_array().unwrap();
    assert_eq!(top_failing.len(), 1);
    assert_eq!(top_failing[0]["pipeline_name"], "etl_orders");
    assert_eq!(top_failing[0]["failure_count"], 1);
}

#[tokio::test]
async fn overview_invalid_window() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::get("/api/health/overview?window=1y")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn overview_notable_events_first_failure() {
    let state = test_state();
    let run_store = &state.run_store;
    let now = SystemTime::now();

    // First a success, then a failure — should produce a "first_failure" notable event.
    let earlier = now - std::time::Duration::from_secs(60);
    let run1 = run_store.create_run("healthy_pipe", "prod").unwrap();
    run_store.set_running(&run1.id, earlier, None).unwrap();
    run_store
        .finish_run(&run1.id, RunStatus::Success, earlier, None)
        .unwrap();

    let run2 = run_store.create_run("healthy_pipe", "prod").unwrap();
    run_store.set_running(&run2.id, now, None).unwrap();
    run_store
        .finish_run(&run2.id, RunStatus::Failed, now, Some("disk full"))
        .unwrap();

    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::get("/api/health/overview")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;

    let notable = body["notable_events"].as_array().unwrap();
    assert!(
        notable
            .iter()
            .any(|e| e["kind"] == "first_failure" && e["pipeline_name"] == "healthy_pipe"),
        "expected a first_failure notable event for healthy_pipe, got: {notable:?}"
    );
}
