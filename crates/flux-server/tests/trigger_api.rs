// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the trigger management API.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_connectors::ConnectorRegistry;
use flux_datafusion::{SqliteBackfillStore, SqliteEnvironmentStore, SqliteRunStore};
use flux_engine::SqlitePipelineStore;
use flux_server::AppState;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> AppState {
    let pipelines_dir = tempfile::tempdir().unwrap().keep();
    AppState {
        pipeline_store: Arc::new(SqlitePipelineStore::open_in_memory(&pipelines_dir).unwrap()),
        run_store: Arc::new(SqliteRunStore::open_in_memory().unwrap()),
        incremental_state_store: Arc::new(SqliteRunStore::open_in_memory().unwrap()),
        lineage_store: Arc::new(SqliteRunStore::open_in_memory().unwrap()),
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
    }
}

fn test_router(state: AppState) -> Router {
    Router::new()
        .nest("/api/triggers", flux_server::api::triggers::router())
        .with_state(state)
}

fn cron_trigger_json(name: &str) -> Value {
    json!({
        "name": name,
        "pipeline_id": "pipe-1",
        "environment": "prod",
        "kind": {
            "kind": "cron",
            "expression": "0 */6 * * *",
            "timezone": "UTC"
        }
    })
}

async fn body_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn list_empty() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/triggers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn create_and_get() {
    let state = test_state();
    let app = test_router(state.clone());

    // Create
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/triggers")
                .header("content-type", "application/json")
                .body(Body::from(cron_trigger_json("my-cron").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let created = body_json(resp.into_body()).await;
    assert_eq!(created["name"], "my-cron");
    assert_eq!(created["pipeline_id"], "pipe-1");
    assert!(created["state"]["next_fire_at"].is_string());
    let id = created["id"].as_str().unwrap();

    // Get by ID
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/triggers/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let fetched = body_json(resp.into_body()).await;
    assert_eq!(fetched["id"], id);
    assert_eq!(fetched["name"], "my-cron");
}

#[tokio::test]
async fn update_trigger() {
    let state = test_state();
    let app = test_router(state.clone());

    // Create
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/triggers")
                .header("content-type", "application/json")
                .body(Body::from(cron_trigger_json("original").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let created = body_json(resp.into_body()).await;
    let id = created["id"].as_str().unwrap();

    // Update name
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/triggers/{id}"))
                .header("content-type", "application/json")
                .body(Body::from(json!({"name": "renamed"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let updated = body_json(resp.into_body()).await;
    assert_eq!(updated["name"], "renamed");
    assert_eq!(updated["pipeline_id"], "pipe-1"); // unchanged
}

#[tokio::test]
async fn delete_trigger() {
    let state = test_state();
    let app = test_router(state.clone());

    // Create
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/triggers")
                .header("content-type", "application/json")
                .body(Body::from(cron_trigger_json("to-delete").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let created = body_json(resp.into_body()).await;
    let id = created["id"].as_str().unwrap();

    // Delete
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/triggers/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Get returns 404
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/triggers/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn enable_disable() {
    let state = test_state();
    let app = test_router(state.clone());

    // Create (enabled by default)
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/triggers")
                .header("content-type", "application/json")
                .body(Body::from(cron_trigger_json("toggle-me").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let created = body_json(resp.into_body()).await;
    let id = created["id"].as_str().unwrap();
    assert_eq!(created["enabled"], true);

    // Disable
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/triggers/{id}/disable"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["enabled"], false);

    // Re-enable
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/triggers/{id}/enable"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["enabled"], true);
}

#[tokio::test]
async fn list_with_filters() {
    let state = test_state();

    // Create two triggers for different pipelines
    let app = test_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/triggers")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "name": "t1",
                    "pipeline_id": "pipe-a",
                    "kind": {"kind": "cron", "expression": "0 * * * *"}
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await
    .unwrap();

    let app = test_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/triggers")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "name": "t2",
                    "pipeline_id": "pipe-b",
                    "kind": {"kind": "interval", "every": "PT1H"}
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await
    .unwrap();

    // List all
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/triggers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 2);

    // Filter by pipeline_id
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/triggers?pipeline_id=pipe-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_json(resp.into_body()).await;
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["name"], "t1");
}

#[tokio::test]
async fn history_empty() {
    let state = test_state();
    let app = test_router(state.clone());

    // Create
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/triggers")
                .header("content-type", "application/json")
                .body(Body::from(cron_trigger_json("hist-test").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let created = body_json(resp.into_body()).await;
    let id = created["id"].as_str().unwrap();

    // History is empty
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/triggers/{id}/history"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn fire_without_scheduler_returns_error() {
    let state = test_state(); // scheduler: None
    let app = test_router(state.clone());

    // Create
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/triggers")
                .header("content-type", "application/json")
                .body(Body::from(cron_trigger_json("fire-test").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let created = body_json(resp.into_body()).await;
    let id = created["id"].as_str().unwrap();

    // Fire — scheduler not available
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/triggers/{id}/fire"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = body_json(resp.into_body()).await;
    assert!(body["error"].as_str().unwrap().contains("scheduler"));
}

#[tokio::test]
async fn get_nonexistent_returns_404() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/triggers/00000000-0000-0000-0000-000000000000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn invalid_id_returns_400() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/triggers/not-a-uuid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
