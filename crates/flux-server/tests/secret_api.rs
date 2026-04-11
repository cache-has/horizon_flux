// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the secrets API.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_connectors::ConnectorRegistry;
use flux_datafusion::{SqliteBackfillStore, SqliteEnvironmentStore, SqliteRunStore};
use flux_engine::SqlitePipelineStore;
use flux_scheduler;
use flux_secrets::SecretStore;
use flux_server::AppState;
use flux_server::state::SecretSession;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

/// Create state with an initialized + unlocked secret store.
fn test_state_unlocked() -> (AppState, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let secrets_path = tmp.path().join("secrets.db");
    let store = SecretStore::init(&secrets_path, b"test-password").unwrap();
    let pipelines_dir = tempfile::tempdir().unwrap().keep();
    let state = AppState {
        pipeline_store: Arc::new(SqlitePipelineStore::open_in_memory(&pipelines_dir).unwrap()),
        run_store: Arc::new(SqliteRunStore::open_in_memory().unwrap()),
        incremental_state_store: Arc::new(SqliteRunStore::open_in_memory().unwrap()),
        lineage_store: Arc::new(SqliteRunStore::open_in_memory().unwrap()),
        connector_registry: Arc::new(ConnectorRegistry::new()),
        environment_store: Arc::new(SqliteEnvironmentStore::open_in_memory().unwrap()),
        secret_session: Arc::new(Mutex::new(SecretSession::new_unlocked(store, secrets_path))),
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
        sla_store: None,
    };
    (state, tmp)
}

/// Create state with no secret store initialized (locked, no db file).
fn test_state_locked() -> (AppState, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let secrets_path = tmp.path().join("secrets.db");
    let pipelines_dir = tempfile::tempdir().unwrap().keep();
    let state = AppState {
        pipeline_store: Arc::new(SqlitePipelineStore::open_in_memory(&pipelines_dir).unwrap()),
        run_store: Arc::new(SqliteRunStore::open_in_memory().unwrap()),
        incremental_state_store: Arc::new(SqliteRunStore::open_in_memory().unwrap()),
        lineage_store: Arc::new(SqliteRunStore::open_in_memory().unwrap()),
        connector_registry: Arc::new(ConnectorRegistry::new()),
        environment_store: Arc::new(SqliteEnvironmentStore::open_in_memory().unwrap()),
        secret_session: Arc::new(Mutex::new(SecretSession::new(secrets_path))),
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
        sla_store: None,
    };
    (state, tmp)
}

fn test_router(state: AppState) -> Router {
    Router::new()
        .nest("/api/secrets", flux_server::api::secrets::router())
        .with_state(state)
}

async fn body_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// ---------------------------------------------------------------------------
// Status endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_not_initialized() {
    let (state, _tmp) = test_state_locked();
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/secrets/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["initialized"], false);
    assert_eq!(body["unlocked"], false);
}

#[tokio::test]
async fn status_unlocked() {
    let (state, _tmp) = test_state_unlocked();
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/secrets/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["initialized"], true);
    assert_eq!(body["unlocked"], true);
}

// ---------------------------------------------------------------------------
// Init endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn init_creates_store_and_unlocks() {
    let (state, _tmp) = test_state_locked();
    let app = test_router(state.clone());

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets/init")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"password": "my-pass", "confirm": "my-pass"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Should now be initialized and unlocked.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/secrets/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["initialized"], true);
    assert_eq!(body["unlocked"], true);
}

#[tokio::test]
async fn init_password_mismatch_returns_400() {
    let (state, _tmp) = test_state_locked();
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets/init")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"password": "a", "confirm": "b"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn init_already_initialized_returns_409() {
    let (state, _tmp) = test_state_unlocked();
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets/init")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"password": "x", "confirm": "x"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

// ---------------------------------------------------------------------------
// Unlock / Lock
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unlock_and_lock_lifecycle() {
    let (state, _tmp) = test_state_unlocked();

    // Lock it.
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets/lock")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Confirm locked.
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/secrets/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["unlocked"], false);

    // CRUD should return 401.
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/secrets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Unlock with correct password.
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets/unlock")
                .header("content-type", "application/json")
                .body(Body::from(json!({"password": "test-password"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // CRUD should work again.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/secrets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn unlock_wrong_password_returns_401() {
    let (state, _tmp) = test_state_unlocked();

    // Lock first.
    let app = test_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/secrets/lock")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Wrong password.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets/unlock")
                .header("content-type", "application/json")
                .body(Body::from(json!({"password": "wrong"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn unlock_not_initialized_returns_503() {
    let (state, _tmp) = test_state_locked();
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets/unlock")
                .header("content-type", "application/json")
                .body(Body::from(json!({"password": "x"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// ---------------------------------------------------------------------------
// CRUD operations (with unlocked store)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_and_list_secrets() {
    let (state, _tmp) = test_state_unlocked();
    let app = test_router(state.clone());

    // Create a secret.
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name": "db_password", "value": "s3cret"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // List secrets.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/secrets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp.into_body()).await;
    let secrets = body.as_array().unwrap();
    assert_eq!(secrets.len(), 1);
    assert_eq!(secrets[0]["name"], "db_password");
    // Value should never be exposed.
    assert!(secrets[0].get("value").is_none());
}

#[tokio::test]
async fn create_secret_with_environment() {
    let (state, _tmp) = test_state_unlocked();
    let app = test_router(state.clone());

    // Create default secret.
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name": "api_key", "value": "default-key"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create environment-specific override.
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name": "api_key", "value": "staging-key", "environment": "staging"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Check environments endpoint.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/secrets/api_key/environments")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp.into_body()).await;
    assert_eq!(body["name"], "api_key");
    let envs = body["environments"].as_array().unwrap();
    assert_eq!(envs.len(), 2);
}

#[tokio::test]
async fn delete_secret() {
    let (state, _tmp) = test_state_unlocked();

    // Create.
    let app = test_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/secrets")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"name": "temp", "value": "val"}).to_string(),
            ))
            .unwrap(),
    )
    .await
    .unwrap();

    // Delete.
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/secrets/temp")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify gone.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/secrets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_json(resp.into_body()).await;
    assert_eq!(body.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn delete_nonexistent_secret_returns_404() {
    let (state, _tmp) = test_state_unlocked();
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/secrets/nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn environments_for_nonexistent_secret_returns_404() {
    let (state, _tmp) = test_state_unlocked();
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/secrets/nope/environments")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn create_secret_empty_name_returns_400() {
    let (state, _tmp) = test_state_unlocked();
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets")
                .header("content-type", "application/json")
                .body(Body::from(json!({"name": "", "value": "val"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unlock_rate_limit_after_5_attempts() {
    let (state, _tmp) = test_state_unlocked();

    // Lock first.
    {
        let mut session = state.secret_session.lock().unwrap();
        session.lock();
    }

    // Make 5 wrong attempts.
    for _ in 0..5 {
        let app = test_router(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/secrets/unlock")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"password": "wrong"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        // These should return 401 (wrong password).
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // 6th attempt should be rate-limited.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/secrets/unlock")
                .header("content-type", "application/json")
                .body(Body::from(json!({"password": "test-password"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
}
