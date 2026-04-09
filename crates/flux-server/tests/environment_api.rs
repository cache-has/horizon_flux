// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the environments API.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_connectors::ConnectorRegistry;
use flux_datafusion::{SqliteEnvironmentStore, SqliteRunStore};
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
        plugin_cwd: std::env::temp_dir(),
        plugin_scan_roots: Some(Vec::new()),
    }
}

fn test_router(state: AppState) -> Router {
    Router::new()
        .nest(
            "/api/environments",
            flux_server::api::environments::router(),
        )
        .with_state(state)
}

async fn body_json(resp: axum::http::Response<Body>) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_returns_default_environments() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/environments")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let envs = body.as_array().unwrap();

    // Default environments: dev and prod.
    assert!(envs.len() >= 2);
    let names: Vec<&str> = envs.iter().map(|e| e["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"prod"));
    assert!(names.contains(&"dev"));
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_environment() {
    let state = test_state();
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/environments")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name": "staging", "fallback": "prod"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = body_json(resp).await;
    assert_eq!(body["name"], "staging");
    assert_eq!(body["fallback"], "prod");
}

#[tokio::test]
async fn create_duplicate_returns_conflict() {
    let state = test_state();
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/environments")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name": "dev", "fallback": "prod"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_environment() {
    let state = test_state();
    // Create staging first.
    state
        .environment_store
        .create("staging", Some("prod"))
        .unwrap();

    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/environments/staging")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn delete_prod_returns_bad_request() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/environments/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Table overrides
// ---------------------------------------------------------------------------

#[tokio::test]
async fn table_override_lifecycle() {
    let state = test_state();
    let app = test_router(state.clone());

    // Create a table override.
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/environments/dev/tables/orders/override")
                .header("content-type", "application/json")
                .body(Body::from(json!({"schema_name": "public"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // List table overrides.
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/environments/dev/tables")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let overrides = body.as_array().unwrap();
    assert_eq!(overrides.len(), 1);
    assert_eq!(overrides[0]["table_name"], "orders");

    // Delete the override.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/environments/dev/tables/orders/override")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

// ---------------------------------------------------------------------------
// Resolve
// ---------------------------------------------------------------------------

#[tokio::test]
async fn resolve_table_chain() {
    let state = test_state();

    // Register an override in prod.
    state
        .environment_store
        .register_table_override("prod", "public", "users")
        .unwrap();

    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/environments/resolve/users?environment=dev")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["table"], "users");

    let chain = body["chain"].as_array().unwrap();
    // dev → prod chain; dev has no override, prod does.
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[0]["environment"], "dev");
    assert_eq!(chain[0]["has_override"], false);
    assert_eq!(chain[1]["environment"], "prod");
    assert_eq!(chain[1]["has_override"], true);
}
