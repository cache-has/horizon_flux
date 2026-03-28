// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the secrets API.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_connectors::ConnectorRegistry;
use flux_datafusion::{EnvironmentStore, RunStore};
use flux_engine::PipelineStore;
use flux_secrets::SecretStore;
use flux_server::AppState;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

fn test_state_with_secrets() -> AppState {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    // Remove the file so SecretStore::init can create it fresh.
    drop(tmp);
    let store = SecretStore::init(&path, b"test-password").unwrap();
    AppState {
        pipeline_store: Arc::new(PipelineStore::open_in_memory().unwrap()),
        run_store: Arc::new(RunStore::open_in_memory().unwrap()),
        connector_registry: Arc::new(ConnectorRegistry::new()),
        environment_store: Arc::new(EnvironmentStore::open_in_memory().unwrap()),
        secret_store: Some(Arc::new(Mutex::new(store))),
        event_tx: AppState::new_event_channel(),
    }
}

fn test_state_without_secrets() -> AppState {
    AppState {
        pipeline_store: Arc::new(PipelineStore::open_in_memory().unwrap()),
        run_store: Arc::new(RunStore::open_in_memory().unwrap()),
        connector_registry: Arc::new(ConnectorRegistry::new()),
        environment_store: Arc::new(EnvironmentStore::open_in_memory().unwrap()),
        secret_store: None,
        event_tx: AppState::new_event_channel(),
    }
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
// Store unavailable
// ---------------------------------------------------------------------------

#[tokio::test]
async fn secrets_unavailable_returns_503() {
    let app = test_router(test_state_without_secrets());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/secrets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// ---------------------------------------------------------------------------
// CRUD operations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_and_list_secrets() {
    let state = test_state_with_secrets();
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
    let state = test_state_with_secrets();
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
    let state = test_state_with_secrets();

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
    let state = test_state_with_secrets();
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
    let state = test_state_with_secrets();
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
    let state = test_state_with_secrets();
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
