// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the pipeline management API.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_connectors::ConnectorRegistry;
use flux_datafusion::{EnvironmentStore, RunStore};
use flux_engine::PipelineStore;
use flux_server::AppState;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> AppState {
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
        .nest("/api/pipelines", flux_server::api::pipelines::router())
        .with_state(state)
}

fn test_pipeline_json(name: &str) -> Value {
    json!({
        "name": name,
        "nodes": [],
        "edges": []
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
                .uri("/api/pipelines")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
    assert_eq!(body["total"], 0);
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
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(test_pipeline_json("my-pipe").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let created = body_json(resp.into_body()).await;
    assert_eq!(created["pipeline"]["name"], "my-pipe");
    let id = created["id"].as_str().unwrap();

    // Get by ID
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/pipelines/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let fetched = body_json(resp.into_body()).await;
    assert_eq!(fetched["pipeline"]["name"], "my-pipe");
}

#[tokio::test]
async fn create_duplicate_name_returns_conflict() {
    let state = test_state();
    let app = test_router(state.clone());

    let body = test_pipeline_json("dup").to_string();

    // First create succeeds
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Second create with same name fails
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn update_pipeline() {
    let state = test_state();
    let app = test_router(state.clone());

    // Create
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(test_pipeline_json("original").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let created = body_json(resp.into_body()).await;
    let id = created["id"].as_str().unwrap();

    // Update
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/pipelines/{id}"))
                .header("content-type", "application/json")
                .body(Body::from(test_pipeline_json("renamed").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let updated = body_json(resp.into_body()).await;
    assert_eq!(updated["pipeline"]["name"], "renamed");
}

#[tokio::test]
async fn delete_pipeline() {
    let state = test_state();
    let app = test_router(state.clone());

    // Create
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(test_pipeline_json("doomed").to_string()))
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
                .uri(format!("/api/pipelines/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify gone
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/pipelines/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_nonexistent_returns_404() {
    let app = test_router(test_state());
    let fake_id = "00000000-0000-0000-0000-000000000000";
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/pipelines/{fake_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_json(resp.into_body()).await;
    assert!(body["error"].as_str().unwrap().contains("not found"));
}

#[tokio::test]
async fn invalid_id_returns_400() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/pipelines/not-a-uuid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn empty_name_returns_400() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(test_pipeline_json("  ").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn pagination() {
    let state = test_state();

    // Create 3 pipelines
    for name in ["alpha", "beta", "gamma"] {
        let app = test_router(state.clone());
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(test_pipeline_json(name).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    }

    // Get first page
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/pipelines?limit=2&offset=0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    assert_eq!(body["total"], 3);
    assert_eq!(body["limit"], 2);

    // Get second page
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/pipelines?limit=2&offset=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
}
