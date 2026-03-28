// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the WebSocket endpoint and CORS configuration.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_connectors::ConnectorRegistry;
use flux_datafusion::{EnvironmentStore, ExecutionEvent, RunId, RunStore};
use flux_engine::PipelineStore;
use flux_server::AppState;
use serde_json::Value;
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

fn test_router(state: AppState) -> axum::Router {
    // Use the full build_router via the public API routes + ws.
    axum::Router::new()
        .nest("/api/pipelines", flux_server::api::pipelines::router())
        .route("/ws", axum::routing::get(flux_server::ws::ws_handler))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// CORS tests
// ---------------------------------------------------------------------------

/// Build the full router (with CORS layer) for CORS testing.
fn cors_router(state: AppState) -> axum::Router {
    use tower_http::cors::{AllowOrigin, CorsLayer};

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(
            |origin: &axum::http::HeaderValue, _| {
                origin
                    .to_str()
                    .map(|s| s.starts_with("http://localhost") || s.starts_with("http://127.0.0.1"))
                    .unwrap_or(false)
            },
        ))
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);

    axum::Router::new()
        .nest("/api/pipelines", flux_server::api::pipelines::router())
        .with_state(state)
        .layer(cors)
}

#[tokio::test]
async fn cors_allows_localhost_origin() {
    let app = cors_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/pipelines")
                .header("origin", "http://localhost:5173")
                .header("access-control-request-method", "GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let allow_origin = resp.headers().get("access-control-allow-origin").unwrap();
    assert_eq!(allow_origin, "http://localhost:5173");
}

#[tokio::test]
async fn cors_allows_127_0_0_1_origin() {
    let app = cors_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/pipelines")
                .header("origin", "http://127.0.0.1:8080")
                .header("access-control-request-method", "GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let allow_origin = resp.headers().get("access-control-allow-origin").unwrap();
    assert_eq!(allow_origin, "http://127.0.0.1:8080");
}

#[tokio::test]
async fn cors_rejects_external_origin() {
    let app = cors_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/pipelines")
                .header("origin", "https://evil.com")
                .header("access-control-request-method", "GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // With a rejected origin, tower-http still returns 200 for OPTIONS but
    // does NOT include an access-control-allow-origin header.
    assert!(resp.headers().get("access-control-allow-origin").is_none());
}

// ---------------------------------------------------------------------------
// Broadcast channel integration test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn broadcast_channel_receives_events() {
    let state = test_state();
    let mut rx = state.event_tx.subscribe();

    let run_id = RunId::new();
    let event = ExecutionEvent::RunStarted {
        run_id: run_id.clone(),
        pipeline_name: "test-pipe".to_string(),
    };

    // Send an event.
    state.event_tx.send(event).unwrap();

    // Receive it.
    let received = rx.recv().await.unwrap();
    match received {
        ExecutionEvent::RunStarted {
            pipeline_name,
            run_id: recv_id,
        } => {
            assert_eq!(pipeline_name, "test-pipe");
            assert_eq!(recv_id, run_id);
        }
        _ => panic!("unexpected event variant"),
    }
}

#[tokio::test]
async fn execution_event_serializes_with_type_tag() {
    let run_id = RunId::new();
    let event = ExecutionEvent::NodeCompleted {
        run_id,
        node_id: flux_engine::NodeId("xform_1".to_string()),
        rows_out: 42,
        duration_ms: 100,
    };

    let json: Value = serde_json::to_value(&event).unwrap();
    assert_eq!(json["type"], "node_completed");
    assert_eq!(json["rows_out"], 42);
    assert_eq!(json["duration_ms"], 100);
    assert_eq!(json["node_id"], "xform_1");
}

// ---------------------------------------------------------------------------
// WebSocket upgrade test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ws_upgrade_requires_websocket_headers() {
    let app = test_router(test_state());
    // A plain GET to /ws without WebSocket headers should fail.
    let resp = app
        .oneshot(Request::builder().uri("/ws").body(Body::empty()).unwrap())
        .await
        .unwrap();

    // Axum returns 400 or similar when upgrade headers are missing.
    assert_ne!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
}
