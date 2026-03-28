// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the single-node preview API.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_datafusion::RunStore;
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
        connector_registry: Arc::new(flux_connectors::default_registry()),
    }
}

fn test_router(state: AppState) -> Router {
    Router::new()
        .nest("/api/preview", flux_server::api::preview::router())
        .with_state(state)
}

async fn body_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preview_source_node_with_csv() {
    // Create a temp CSV file for the source to read.
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,alice\n2,bob\n").unwrap();

    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/preview/node")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "node": {
                            "type": "source",
                            "connector": "csv",
                            "config": {
                                "path": csv_path.to_str().unwrap(),
                                "format": "csv"
                            }
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["row_count"], 2);
    assert_eq!(body["columns"].as_array().unwrap().len(), 2);
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn preview_transform_node_with_sql() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/preview/node")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "node": {
                            "type": "transform",
                            "mode": "sql",
                            "code": "SELECT id, name FROM input WHERE id > 1"
                        },
                        "upstream": {
                            "input": [
                                {"id": 1, "name": "alice"},
                                {"id": 2, "name": "bob"},
                                {"id": 3, "name": "charlie"}
                            ]
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["row_count"], 2);
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn preview_transform_without_upstream_returns_400() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/preview/node")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "node": {
                            "type": "transform",
                            "mode": "sql",
                            "code": "SELECT 1"
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preview_source_with_sampling() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("big.csv");
    let mut content = "id,val\n".to_string();
    for i in 1..=100 {
        content.push_str(&format!("{i},x\n"));
    }
    std::fs::write(&csv_path, content).unwrap();

    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/preview/node")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "node": {
                            "type": "source",
                            "connector": "csv",
                            "config": { "path": csv_path.to_str().unwrap(), "format": "csv" }
                        },
                        "sample": { "mode": "first_n", "count": 10 }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["row_count"], 10);
}
