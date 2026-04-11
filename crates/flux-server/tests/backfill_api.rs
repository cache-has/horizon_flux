// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the backfill management API (planning doc 33).

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
        sla_store: None,
    }
}

fn test_router(state: AppState) -> Router {
    Router::new()
        .nest("/api/backfills", flux_server::api::backfills::router())
        .with_state(state)
}

async fn body_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

/// Helper: create a pipeline in the store so backfill endpoints can reference it.
fn create_test_pipeline(state: &AppState) -> String {
    let pipeline: flux_engine::Pipeline = serde_json::from_value(serde_json::json!({
        "name": "test-pipe",
        "nodes": [],
        "edges": []
    }))
    .unwrap();
    let record = state.pipeline_store.create(pipeline).unwrap();
    record.id.0.to_string()
}

#[tokio::test]
async fn list_empty() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/backfills")
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
async fn create_backfill_validates_pipeline() {
    let state = test_state();
    let app = test_router(state);

    // Create with non-existent pipeline
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/backfills")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "pipeline_id": "00000000-0000-0000-0000-000000000000",
                        "range_definition": {
                            "kind": "list",
                            "values": ["a", "b"],
                            "variable_mapping": {"v": "$iteration.value"}
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn create_backfill_validates_range() {
    let state = test_state();
    let pipeline_id = create_test_pipeline(&state);
    let app = test_router(state);

    // Invalid date range (start > end)
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/backfills")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "pipeline_id": pipeline_id,
                        "range_definition": {
                            "kind": "date_range",
                            "start": "2024-02-01",
                            "end": "2024-01-01",
                            "granularity": "day",
                            "variable_mapping": {"d": "$iteration.start"}
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp.into_body()).await;
    assert!(body["error"].as_str().unwrap().contains("invalid range"));
}

#[tokio::test]
async fn create_and_get_backfill() {
    let state = test_state();
    let pipeline_id = create_test_pipeline(&state);

    // Create
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/backfills")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "pipeline_id": pipeline_id,
                        "range_definition": {
                            "kind": "list",
                            "values": ["US", "EU"],
                            "variable_mapping": {"region": "$iteration.value"}
                        },
                        "concurrency": 2,
                        "fail_fast": true
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let created = body_json(resp.into_body()).await;
    assert_eq!(created["pipeline_id"], pipeline_id);
    assert_eq!(created["concurrency"], 2);
    assert_eq!(created["fail_fast"], true);
    assert_eq!(created["status"], "pending");
    let id = created["id"].as_str().unwrap();

    // Allow a moment for the background task to persist iterations.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Get by ID
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/backfills/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let detail = body_json(resp.into_body()).await;
    assert_eq!(detail["id"], id);
    assert!(detail["progress"].is_object());
    assert!(detail["iterations"].is_array());
}

#[tokio::test]
async fn delete_backfill() {
    let state = test_state();

    // Manually insert a completed backfill directly in the store.
    use flux_engine::backfill::*;
    let bf = Backfill {
        id: BackfillId("del-test".into()),
        pipeline_id: "pipe-1".into(),
        environment: "default".into(),
        range_definition: RangeDefinition::List {
            values: vec!["a".into()],
            variable_mapping: std::collections::HashMap::new(),
        },
        concurrency: 1,
        fail_fast: false,
        full_refresh: true,
        status: BackfillStatus::Completed,
        created_at: "2024-01-01T00:00:00Z".into(),
        started_at: None,
        completed_at: Some("2024-01-01T01:00:00Z".into()),
        created_by: None,
    };
    state.backfill_store.create_backfill(&bf).unwrap();

    // Delete
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/backfills/del-test")
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
                .uri("/api/backfills/del-test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_running_backfill_rejected() {
    let state = test_state();

    use flux_engine::backfill::*;
    let bf = Backfill {
        id: BackfillId("running-bf".into()),
        pipeline_id: "pipe-1".into(),
        environment: "default".into(),
        range_definition: RangeDefinition::List {
            values: vec!["a".into()],
            variable_mapping: std::collections::HashMap::new(),
        },
        concurrency: 1,
        fail_fast: false,
        full_refresh: true,
        status: BackfillStatus::Running,
        created_at: "2024-01-01T00:00:00Z".into(),
        started_at: Some("2024-01-01T00:00:01Z".into()),
        completed_at: None,
        created_by: None,
    };
    state.backfill_store.create_backfill(&bf).unwrap();

    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/backfills/running-bf")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn cancel_backfill_updates_status() {
    let state = test_state();

    use flux_engine::backfill::*;
    let bf = Backfill {
        id: BackfillId("cancel-test".into()),
        pipeline_id: "pipe-1".into(),
        environment: "default".into(),
        range_definition: RangeDefinition::List {
            values: vec!["a".into()],
            variable_mapping: std::collections::HashMap::new(),
        },
        concurrency: 1,
        fail_fast: false,
        full_refresh: true,
        status: BackfillStatus::Running,
        created_at: "2024-01-01T00:00:00Z".into(),
        started_at: Some("2024-01-01T00:00:01Z".into()),
        completed_at: None,
        created_by: None,
    };
    state.backfill_store.create_backfill(&bf).unwrap();

    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/backfills/cancel-test/cancel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["status"], "cancelled");
}

#[tokio::test]
async fn cancel_completed_backfill_rejected() {
    let state = test_state();

    use flux_engine::backfill::*;
    let bf = Backfill {
        id: BackfillId("done-bf".into()),
        pipeline_id: "pipe-1".into(),
        environment: "default".into(),
        range_definition: RangeDefinition::List {
            values: vec!["a".into()],
            variable_mapping: std::collections::HashMap::new(),
        },
        concurrency: 1,
        fail_fast: false,
        full_refresh: true,
        status: BackfillStatus::Completed,
        created_at: "2024-01-01T00:00:00Z".into(),
        started_at: None,
        completed_at: Some("2024-01-01T01:00:00Z".into()),
        created_by: None,
    };
    state.backfill_store.create_backfill(&bf).unwrap();

    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/backfills/done-bf/cancel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn get_nonexistent_returns_404() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/backfills/no-such-id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_with_status_filter() {
    let state = test_state();

    use flux_engine::backfill::*;
    for (id, status) in [
        ("bf-a", BackfillStatus::Completed),
        ("bf-b", BackfillStatus::Failed),
    ] {
        let bf = Backfill {
            id: BackfillId(id.into()),
            pipeline_id: "pipe-1".into(),
            environment: "default".into(),
            range_definition: RangeDefinition::List {
                values: vec!["x".into()],
                variable_mapping: std::collections::HashMap::new(),
            },
            concurrency: 1,
            fail_fast: false,
            full_refresh: true,
            status,
            created_at: "2024-01-01T00:00:00Z".into(),
            started_at: None,
            completed_at: None,
            created_by: None,
        };
        state.backfill_store.create_backfill(&bf).unwrap();
    }

    // Filter by status=failed
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/backfills?status=failed")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["id"], "bf-b");

    // Invalid status filter
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/backfills?status=bogus")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn create_validates_concurrency() {
    let state = test_state();
    let pipeline_id = create_test_pipeline(&state);
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/backfills")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "pipeline_id": pipeline_id,
                        "range_definition": {
                            "kind": "list",
                            "values": ["a"],
                            "variable_mapping": {"v": "$iteration.value"}
                        },
                        "concurrency": 0
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp.into_body()).await;
    assert!(body["error"].as_str().unwrap().contains("concurrency"));
}
