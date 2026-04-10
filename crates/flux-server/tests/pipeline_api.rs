// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the pipeline management API.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_connectors::ConnectorRegistry;
use flux_datafusion::{SqliteBackfillStore, SqliteEnvironmentStore, SqliteRunStore};
use flux_engine::SqlitePipelineStore;
use flux_scheduler;
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
async fn list_udfs_returns_loaded_definitions() {
    let state = test_state();
    let app = test_router(state.clone());

    // Stage a UDF on disk.
    let udfs_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        udfs_dir.path().join("normalize_name.sql"),
        r#"CREATE OR REPLACE FUNCTION normalize_name(s VARCHAR) RETURNS VARCHAR
AS $$ LOWER(TRIM(s)) $$ LANGUAGE SQL IMMUTABLE;"#,
    )
    .unwrap();

    // Create a pipeline pointing at it.
    let pipeline = json!({
        "name": "udf-pipe",
        "udfs_dir": udfs_dir.path().to_str().unwrap(),
        "nodes": [],
        "edges": []
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(pipeline.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let id = body_json(resp.into_body()).await["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Hit the UDFs endpoint.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/pipelines/{id}/udfs"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    let udfs = body["udfs"].as_array().unwrap();
    assert_eq!(udfs.len(), 1);
    assert_eq!(udfs[0]["name"], "normalize_name");
    assert_eq!(udfs[0]["params"][0]["name"], "s");
    assert!(udfs[0]["signature"].as_str().unwrap().contains("->"));
}

#[tokio::test]
async fn list_udfs_empty_when_no_udfs_dir() {
    let state = test_state();
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(test_pipeline_json("no-udfs").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let id = body_json(resp.into_body()).await["id"]
        .as_str()
        .unwrap()
        .to_string();

    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/pipelines/{id}/udfs"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["udfs"].as_array().unwrap().len(), 0);
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

// ---------------------------------------------------------------------------
// Import / Export tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn export_pipeline() {
    let state = test_state();
    let app = test_router(state.clone());

    // Create a pipeline first.
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(test_pipeline_json("export-me").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let created = body_json(resp.into_body()).await;
    let id = created["id"].as_str().unwrap();

    // Export it.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/pipelines/{id}/export"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(ct.contains("application/json"));
    let cd = resp
        .headers()
        .get("content-disposition")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(cd.contains("export-me.json"), "got: {cd}");

    // Verify the body is valid pipeline JSON.
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["name"], "export-me");
}

#[tokio::test]
async fn import_pipeline_new() {
    let state = test_state();
    let app = test_router(state);

    let req = json!({
        "pipeline": test_pipeline_json("imported"),
    });

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines/import")
                .header("content-type", "application/json")
                .body(Body::from(req.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["pipeline"]["name"], "imported");
}

#[tokio::test]
async fn import_pipeline_conflict_reject() {
    let state = test_state();

    // Create a pipeline.
    let app = test_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/pipelines")
            .header("content-type", "application/json")
            .body(Body::from(test_pipeline_json("conflict").to_string()))
            .unwrap(),
    )
    .await
    .unwrap();

    // Import with same name (default = reject).
    let app = test_router(state);
    let req = json!({ "pipeline": test_pipeline_json("conflict") });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines/import")
                .header("content-type", "application/json")
                .body(Body::from(req.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn import_pipeline_conflict_rename() {
    let state = test_state();

    // Create a pipeline.
    let app = test_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/pipelines")
            .header("content-type", "application/json")
            .body(Body::from(test_pipeline_json("shared").to_string()))
            .unwrap(),
    )
    .await
    .unwrap();

    // Import with same name and rename.
    let app = test_router(state);
    let req = json!({
        "pipeline": test_pipeline_json("shared"),
        "on_conflict": "rename",
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines/import")
                .header("content-type", "application/json")
                .body(Body::from(req.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["pipeline"]["name"], "shared (2)");
}

#[tokio::test]
async fn import_pipeline_conflict_overwrite() {
    let state = test_state();

    // Create a pipeline.
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(test_pipeline_json("overwrite-me").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let created = body_json(resp.into_body()).await;
    let id = created["id"].as_str().unwrap();

    // Import with overwrite — same ID should be preserved.
    let app = test_router(state.clone());
    let req = json!({
        "pipeline": test_pipeline_json("overwrite-me"),
        "on_conflict": "overwrite",
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines/import")
                .header("content-type", "application/json")
                .body(Body::from(req.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["id"], id, "overwrite should preserve the same ID");
}

#[tokio::test]
async fn import_invalid_pipeline_returns_400() {
    let app = test_router(test_state());
    let req = json!({
        "pipeline": { "name": "", "nodes": [], "edges": [] },
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines/import")
                .header("content-type", "application/json")
                .body(Body::from(req.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bulk_export_pipelines() {
    let state = test_state();

    // Create two pipelines.
    for name in ["alpha", "beta"] {
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

    // Bulk export.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines/export")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert!(body["alpha"].is_object());
    assert!(body["beta"].is_object());
    assert_eq!(body["alpha"]["name"], "alpha");
    assert_eq!(body["beta"]["name"], "beta");
}

#[tokio::test]
async fn incremental_state_endpoints_roundtrip() {
    use flux_datafusion::IncrementalState;

    let state = test_state();

    // Create a pipeline so the path validation passes.
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(test_pipeline_json("inc-pipe").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created = body_json(resp.into_body()).await;
    let pid = created["id"].as_str().unwrap().to_string();

    // Seed an incremental state row directly via the storage trait.
    let row = IncrementalState {
        pipeline_id: pid.clone(),
        node_id: "sink_a".to_string(),
        environment: "default".to_string(),
        watermark_column: "updated_at".to_string(),
        watermark_value: "2026-04-08T00:00:00Z".to_string(),
        watermark_type: "timestamp".to_string(),
        last_run_at_ms: 1_700_000_000_000,
        last_run_id: uuid::Uuid::new_v4().to_string(),
        rows_processed: 42,
        schema_fingerprint: None,
    };
    state.incremental_state_store.save_state(&row).unwrap();

    // GET list
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/pipelines/{pid}/incremental-state"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    let states = body["states"].as_array().unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0]["node_id"], "sink_a");
    assert_eq!(states[0]["rows_processed"], 42);

    // POST reset
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/pipelines/{pid}/nodes/sink_a/incremental/reset"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["reset"], true);

    // Re-list — should be empty.
    let app = test_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/pipelines/{pid}/incremental-state"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["states"].as_array().unwrap().len(), 0);

    // Reset again — should 404.
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/pipelines/{pid}/nodes/sink_a/incremental/reset"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
