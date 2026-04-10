// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the connectors API.

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
        connector_registry: Arc::new(flux_connectors::default_registry()),
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
        .nest("/api/connectors", flux_server::api::connectors::router())
        .with_state(state)
}

async fn body_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn list_connectors_returns_all_registered() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/connectors")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    let connectors = body["connectors"].as_array().unwrap();

    // The default registry has: csv, file, http, parquet, postgres, postgresql,
    // rest, rest_api, stdout — at minimum.
    assert!(connectors.len() >= 9);

    // Check that csv is both source and sink.
    let csv = connectors.iter().find(|c| c["name"] == "csv").unwrap();
    let roles = csv["roles"].as_array().unwrap();
    assert!(roles.contains(&json!("source")));
    assert!(roles.contains(&json!("sink")));

    // Check that stdout is only a sink.
    let stdout = connectors.iter().find(|c| c["name"] == "stdout").unwrap();
    let roles = stdout["roles"].as_array().unwrap();
    assert_eq!(roles, &[json!("sink")]);
}

#[tokio::test]
async fn list_connectors_empty_registry() {
    let pipelines_dir = tempfile::tempdir().unwrap().keep();
    let state = AppState {
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
    };
    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/connectors")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["connectors"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_connector_unknown_source() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/connectors/test")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "role": "source",
                        "connector": "nonexistent",
                        "config": {}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_connector_invalid_role() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/connectors/test")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "role": "invalid",
                        "connector": "csv",
                        "config": {}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_source_connector_with_bad_config() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/connectors/test")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "role": "source",
                        "connector": "csv",
                        "config": { "path": "/nonexistent/file.csv" }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["success"], false);
    assert!(!body["error"].as_str().unwrap().is_empty());
}
