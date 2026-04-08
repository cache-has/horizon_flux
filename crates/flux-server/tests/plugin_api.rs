// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the plugins API.

use std::fs;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_datafusion::{SqliteEnvironmentStore, SqliteRunStore};
use flux_engine::SqlitePipelineStore;
use flux_plugin_host::{PluginRegistry, discover_plugins_in};
use flux_server::AppState;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

fn write_plugin(root: &std::path::Path, name: &str, sink_type: &str) {
    let dir = root.join(name);
    fs::create_dir_all(&dir).unwrap();
    let manifest = format!(
        r#"
name = "{name}"
version = "0.1.0"
flux_plugin_protocol = 1
flux_min_version = "0.1.0"
executable = "{name}-plugin"

[[sinks]]
type = "{sink_type}"
display_name = "Test Sink"
config_schema = "schema.json"
"#
    );
    fs::write(dir.join("plugin.toml"), manifest).unwrap();
    let schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": { "path": { "type": "string" } },
        "required": ["path"]
    });
    fs::write(
        dir.join("schema.json"),
        serde_json::to_string(&schema).unwrap(),
    )
    .unwrap();
}

fn state_with_registry_and_roots(
    cwd: std::path::PathBuf,
    registry: PluginRegistry,
    scan_roots: Vec<std::path::PathBuf>,
) -> AppState {
    let pipelines_dir = tempfile::tempdir().unwrap().keep();
    AppState {
        pipeline_store: Arc::new(SqlitePipelineStore::open_in_memory(&pipelines_dir).unwrap()),
        run_store: Arc::new(SqliteRunStore::open_in_memory().unwrap()),
        connector_registry: Arc::new(flux_connectors::ConnectorRegistry::new()),
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
        plugin_registry: Arc::new(std::sync::RwLock::new(Arc::new(registry))),
        plugin_cwd: cwd,
        plugin_scan_roots: Some(scan_roots),
    }
}

fn state_with_registry(cwd: std::path::PathBuf, registry: PluginRegistry) -> AppState {
    state_with_registry_and_roots(cwd, registry, Vec::new())
}

fn router(state: AppState) -> Router {
    Router::new()
        .nest("/api/plugins", flux_server::api::plugins::router())
        .with_state(state)
}

async fn body_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn list_plugins_returns_discovered() {
    let cwd = tempfile::tempdir().unwrap();
    let plugins_dir = cwd.path().join("plugins");
    fs::create_dir_all(&plugins_dir).unwrap();
    write_plugin(&plugins_dir, "alpha", "alpha_sink");
    let registry = discover_plugins_in(std::slice::from_ref(&plugins_dir));

    let state = state_with_registry(cwd.path().to_path_buf(), registry);
    let app = router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/plugins")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    let plugins = body["plugins"].as_array().unwrap();
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0]["name"], "alpha");
    assert_eq!(plugins[0]["status"]["status"], "ok");
}

#[tokio::test]
async fn get_sink_schema_returns_json_schema() {
    let cwd = tempfile::tempdir().unwrap();
    let plugins_dir = cwd.path().join("plugins");
    fs::create_dir_all(&plugins_dir).unwrap();
    write_plugin(&plugins_dir, "alpha", "alpha_sink");
    let registry = discover_plugins_in(std::slice::from_ref(&plugins_dir));

    let state = state_with_registry(cwd.path().to_path_buf(), registry);
    let app = router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/plugins/alpha/sinks/alpha_sink/schema")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["type"], "object");
    assert_eq!(body["required"][0], "path");
}

#[tokio::test]
async fn get_sink_schema_unknown_plugin_404() {
    let cwd = tempfile::tempdir().unwrap();
    let state = state_with_registry(cwd.path().to_path_buf(), PluginRegistry::default());
    let app = router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/plugins/missing/sinks/x/schema")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn reload_picks_up_new_plugin() {
    let cwd = tempfile::tempdir().unwrap();
    let plugins_dir = cwd.path().join("plugins");
    fs::create_dir_all(&plugins_dir).unwrap();
    // Start with empty registry; cwd has no plugins yet. Pin the scan
    // roots to the temp `plugins_dir` so the developer machine's installed
    // plugins do not leak into the test.
    let state = state_with_registry_and_roots(
        cwd.path().to_path_buf(),
        PluginRegistry::default(),
        vec![plugins_dir.clone()],
    );
    let app = router(state.clone());

    // Now add a plugin and reload.
    write_plugin(&plugins_dir, "beta", "beta_sink");
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/plugins/reload")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["count"], 1);

    // Confirm via list endpoint that the registry was swapped.
    let app2 = router(state);
    let resp2 = app2
        .oneshot(
            Request::builder()
                .uri("/api/plugins")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body2 = body_json(resp2.into_body()).await;
    assert_eq!(body2["plugins"][0]["name"], "beta");
}

#[tokio::test]
async fn reload_broadcasts_plugin_event() {
    let cwd = tempfile::tempdir().unwrap();
    let plugins_dir = cwd.path().join("plugins");
    fs::create_dir_all(&plugins_dir).unwrap();
    let state = state_with_registry_and_roots(
        cwd.path().to_path_buf(),
        PluginRegistry::default(),
        vec![plugins_dir.clone()],
    );
    let mut rx = state.plugin_event_tx.subscribe();
    let app = router(state);

    write_plugin(&plugins_dir, "gamma", "gamma_sink");
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/plugins/reload")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let event = rx
        .try_recv()
        .expect("plugin event should have been broadcast");
    let json = serde_json::to_value(&event).unwrap();
    assert_eq!(json["type"], "plugin_registry_reloaded");
    // The test plugin we just wrote should be present; other plugins may be
    // discovered from system dirs depending on the developer's environment,
    // so just assert "at least one" rather than exact counts.
    assert!(json["count"].as_u64().unwrap() >= 1);
    assert!(json["ok_count"].as_u64().unwrap() >= 1);
}
