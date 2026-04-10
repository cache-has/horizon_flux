// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the resource catalog API (planning doc 34).

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_connectors::ConnectorRegistry;
use flux_datafusion::{
    NodeRunStats, RunStatus, SqliteBackfillStore, SqliteEnvironmentStore, SqliteRunStore,
    StoredResourceBinding,
};
use flux_engine::SqlitePipelineStore;
use flux_engine::lineage::{BindingDirection, ResourceFingerprint};
use flux_scheduler::SqliteTriggerStore;
use flux_server::AppState;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

/// Simple percent-encoding for query parameter values in test URLs.
fn encode_qp(s: &str) -> String {
    s.replace('%', "%25")
        .replace(':', "%3A")
        .replace('/', "%2F")
        .replace('@', "%40")
        .replace('&', "%26")
        .replace('=', "%3D")
}

fn test_state_with_metadata(metadata_dir: std::path::PathBuf) -> AppState {
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
        trigger_store: Arc::new(SqliteTriggerStore::open_in_memory().unwrap()),
        scheduler: None,
        plugin_cwd: std::env::temp_dir(),
        plugin_scan_roots: Some(Vec::new()),
        metadata_dir: Some(metadata_dir),
        catalog_event_tx: AppState::new_catalog_event_channel(),
        column_lineage_store: None,
        column_lineage_event_tx: AppState::new_column_lineage_event_channel(),
        openlineage_client: None,
    }
}

fn test_router(state: AppState) -> Router {
    Router::new()
        .nest("/api/catalog", flux_server::api::catalog::router())
        .with_state(state)
}

async fn body_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

/// Seed lineage bindings for two resources.
fn seed_resources(state: &AppState) {
    let id_a = "00000000-0000-0000-0000-000000000001";
    let id_b = "00000000-0000-0000-0000-000000000002";
    let env = "default";
    let now = 1000i64;

    state
        .lineage_store
        .save_bindings(
            id_a,
            env,
            &[StoredResourceBinding {
                pipeline_id: id_a.to_string(),
                node_id: "sink1".to_string(),
                direction: BindingDirection::Sink,
                resource_fingerprint: ResourceFingerprint::new(
                    "postgres://db:5432/analytics/public.orders",
                ),
                environment: env.to_string(),
                updated_at_ms: now,
            }],
        )
        .unwrap();

    state
        .lineage_store
        .save_bindings(
            id_b,
            env,
            &[
                StoredResourceBinding {
                    pipeline_id: id_b.to_string(),
                    node_id: "src1".to_string(),
                    direction: BindingDirection::Source,
                    resource_fingerprint: ResourceFingerprint::new(
                        "postgres://db:5432/analytics/public.orders",
                    ),
                    environment: env.to_string(),
                    updated_at_ms: now,
                },
                StoredResourceBinding {
                    pipeline_id: id_b.to_string(),
                    node_id: "src2".to_string(),
                    direction: BindingDirection::Source,
                    resource_fingerprint: ResourceFingerprint::new("file:///data/raw.csv"),
                    environment: env.to_string(),
                    updated_at_ms: now,
                },
            ],
        )
        .unwrap();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_empty() {
    let dir = tempfile::tempdir().unwrap();
    let state = test_state_with_metadata(dir.path().to_path_buf());
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/catalog/resources")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["total"], 0);
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn list_with_resources() {
    let dir = tempfile::tempdir().unwrap();
    let state = test_state_with_metadata(dir.path().to_path_buf());
    seed_resources(&state);
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/catalog/resources")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["total"], 2);
}

#[tokio::test]
async fn get_resource_detail() {
    let dir = tempfile::tempdir().unwrap();
    let state = test_state_with_metadata(dir.path().to_path_buf());
    seed_resources(&state);
    let app = test_router(state);

    let fp = "postgres://db:5432/analytics/public.orders";
    let uri = format!(
        "/api/catalog/resources/detail?fingerprint={}",
        encode_qp(fp)
    );
    let resp = app
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["fingerprint"], fp);
    assert_eq!(body["name"], "public.orders");
}

#[tokio::test]
async fn get_resource_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let state = test_state_with_metadata(dir.path().to_path_buf());
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/catalog/resources/detail?fingerprint=nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn search_resources() {
    let dir = tempfile::tempdir().unwrap();
    let state = test_state_with_metadata(dir.path().to_path_buf());
    seed_resources(&state);
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/catalog/resources?q=orders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["total"], 1);
    assert!(
        body["data"][0]["fingerprint"]
            .as_str()
            .unwrap()
            .contains("orders")
    );
}

#[tokio::test]
async fn tags_and_owners_empty() {
    let dir = tempfile::tempdir().unwrap();
    let state = test_state_with_metadata(dir.path().to_path_buf());
    let app = test_router(state.clone());

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/catalog/tags")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["tags"].as_array().unwrap().len(), 0);

    let app2 = test_router(state);
    let resp = app2
        .oneshot(
            Request::builder()
                .uri("/api/catalog/owners")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["owners"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn update_metadata_creates_annotation() {
    let dir = tempfile::tempdir().unwrap();
    let state = test_state_with_metadata(dir.path().to_path_buf());
    seed_resources(&state);

    // Subscribe to catalog events before making the request.
    let mut catalog_rx = state.catalog_event_tx.subscribe();

    let app = test_router(state.clone());

    let fp = "postgres://db:5432/analytics/public.orders";
    let uri = format!(
        "/api/catalog/resources/metadata?fingerprint={}",
        encode_qp(fp)
    );

    let body = json!({
        "name": "Customer Orders",
        "description": "All customer orders across channels",
        "owner": { "team": "commerce", "contact": "commerce@example.com" },
        "tags": ["commerce", "pii"]
    });

    let resp = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&uri)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let resp_body = body_json(resp.into_body()).await;
    assert_eq!(resp_body["name"], "Customer Orders");
    assert_eq!(resp_body["tags"][0], "commerce");

    // Verify the YAML file was created on disk.
    let expected_file = dir
        .path()
        .join("postgres/db__5432__analytics__public.orders.yaml");
    assert!(
        expected_file.exists(),
        "annotation file should exist on disk"
    );

    // Verify WebSocket event was sent.
    let event = catalog_rx.try_recv().unwrap();
    match event {
        flux_server::CatalogEvent::MetadataUpdated { fingerprint } => {
            assert_eq!(fingerprint, fp);
        }
    }

    // Verify tags endpoint now returns the tags.
    let app2 = test_router(state);
    let resp = app2
        .oneshot(
            Request::builder()
                .uri("/api/catalog/tags")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let tags_body = body_json(resp.into_body()).await;
    let tags: Vec<&str> = tags_body["tags"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(tags.contains(&"commerce"));
    assert!(tags.contains(&"pii"));
}

#[tokio::test]
async fn describe_scaffolds_all() {
    let dir = tempfile::tempdir().unwrap();
    let state = test_state_with_metadata(dir.path().to_path_buf());
    seed_resources(&state);
    let app = test_router(state);

    let body = json!({ "all": true });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/catalog/describe")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let resp_body = body_json(resp.into_body()).await;
    let created = resp_body["created"].as_array().unwrap();
    assert_eq!(created.len(), 2, "should scaffold 2 resource files");
}

#[tokio::test]
async fn filter_by_tag() {
    let dir = tempfile::tempdir().unwrap();
    let state = test_state_with_metadata(dir.path().to_path_buf());
    seed_resources(&state);

    // Write an annotation with a tag for one resource.
    let pg_dir = dir.path().join("postgres");
    std::fs::create_dir_all(&pg_dir).unwrap();
    std::fs::write(
        pg_dir.join("db__5432__analytics__public.orders.yaml"),
        r#"resource:
  fingerprint: "postgres://db:5432/analytics/public.orders"
name: Orders
tags: [commerce]
"#,
    )
    .unwrap();

    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/catalog/resources?tag=commerce")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["total"], 1);
    assert!(
        body["data"][0]["fingerprint"]
            .as_str()
            .unwrap()
            .contains("orders")
    );
}

#[tokio::test]
async fn auto_derived_freshness_from_run_history() {
    use std::time::{Duration, UNIX_EPOCH};

    let dir = tempfile::tempdir().unwrap();
    let state = test_state_with_metadata(dir.path().to_path_buf());

    // Create a pipeline in the store so the enrichment can map ID → name.
    let pipeline: flux_engine::Pipeline = serde_json::from_value(json!({
        "name": "etl-orders",
        "nodes": [],
        "edges": []
    }))
    .unwrap();
    let record = state.pipeline_store.create(pipeline).unwrap();
    let pid = record.id.to_string();

    // Seed a lineage binding: this pipeline's sink1 produces the orders resource.
    state
        .lineage_store
        .save_bindings(
            &pid,
            "default",
            &[StoredResourceBinding {
                pipeline_id: pid.clone(),
                node_id: "sink1".to_string(),
                direction: BindingDirection::Sink,
                resource_fingerprint: ResourceFingerprint::new(
                    "postgres://db:5432/analytics/public.orders",
                ),
                environment: "default".to_string(),
                updated_at_ms: 1000,
            }],
        )
        .unwrap();

    // Create a successful run for this pipeline with node stats.
    let end_time = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let start_time = end_time - Duration::from_secs(60);
    let run = state.run_store.create_run("etl-orders", "default").unwrap();
    let run_id = run.id.clone();
    state.run_store.set_running(&run_id, start_time).unwrap();
    state
        .run_store
        .save_node_stats(
            &run_id,
            &NodeRunStats {
                node_id: flux_engine::NodeId("sink1".to_string()),
                start_time,
                end_time,
                rows_in: 500,
                rows_out: 500,
                error: None,
                materialization_receipt: None,
            },
        )
        .unwrap();
    state
        .run_store
        .finish_run(&run_id, RunStatus::Success, end_time, None)
        .unwrap();

    // Query the catalog detail endpoint and verify enrichment.
    let app = test_router(state);
    let fp = "postgres://db:5432/analytics/public.orders";
    let uri = format!(
        "/api/catalog/resources/detail?fingerprint={}",
        encode_qp(fp)
    );
    let resp = app
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;

    // last_updated should be populated from the run end_time.
    let last_updated = body["derived"]["last_updated"]
        .as_str()
        .expect("last_updated should be set");
    assert!(
        last_updated.starts_with("2023-11-14"),
        "expected 2023-11-14 timestamp, got: {last_updated}"
    );

    // row_count should be populated from sink node stats.
    assert_eq!(
        body["derived"]["row_count"].as_u64(),
        Some(500),
        "row_count should be enriched from node stats"
    );
}
