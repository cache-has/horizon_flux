// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the cross-pipeline lineage API (planning doc 31).

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use flux_connectors::ConnectorRegistry;
use flux_datafusion::{
    LineageObservation, SqliteBackfillStore, SqliteEnvironmentStore, SqliteRunStore, StoredResourceBinding,
};
use flux_engine::SqlitePipelineStore;
use flux_engine::lineage::{BindingDirection, ResourceFingerprint};
use flux_scheduler;
use flux_server::AppState;
use http_body_util::BodyExt;
use serde_json::Value;
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
    }
}

fn test_router(state: AppState) -> Router {
    Router::new()
        .nest("/api/lineage", flux_server::api::lineage::router())
        .with_state(state)
}

async fn body_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

/// Seed bindings for a linear chain: A (sink → table_x) → B (source ← table_x, sink → table_y) → C (source ← table_y).
fn seed_linear_chain(state: &AppState) -> (String, String, String) {
    let id_a = "00000000-0000-0000-0000-000000000001";
    let id_b = "00000000-0000-0000-0000-000000000002";
    let id_c = "00000000-0000-0000-0000-000000000003";
    let env = "default";
    let now = 1000i64;

    state
        .lineage_store
        .save_bindings(
            id_a,
            env,
            &[StoredResourceBinding {
                pipeline_id: id_a.into(),
                node_id: "sink_a".into(),
                direction: BindingDirection::Sink,
                resource_fingerprint: ResourceFingerprint::new("postgres://db/public.table_x"),
                environment: env.into(),
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
                    pipeline_id: id_b.into(),
                    node_id: "src_b".into(),
                    direction: BindingDirection::Source,
                    resource_fingerprint: ResourceFingerprint::new("postgres://db/public.table_x"),
                    environment: env.into(),
                    updated_at_ms: now,
                },
                StoredResourceBinding {
                    pipeline_id: id_b.into(),
                    node_id: "sink_b".into(),
                    direction: BindingDirection::Sink,
                    resource_fingerprint: ResourceFingerprint::new("postgres://db/public.table_y"),
                    environment: env.into(),
                    updated_at_ms: now,
                },
            ],
        )
        .unwrap();

    state
        .lineage_store
        .save_bindings(
            id_c,
            env,
            &[StoredResourceBinding {
                pipeline_id: id_c.into(),
                node_id: "src_c".into(),
                direction: BindingDirection::Source,
                resource_fingerprint: ResourceFingerprint::new("postgres://db/public.table_y"),
                environment: env.into(),
                updated_at_ms: now,
            }],
        )
        .unwrap();

    (id_a.into(), id_b.into(), id_c.into())
}

#[tokio::test]
async fn graph_empty() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/lineage/graph")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["edges"].as_array().unwrap().len(), 0);
    assert_eq!(body["pipelines"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn graph_with_chain() {
    let state = test_state();
    seed_linear_chain(&state);
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/lineage/graph")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["edges"].as_array().unwrap().len(), 2);
    // All three pipeline IDs should appear.
    assert_eq!(body["pipelines"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn upstream_of_middle() {
    let state = test_state();
    let (_id_a, id_b, _id_c) = seed_linear_chain(&state);
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri(&format!("/api/lineage/pipelines/{id_b}/upstream"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["direct"].as_array().unwrap().len(), 1);
    assert_eq!(body["transitive"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn downstream_of_first() {
    let state = test_state();
    let (id_a, _id_b, _id_c) = seed_linear_chain(&state);
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri(&format!("/api/lineage/pipelines/{id_a}/downstream"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    // Direct: A→B only.
    assert_eq!(body["direct"].as_array().unwrap().len(), 1);
    // Transitive: B and C.
    assert_eq!(body["transitive"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn impact_analysis() {
    let state = test_state();
    let (id_a, _id_b, _id_c) = seed_linear_chain(&state);
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri(&format!("/api/lineage/pipelines/{id_a}/impact"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["affected_pipelines"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn no_cycles_in_linear_chain() {
    let state = test_state();
    seed_linear_chain(&state);
    let app = test_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/lineage/cycles")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert!(body["cycles"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn orphans_detected() {
    let state = test_state();
    let env = "default";
    let now = 1000i64;
    // Pipeline with a sink nobody reads from.
    state
        .lineage_store
        .save_bindings(
            "00000000-0000-0000-0000-000000000010",
            env,
            &[StoredResourceBinding {
                pipeline_id: "00000000-0000-0000-0000-000000000010".into(),
                node_id: "lonely_sink".into(),
                direction: BindingDirection::Sink,
                resource_fingerprint: ResourceFingerprint::new("file:///tmp/orphan.csv"),
                environment: env.into(),
                updated_at_ms: now,
            }],
        )
        .unwrap();
    // Pipeline with a source nobody writes to.
    state
        .lineage_store
        .save_bindings(
            "00000000-0000-0000-0000-000000000011",
            env,
            &[StoredResourceBinding {
                pipeline_id: "00000000-0000-0000-0000-000000000011".into(),
                node_id: "lonely_src".into(),
                direction: BindingDirection::Source,
                resource_fingerprint: ResourceFingerprint::new("file:///tmp/dangling.csv"),
                environment: env.into(),
                updated_at_ms: now,
            }],
        )
        .unwrap();

    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/lineage/orphans")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["orphaned_sinks"].as_array().unwrap().len(), 1);
    assert_eq!(body["dangling_sources"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn invalid_pipeline_id_returns_400() {
    let app = test_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/lineage/pipelines/not-a-uuid/upstream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

/// Pipeline A writes to a resource, pipeline B reads from it — with no static
/// bindings, the observed edge should appear in the lineage graph.
#[tokio::test]
async fn runtime_observation_creates_edge() {
    let state = test_state();
    let id_a = "00000000-0000-0000-0000-000000000020";
    let id_b = "00000000-0000-0000-0000-000000000021";
    let env = "default";
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Pipeline A sink writes to a resource.
    state
        .lineage_store
        .record_observation(&LineageObservation {
            pipeline_id: id_a.into(),
            node_id: "sink_a".into(),
            run_id: "run-001".into(),
            direction: BindingDirection::Sink,
            resource_fingerprint: ResourceFingerprint::new("postgres://db/public.shared_table"),
            environment: env.into(),
            observed_at_ms: now_ms,
        })
        .unwrap();

    // Pipeline B source reads from the same resource.
    state
        .lineage_store
        .record_observation(&LineageObservation {
            pipeline_id: id_b.into(),
            node_id: "src_b".into(),
            run_id: "run-002".into(),
            direction: BindingDirection::Source,
            resource_fingerprint: ResourceFingerprint::new("postgres://db/public.shared_table"),
            environment: env.into(),
            observed_at_ms: now_ms,
        })
        .unwrap();

    let app = test_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/lineage/graph")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;

    let edges = body["edges"].as_array().unwrap();
    assert_eq!(edges.len(), 1, "expected one observed edge");

    let edge = &edges[0];
    assert_eq!(edge["upstream_pipeline_id"], id_a);
    assert_eq!(edge["upstream_node_id"], "sink_a");
    assert_eq!(edge["downstream_pipeline_id"], id_b);
    assert_eq!(edge["downstream_node_id"], "src_b");
    assert_eq!(edge["source"], "observed");
    assert_eq!(edge["resource"], "postgres://db/public.shared_table");

    // Both pipelines should appear in the pipeline list.
    let pipelines = body["pipelines"].as_array().unwrap();
    assert_eq!(pipelines.len(), 2);
}
