// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Backfill management API routes (planning doc 33).
//!
//! CRUD endpoints for backfills, plus resume and cancel. Mounted under
//! `/api/backfills`.

use crate::api::ApiError;
use crate::state::AppState;
use armillary_datafusion::{BackfillEvent, BackfillRunOptions, ExecutionEvent, ExecutionOptions};
use armillary_engine::backfill::{
    Backfill, BackfillId, BackfillProgress, BackfillStatus, RangeDefinition,
};
use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::mpsc;

/// Build the `/backfills` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_backfills).post(create_backfill))
        .route("/{id}", get(get_backfill).delete(delete_backfill))
        .route("/{id}/resume", post(resume_backfill))
        .route("/{id}/cancel", post(cancel_backfill))
}

// ---------------------------------------------------------------------------
// Query / request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ListQuery {
    pipeline_id: Option<String>,
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    50
}

#[derive(Debug, Deserialize)]
struct CreateBackfillRequest {
    pipeline_id: String,
    #[serde(default = "default_environment")]
    environment: String,
    range_definition: RangeDefinition,
    #[serde(default = "default_concurrency")]
    concurrency: u32,
    #[serde(default)]
    fail_fast: bool,
    #[serde(default = "default_full_refresh")]
    full_refresh: bool,
    #[serde(default)]
    variables: HashMap<String, serde_json::Value>,
}

fn default_environment() -> String {
    "dev".to_string()
}
fn default_concurrency() -> u32 {
    1
}
fn default_full_refresh() -> bool {
    true
}

#[derive(Debug, Serialize)]
struct BackfillResponse {
    #[serde(flatten)]
    backfill: Backfill,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<BackfillProgress>,
}

#[derive(Debug, Serialize)]
struct BackfillDetailResponse {
    #[serde(flatten)]
    backfill: Backfill,
    progress: BackfillProgress,
    iterations: Vec<armillary_engine::backfill::BackfillIteration>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /api/backfills`
async fn list_backfills(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> impl IntoResponse {
    let status_filter = q.status.as_deref().and_then(BackfillStatus::parse);
    if q.status.is_some() && status_filter.is_none() {
        return ApiError::bad_request("invalid status filter").into_response();
    }

    match state
        .backfill_store
        .list_backfills(q.pipeline_id.as_deref(), status_filter, q.limit)
    {
        Ok(backfills) => Json(serde_json::json!({ "data": backfills })).into_response(),
        Err(e) => ApiError::internal(e.to_string()).into_response(),
    }
}

/// `POST /api/backfills` — create and start a backfill.
async fn create_backfill(
    State(state): State<AppState>,
    Json(req): Json<CreateBackfillRequest>,
) -> impl IntoResponse {
    // Validate the pipeline exists.
    let pipeline_id = match req.pipeline_id.parse::<armillary_engine::PipelineId>() {
        Ok(id) => id,
        Err(_) => return ApiError::bad_request("invalid pipeline ID").into_response(),
    };
    let record = match state.pipeline_store.get(&pipeline_id) {
        Ok(Some(r)) => r,
        Ok(None) => return ApiError::not_found("pipeline", &req.pipeline_id).into_response(),
        Err(e) => return ApiError::internal(e.to_string()).into_response(),
    };

    // Validate concurrency.
    if req.concurrency == 0 {
        return ApiError::bad_request("concurrency must be >= 1").into_response();
    }

    // Validate the range can be expanded (catches bad dates, unknown
    // placeholders, etc.) — except for SQL ranges which need a connection.
    if !matches!(req.range_definition, RangeDefinition::Sql { .. }) {
        if let Err(e) = armillary_engine::backfill::expand_range(&req.range_definition) {
            return ApiError::bad_request(format!("invalid range: {e}")).into_response();
        }
    }

    let now = Utc::now().to_rfc3339();
    let backfill = Backfill {
        id: BackfillId::new(),
        pipeline_id: req.pipeline_id.clone(),
        environment: req.environment.clone(),
        range_definition: req.range_definition,
        concurrency: req.concurrency,
        fail_fast: req.fail_fast,
        full_refresh: req.full_refresh,
        status: BackfillStatus::Pending,
        created_at: now,
        started_at: None,
        completed_at: None,
        created_by: None,
    };

    let backfill_id = backfill.id.clone();
    let response_backfill = backfill.clone();

    // Set up a progress channel that forwards backfill events to the
    // broadcast channel for WebSocket clients.
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<BackfillEvent>();
    let event_tx = state.event_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = progress_rx.recv().await {
            let _ = event_tx.send(ExecutionEvent::Backfill(event));
        }
    });

    // Build execution options.
    let cancel = Arc::new(AtomicBool::new(false));
    let provider_registry = state.connector_registry.to_provider_registry();

    let base_options = ExecutionOptions {
        environment: req.environment,
        run_store: Some(Arc::clone(&state.run_store)),
        cancel: cancel.clone(),
        environment_resolver: None,
        progress: None,
        variable_overrides: req.variables,
        secret_resolver: state.secret_resolver(),
        session_factory: state.session_factory.clone(),
        incremental_state_store: Some(Arc::clone(&state.incremental_state_store)),
        full_refresh: req.full_refresh,
        bootstrap_incremental: false,
        dry_run_no_sinks: false,
        lineage_store: Some(Arc::clone(&state.lineage_store)),
        fingerprint_fn: Some(armillary_connectors::fingerprint::fingerprint),
        pipeline_id: Some(req.pipeline_id),
        column_lineage_store: state.column_lineage_store.clone(),
        on_column_lineage_updated: None,
        triggered_by: Some("api:backfill".into()),
        openlineage_client: state.openlineage_client.clone(),
    };

    let opts = BackfillRunOptions {
        pipeline: record.pipeline,
        registry: Arc::new(provider_registry),
        base_options,
        backfill_store: Arc::clone(&state.backfill_store),
        progress: Some(progress_tx),
        cancel,
    };

    // Spawn the backfill coordinator in the background — don't block the
    // HTTP response.
    tokio::spawn(async move {
        if let Err(e) = armillary_datafusion::backfill::start_backfill(backfill, opts).await {
            tracing::error!(backfill_id = %backfill_id, error = %e, "backfill failed");
        }
    });

    (
        StatusCode::CREATED,
        Json(BackfillResponse {
            backfill: response_backfill,
            progress: None,
        }),
    )
        .into_response()
}

/// `GET /api/backfills/:id`
async fn get_backfill(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let bf_id = BackfillId(id.clone());

    let backfill = match state.backfill_store.get_backfill(&bf_id) {
        Ok(Some(b)) => b,
        Ok(None) => return ApiError::not_found("backfill", &id).into_response(),
        Err(e) => return ApiError::internal(e.to_string()).into_response(),
    };

    let progress = match state.backfill_store.get_progress(&bf_id) {
        Ok(p) => p,
        Err(e) => return ApiError::internal(e.to_string()).into_response(),
    };

    let iterations = match state.backfill_store.list_iterations(&bf_id) {
        Ok(i) => i,
        Err(e) => return ApiError::internal(e.to_string()).into_response(),
    };

    Json(BackfillDetailResponse {
        backfill,
        progress,
        iterations,
    })
    .into_response()
}

/// `DELETE /api/backfills/:id`
async fn delete_backfill(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let bf_id = BackfillId(id.clone());

    // Don't allow deleting running backfills.
    match state.backfill_store.get_backfill(&bf_id) {
        Ok(Some(b)) if b.status == BackfillStatus::Running => {
            return ApiError::conflict("cannot delete a running backfill — cancel it first")
                .into_response();
        }
        Ok(None) => return ApiError::not_found("backfill", &id).into_response(),
        Err(e) => return ApiError::internal(e.to_string()).into_response(),
        _ => {}
    }

    match state.backfill_store.delete_backfill(&bf_id) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => ApiError::not_found("backfill", &id).into_response(),
        Err(e) => ApiError::internal(e.to_string()).into_response(),
    }
}

/// `POST /api/backfills/:id/resume`
async fn resume_backfill(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let bf_id = BackfillId(id.clone());

    let backfill = match state.backfill_store.get_backfill(&bf_id) {
        Ok(Some(b)) => b,
        Ok(None) => return ApiError::not_found("backfill", &id).into_response(),
        Err(e) => return ApiError::internal(e.to_string()).into_response(),
    };

    // Only allow resuming failed or cancelled backfills.
    match backfill.status {
        BackfillStatus::Failed | BackfillStatus::Cancelled => {}
        BackfillStatus::Running => {
            return ApiError::conflict("backfill is already running").into_response();
        }
        BackfillStatus::Completed => {
            return ApiError::conflict("backfill already completed").into_response();
        }
        BackfillStatus::Pending => {
            return ApiError::conflict("backfill has not started yet").into_response();
        }
    }

    // Look up the pipeline.
    let pipeline_id = match backfill.pipeline_id.parse::<armillary_engine::PipelineId>() {
        Ok(id) => id,
        Err(_) => {
            return ApiError::internal("invalid pipeline ID in backfill record").into_response();
        }
    };
    let record = match state.pipeline_store.get(&pipeline_id) {
        Ok(Some(r)) => r,
        Ok(None) => {
            return ApiError::bad_request(format!(
                "pipeline `{}` no longer exists",
                backfill.pipeline_id
            ))
            .into_response();
        }
        Err(e) => return ApiError::internal(e.to_string()).into_response(),
    };

    // Set up progress forwarding.
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<BackfillEvent>();
    let event_tx = state.event_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = progress_rx.recv().await {
            let _ = event_tx.send(ExecutionEvent::Backfill(event));
        }
    });

    let cancel = Arc::new(AtomicBool::new(false));
    let provider_registry = state.connector_registry.to_provider_registry();

    let base_options = ExecutionOptions {
        environment: backfill.environment.clone(),
        run_store: Some(Arc::clone(&state.run_store)),
        cancel: cancel.clone(),
        environment_resolver: None,
        progress: None,
        variable_overrides: HashMap::new(),
        secret_resolver: state.secret_resolver(),
        session_factory: state.session_factory.clone(),
        incremental_state_store: Some(Arc::clone(&state.incremental_state_store)),
        full_refresh: backfill.full_refresh,
        bootstrap_incremental: false,
        dry_run_no_sinks: false,
        lineage_store: Some(Arc::clone(&state.lineage_store)),
        fingerprint_fn: Some(armillary_connectors::fingerprint::fingerprint),
        pipeline_id: Some(backfill.pipeline_id.clone()),
        column_lineage_store: state.column_lineage_store.clone(),
        on_column_lineage_updated: None,
        triggered_by: Some("api:backfill".into()),
        openlineage_client: state.openlineage_client.clone(),
    };

    let opts = BackfillRunOptions {
        pipeline: record.pipeline,
        registry: Arc::new(provider_registry),
        base_options,
        backfill_store: Arc::clone(&state.backfill_store),
        progress: Some(progress_tx),
        cancel,
    };

    let bf_id_clone = bf_id.clone();
    tokio::spawn(async move {
        if let Err(e) = armillary_datafusion::backfill::resume_backfill(&bf_id_clone, opts).await {
            tracing::error!(backfill_id = %bf_id_clone, error = %e, "backfill resume failed");
        }
    });

    let progress = state
        .backfill_store
        .get_progress(&bf_id)
        .unwrap_or(BackfillProgress {
            total: 0,
            succeeded: 0,
            failed: 0,
            running: 0,
            pending: 0,
            skipped: 0,
        });

    Json(BackfillResponse {
        backfill,
        progress: Some(progress),
    })
    .into_response()
}

/// `POST /api/backfills/:id/cancel`
async fn cancel_backfill(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let bf_id = BackfillId(id.clone());

    let backfill = match state.backfill_store.get_backfill(&bf_id) {
        Ok(Some(b)) => b,
        Ok(None) => return ApiError::not_found("backfill", &id).into_response(),
        Err(e) => return ApiError::internal(e.to_string()).into_response(),
    };

    if backfill.status != BackfillStatus::Running && backfill.status != BackfillStatus::Pending {
        return ApiError::conflict("backfill is not running").into_response();
    }

    if let Err(e) =
        armillary_datafusion::backfill::cancel_backfill(&bf_id, state.backfill_store.as_ref())
    {
        return ApiError::internal(e.to_string()).into_response();
    }

    // Broadcast the cancellation event.
    let _ = state
        .event_tx
        .send(ExecutionEvent::Backfill(BackfillEvent::BackfillCancelled {
            backfill_id: bf_id.clone(),
        }));

    // Return the updated backfill.
    match state.backfill_store.get_backfill(&bf_id) {
        Ok(Some(updated)) => {
            let progress = state.backfill_store.get_progress(&bf_id).ok();
            Json(BackfillResponse {
                backfill: updated,
                progress,
            })
            .into_response()
        }
        _ => StatusCode::NO_CONTENT.into_response(),
    }
}
