// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Trigger management API routes.
//!
//! CRUD endpoints for triggers, plus enable/disable, manual fire, and
//! firing history. Mounted under `/api/triggers`.

use crate::api::ApiError;
use crate::state::AppState;
use armillary_scheduler::{SchedulerError, Trigger, TriggerId, TriggerKind, TriggerState};
use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

/// Build the `/triggers` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_triggers).post(create_trigger))
        .route(
            "/{id}",
            get(get_trigger).put(update_trigger).delete(delete_trigger),
        )
        .route("/{id}/enable", post(enable_trigger))
        .route("/{id}/disable", post(disable_trigger))
        .route("/{id}/fire", post(fire_trigger))
        .route("/{id}/history", get(trigger_history))
}

// ---------------------------------------------------------------------------
// Query / request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ListQuery {
    pipeline_id: Option<String>,
    environment: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateTriggerRequest {
    name: String,
    pipeline_id: String,
    #[serde(default = "default_environment")]
    environment: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
    kind: TriggerKind,
    #[serde(default)]
    run_policy: armillary_scheduler::RunPolicy,
    #[serde(default)]
    variable_overrides: Option<HashMap<String, serde_json::Value>>,
    #[serde(default = "default_max_queue_depth")]
    max_queue_depth: u32,
}

fn default_environment() -> String {
    "dev".to_string()
}
fn default_enabled() -> bool {
    true
}
fn default_max_queue_depth() -> u32 {
    3
}

#[derive(Debug, Deserialize)]
struct UpdateTriggerRequest {
    name: Option<String>,
    pipeline_id: Option<String>,
    environment: Option<String>,
    enabled: Option<bool>,
    kind: Option<TriggerKind>,
    run_policy: Option<armillary_scheduler::RunPolicy>,
    variable_overrides: Option<HashMap<String, serde_json::Value>>,
    max_queue_depth: Option<u32>,
}

#[derive(Debug, Serialize)]
struct TriggerResponse {
    #[serde(flatten)]
    trigger: Trigger,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<TriggerState>,
}

#[derive(Debug, Serialize)]
struct FireResponse {
    outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    #[serde(default = "default_history_limit")]
    limit: u32,
}

fn default_history_limit() -> u32 {
    50
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /api/triggers`
async fn list_triggers(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> impl IntoResponse {
    match state
        .trigger_store
        .list_triggers(q.pipeline_id.as_deref(), q.environment.as_deref())
    {
        Ok(triggers) => Json(serde_json::json!({ "data": triggers })).into_response(),
        Err(e) => ApiError::internal(e.to_string()).into_response(),
    }
}

/// `POST /api/triggers`
async fn create_trigger(
    State(state): State<AppState>,
    Json(req): Json<CreateTriggerRequest>,
) -> impl IntoResponse {
    let now = Utc::now().to_rfc3339();
    let trigger = Trigger {
        id: TriggerId::new(),
        name: req.name,
        pipeline_id: req.pipeline_id,
        environment: req.environment,
        enabled: req.enabled,
        kind: req.kind,
        run_policy: req.run_policy,
        variable_overrides: req.variable_overrides,
        max_queue_depth: req.max_queue_depth,
        created_at: now.clone(),
        updated_at: now,
    };

    if let Err(e) = state.trigger_store.create_trigger(&trigger) {
        return ApiError::internal(e.to_string()).into_response();
    }

    // Initialize state for cron/interval triggers with next_fire_at.
    let initial_state = TriggerState {
        trigger_id: trigger.id.clone(),
        last_evaluated_at: None,
        last_fired_at: None,
        next_fire_at: compute_next_fire(&trigger.kind),
        sensor_state: None,
        consecutive_errors: 0,
    };
    let _ = state.trigger_store.upsert_state(&initial_state);

    // Broadcast trigger creation event.
    let _ = state
        .event_tx
        .send(armillary_datafusion::ExecutionEvent::TriggerChanged {
            trigger_id: trigger.id.to_string(),
            action: "created".to_string(),
        });

    (
        StatusCode::CREATED,
        Json(TriggerResponse {
            trigger,
            state: Some(initial_state),
        }),
    )
        .into_response()
}

/// `GET /api/triggers/:id`
async fn get_trigger(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let tid = match parse_trigger_id(&id) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let trigger = match state.trigger_store.get_trigger(&tid) {
        Ok(t) => t,
        Err(_) => return ApiError::not_found("trigger", &id).into_response(),
    };

    let trigger_state = state.trigger_store.get_state(&tid).ok().flatten();

    Json(TriggerResponse {
        trigger,
        state: trigger_state,
    })
    .into_response()
}

/// `PUT /api/triggers/:id`
async fn update_trigger(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateTriggerRequest>,
) -> impl IntoResponse {
    let tid = match parse_trigger_id(&id) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let mut trigger = match state.trigger_store.get_trigger(&tid) {
        Ok(t) => t,
        Err(_) => return ApiError::not_found("trigger", &id).into_response(),
    };

    // Apply partial updates.
    if let Some(name) = req.name {
        trigger.name = name;
    }
    if let Some(pipeline_id) = req.pipeline_id {
        trigger.pipeline_id = pipeline_id;
    }
    if let Some(environment) = req.environment {
        trigger.environment = environment;
    }
    if let Some(enabled) = req.enabled {
        trigger.enabled = enabled;
    }
    if let Some(kind) = req.kind {
        trigger.kind = kind;
    }
    if let Some(run_policy) = req.run_policy {
        trigger.run_policy = run_policy;
    }
    if req.variable_overrides.is_some() {
        trigger.variable_overrides = req.variable_overrides;
    }
    if let Some(max_queue_depth) = req.max_queue_depth {
        trigger.max_queue_depth = max_queue_depth;
    }
    trigger.updated_at = Utc::now().to_rfc3339();

    if let Err(e) = state.trigger_store.update_trigger(&trigger) {
        return ApiError::internal(e.to_string()).into_response();
    }

    // Update next_fire_at if kind changed.
    if let Ok(Some(mut ts)) = state.trigger_store.get_state(&tid) {
        ts.next_fire_at = compute_next_fire(&trigger.kind);
        let _ = state.trigger_store.upsert_state(&ts);
    }

    let _ = state
        .event_tx
        .send(armillary_datafusion::ExecutionEvent::TriggerChanged {
            trigger_id: trigger.id.to_string(),
            action: "updated".to_string(),
        });

    let trigger_state = state.trigger_store.get_state(&tid).ok().flatten();
    Json(TriggerResponse {
        trigger,
        state: trigger_state,
    })
    .into_response()
}

/// `DELETE /api/triggers/:id`
async fn delete_trigger(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let tid = match parse_trigger_id(&id) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    match state.trigger_store.delete_trigger(&tid) {
        Ok(()) => {
            let _ = state
                .event_tx
                .send(armillary_datafusion::ExecutionEvent::TriggerChanged {
                    trigger_id: id,
                    action: "deleted".to_string(),
                });
            StatusCode::NO_CONTENT.into_response()
        }
        Err(armillary_scheduler::TriggerStoreError::NotFound(_)) => {
            ApiError::not_found("trigger", &id).into_response()
        }
        Err(e) => ApiError::internal(e.to_string()).into_response(),
    }
}

/// `POST /api/triggers/:id/enable`
async fn enable_trigger(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    set_enabled(state, &id, true).await
}

/// `POST /api/triggers/:id/disable`
async fn disable_trigger(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    set_enabled(state, &id, false).await
}

async fn set_enabled(state: AppState, id: &str, enabled: bool) -> axum::response::Response {
    let tid = match parse_trigger_id(id) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    // Verify trigger exists.
    let trigger = match state.trigger_store.get_trigger(&tid) {
        Ok(t) => t,
        Err(_) => return ApiError::not_found("trigger", id).into_response(),
    };

    if let Err(e) = state.trigger_store.set_enabled(&tid, enabled) {
        return ApiError::internal(e.to_string()).into_response();
    }

    let action = if enabled { "enabled" } else { "disabled" };
    let _ = state
        .event_tx
        .send(armillary_datafusion::ExecutionEvent::TriggerChanged {
            trigger_id: id.to_string(),
            action: action.to_string(),
        });

    // Return updated trigger.
    let mut updated = trigger;
    updated.enabled = enabled;
    let trigger_state = state.trigger_store.get_state(&tid).ok().flatten();
    Json(TriggerResponse {
        trigger: updated,
        state: trigger_state,
    })
    .into_response()
}

/// `POST /api/triggers/:id/fire`
async fn fire_trigger(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let tid = match parse_trigger_id(&id) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let scheduler = match &state.scheduler {
        Some(s) => Arc::clone(s),
        None => return ApiError::internal("scheduler not available").into_response(),
    };

    match scheduler.manual_fire(&tid) {
        Ok(result) => Json(FireResponse {
            outcome: result.outcome.to_string(),
            run_id: result.run_id,
        })
        .into_response(),
        Err(SchedulerError::TriggerDisabled(_)) => {
            ApiError::conflict("trigger is disabled").into_response()
        }
        Err(SchedulerError::Store(armillary_scheduler::TriggerStoreError::NotFound(_))) => {
            ApiError::not_found("trigger", &id).into_response()
        }
        Err(e) => ApiError::internal(e.to_string()).into_response(),
    }
}

/// `GET /api/triggers/:id/history`
async fn trigger_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<HistoryQuery>,
) -> impl IntoResponse {
    let tid = match parse_trigger_id(&id) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    // Verify trigger exists.
    if state.trigger_store.get_trigger(&tid).is_err() {
        return ApiError::not_found("trigger", &id).into_response();
    }

    match state.trigger_store.get_history(&tid, q.limit) {
        Ok(entries) => Json(serde_json::json!({ "data": entries })).into_response(),
        Err(e) => ApiError::internal(e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[allow(clippy::result_large_err)]
fn parse_trigger_id(id: &str) -> Result<TriggerId, axum::response::Response> {
    TriggerId::from_str(id).map_err(|_| ApiError::bad_request("invalid trigger ID").into_response())
}

/// Compute the initial `next_fire_at` for cron/interval triggers.
fn compute_next_fire(kind: &TriggerKind) -> Option<String> {
    match kind {
        TriggerKind::Cron {
            expression,
            timezone,
        } => armillary_scheduler::cron::CronSchedule::parse(expression, timezone)
            .ok()
            .and_then(|sched| sched.next_after(Utc::now()))
            .map(|dt| dt.to_rfc3339()),
        TriggerKind::Interval { every, start_at } => {
            let start = start_at
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);
            armillary_scheduler::interval::Iso8601Duration::parse(every)
                .ok()
                .map(|dur| dur.next_after(start, Utc::now()).to_rfc3339())
        }
        _ => None,
    }
}
