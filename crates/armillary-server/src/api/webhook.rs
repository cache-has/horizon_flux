// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Webhook trigger endpoint.
//!
//! Mounts under `/triggers/webhook/:trigger_id`. External systems POST JSON to
//! this endpoint to fire the associated trigger. Authentication is via a
//! per-trigger bearer token stored in the trigger's sensor state.

use crate::api::ApiError;
use crate::state::AppState;
use armillary_scheduler::{TriggerId, TriggerKind, TriggerState, TriggerStorage};
use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use serde::Serialize;
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;
use uuid::Uuid;

/// Webhook auth token state stored in `TriggerState.sensor_state`.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct WebhookState {
    token: String,
}

/// Response returned when a webhook fires successfully.
#[derive(Debug, Serialize)]
struct WebhookFireResponse {
    outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<String>,
}

pub fn router() -> Router<AppState> {
    Router::new().route("/{trigger_id}", post(webhook_handler))
}

/// `POST /triggers/webhook/:trigger_id`
///
/// Authenticates via `Authorization: Bearer <token>` header or `?token=<token>`
/// query parameter. Fires the webhook trigger and returns the outcome.
async fn webhook_handler(
    State(state): State<AppState>,
    Path(trigger_id): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: Option<Json<serde_json::Value>>,
) -> impl IntoResponse {
    // Parse trigger ID.
    let tid = match TriggerId::from_str(&trigger_id) {
        Ok(id) => id,
        Err(_) => return ApiError::bad_request("invalid trigger ID").into_response(),
    };

    // Verify the trigger exists and is a webhook.
    let trigger = match state.trigger_store.get_trigger(&tid) {
        Ok(t) => t,
        Err(_) => return ApiError::not_found("trigger", &trigger_id).into_response(),
    };
    if !matches!(trigger.kind, TriggerKind::Webhook { .. }) {
        return ApiError::bad_request("trigger is not a webhook trigger").into_response();
    }

    // Resolve or create the auth token.
    let expected_token = match ensure_webhook_token(&state.trigger_store, &tid) {
        Ok(tok) => tok,
        Err(e) => {
            warn!(trigger = %tid, "failed to resolve webhook token: {e}");
            return ApiError::internal("failed to resolve webhook auth token").into_response();
        }
    };

    // Extract the provided token from header or query param.
    let provided_token = extract_token(&headers, &params);
    match provided_token {
        Some(tok) if tok == expected_token => {}
        Some(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ApiError::new("invalid token")),
            )
                .into_response();
        }
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ApiError::new("missing authentication token")),
            )
                .into_response();
        }
    }

    // Fire the trigger via the scheduler.
    let scheduler = match &state.scheduler {
        Some(s) => Arc::clone(s),
        None => return ApiError::internal("scheduler not available").into_response(),
    };

    let request_body = body.map(|b| b.0).unwrap_or(serde_json::Value::Null);

    match scheduler.fire_webhook(&tid, &request_body) {
        Ok(result) => Json(WebhookFireResponse {
            outcome: result.outcome.to_string(),
            run_id: result.run_id,
        })
        .into_response(),
        Err(e) => {
            let status = if matches!(e, armillary_scheduler::SchedulerError::TriggerDisabled(_)) {
                StatusCode::CONFLICT
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (status, Json(ApiError::new(e.to_string()))).into_response()
        }
    }
}

/// Ensure the webhook trigger has an auth token in its sensor state.
/// If none exists, generate one and persist it.
fn ensure_webhook_token(
    store: &Arc<dyn TriggerStorage>,
    trigger_id: &TriggerId,
) -> Result<String, String> {
    let state = store.get_state(trigger_id).map_err(|e| e.to_string())?;

    // Try to read existing token.
    if let Some(ref s) = state {
        if let Some(ref sensor) = s.sensor_state {
            if let Ok(ws) = serde_json::from_value::<WebhookState>(sensor.clone()) {
                return Ok(ws.token);
            }
        }
    }

    // Generate a new token and persist it.
    let token = Uuid::new_v4().to_string();
    let ws = WebhookState {
        token: token.clone(),
    };
    let new_state = TriggerState {
        trigger_id: trigger_id.clone(),
        last_evaluated_at: state.as_ref().and_then(|s| s.last_evaluated_at.clone()),
        last_fired_at: state.as_ref().and_then(|s| s.last_fired_at.clone()),
        next_fire_at: None,
        sensor_state: Some(serde_json::to_value(&ws).unwrap()),
        consecutive_errors: state.as_ref().map_or(0, |s| s.consecutive_errors),
    };
    store.upsert_state(&new_state).map_err(|e| e.to_string())?;

    Ok(token)
}

/// Extract bearer token from `Authorization` header or `token` query param.
fn extract_token(
    headers: &HeaderMap,
    params: &std::collections::HashMap<String, String>,
) -> Option<String> {
    // Try Authorization: Bearer <token>
    if let Some(auth) = headers.get("authorization") {
        if let Ok(val) = auth.to_str() {
            if let Some(token) = val.strip_prefix("Bearer ") {
                return Some(token.to_string());
            }
        }
    }
    // Try query parameter.
    params.get("token").cloned()
}
