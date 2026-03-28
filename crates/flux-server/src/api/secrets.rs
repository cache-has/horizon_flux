// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Secrets management API routes.

use crate::api::ApiError;
use crate::state::AppState;
use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use flux_secrets::{SecretError, SecretStore};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// Build the `/secrets` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_secrets).post(create_or_update_secret))
        .route("/{name}", delete(delete_secret))
        .route("/{name}/environments", get(list_secret_environments))
}

// ---------------------------------------------------------------------------
// Request/Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct SecretMetadataResponse {
    name: String,
    environment: Option<String>,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Deserialize)]
struct CreateSecretRequest {
    name: String,
    value: String,
    environment: Option<String>,
}

#[derive(Debug, Serialize)]
struct SecretEnvironmentEntry {
    environment: Option<String>,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Serialize)]
struct SecretEnvironmentsResponse {
    name: String,
    environments: Vec<SecretEnvironmentEntry>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn require_store(
    store: &Option<Arc<Mutex<SecretStore>>>,
) -> Result<Arc<Mutex<SecretStore>>, (StatusCode, Json<ApiError>)> {
    store.clone().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::with_details(
                "secret store not available",
                "Set HORIZON_FLUX_SECRET_PASSWORD or run `horizon-flux secret init` first",
            )),
        )
    })
}

fn map_secret_error(e: SecretError, name: &str) -> (StatusCode, Json<ApiError>) {
    match &e {
        SecretError::NotFound(_) => ApiError::not_found("secret", name),
        SecretError::NotInitialized => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new(e.to_string())),
        ),
        _ => ApiError::internal(e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /api/secrets` — list secret names and metadata (never values).
async fn list_secrets(
    State(state): State<AppState>,
) -> Result<Json<Vec<SecretMetadataResponse>>, (StatusCode, Json<ApiError>)> {
    let store = require_store(&state.secret_store)?;
    let guard = store.lock().map_err(|e| ApiError::internal(e.to_string()))?;

    let secrets = guard.list().map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(
        secrets
            .into_iter()
            .map(|s| SecretMetadataResponse {
                name: s.name,
                environment: s.environment,
                created_at: s.created_at,
                updated_at: s.updated_at,
            })
            .collect(),
    ))
}

/// `POST /api/secrets` — create or update a secret (value transmitted, encrypted at rest).
async fn create_or_update_secret(
    State(state): State<AppState>,
    Json(req): Json<CreateSecretRequest>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("secret name must not be empty"));
    }
    if req.value.is_empty() {
        return Err(ApiError::bad_request("secret value must not be empty"));
    }

    let store = require_store(&state.secret_store)?;
    let guard = store.lock().map_err(|e| ApiError::internal(e.to_string()))?;

    guard
        .set(&req.name, req.value.as_bytes(), req.environment.as_deref())
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(StatusCode::CREATED)
}

/// `DELETE /api/secrets/:name` — delete a secret.
///
/// Deletes the default (unscoped) entry. To delete an environment-specific
/// override, use `DELETE /api/secrets/:name?environment=staging`.
async fn delete_secret(
    State(state): State<AppState>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<DeleteSecretQuery>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    let store = require_store(&state.secret_store)?;
    let guard = store.lock().map_err(|e| ApiError::internal(e.to_string()))?;

    guard
        .delete(&name, params.environment.as_deref())
        .map_err(|e| map_secret_error(e, &name))?;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct DeleteSecretQuery {
    environment: Option<String>,
}

/// `GET /api/secrets/:name/environments` — list which environments have overrides
/// for this secret.
async fn list_secret_environments(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<SecretEnvironmentsResponse>, (StatusCode, Json<ApiError>)> {
    let store = require_store(&state.secret_store)?;
    let guard = store.lock().map_err(|e| ApiError::internal(e.to_string()))?;

    let all = guard.list().map_err(|e| ApiError::internal(e.to_string()))?;
    let entries: Vec<SecretEnvironmentEntry> = all
        .into_iter()
        .filter(|s| s.name == name)
        .map(|s| SecretEnvironmentEntry {
            environment: s.environment,
            created_at: s.created_at,
            updated_at: s.updated_at,
        })
        .collect();

    if entries.is_empty() {
        return Err(ApiError::not_found("secret", &name));
    }

    Ok(Json(SecretEnvironmentsResponse {
        name,
        environments: entries,
    }))
}
