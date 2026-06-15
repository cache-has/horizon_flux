// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Secrets management API routes.
//!
//! Provides endpoints for secret store lifecycle (init, unlock, lock, status)
//! and CRUD operations on secrets. Secret values are never returned by any
//! endpoint.

use crate::api::ApiError;
use crate::state::AppState;
use armillary_secrets::SecretError;
use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use serde::{Deserialize, Serialize};

/// Build the `/secrets` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/status", get(status))
        .route("/init", post(init_store))
        .route("/unlock", post(unlock))
        .route("/lock", post(lock))
        .route("/", get(list_secrets).post(create_or_update_secret))
        .route("/{name}", delete(delete_secret))
        .route("/{name}/environments", get(list_secret_environments))
}

// ---------------------------------------------------------------------------
// Request/Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct StatusResponse {
    initialized: bool,
    unlocked: bool,
}

#[derive(Debug, Deserialize)]
struct InitRequest {
    password: String,
    confirm: String,
}

#[derive(Debug, Deserialize)]
struct UnlockRequest {
    password: String,
}

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

#[derive(Debug, Deserialize)]
struct DeleteSecretQuery {
    environment: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn map_secret_error(e: SecretError, name: &str) -> (StatusCode, Json<ApiError>) {
    match &e {
        SecretError::NotFound(_) => ApiError::not_found("secret", name),
        SecretError::NotInitialized => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new(e.to_string())),
        ),
        SecretError::AlreadyInitialized => ApiError::conflict(e.to_string()),
        SecretError::Decryption(_) => (
            StatusCode::UNAUTHORIZED,
            Json(ApiError::new("incorrect password")),
        ),
        _ => ApiError::internal(e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Session lifecycle handlers
// ---------------------------------------------------------------------------

/// `GET /api/secrets/status` — check if the store is initialized and/or unlocked.
async fn status(
    State(state): State<AppState>,
) -> Result<Json<StatusResponse>, (StatusCode, Json<ApiError>)> {
    let session = state
        .secret_session
        .lock()
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(StatusResponse {
        initialized: session.is_initialized(),
        unlocked: session.is_unlocked(),
    }))
}

/// `POST /api/secrets/init` — initialize a new secret store.
async fn init_store(
    State(state): State<AppState>,
    Json(req): Json<InitRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ApiError>)> {
    if req.password.is_empty() {
        return Err(ApiError::bad_request("password must not be empty"));
    }
    if req.password != req.confirm {
        return Err(ApiError::bad_request("passwords do not match"));
    }

    let mut session = state
        .secret_session
        .lock()
        .map_err(|e| ApiError::internal(e.to_string()))?;

    session
        .init(req.password.as_bytes())
        .map_err(|e| map_secret_error(e, ""))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "initialized": true })),
    ))
}

/// `POST /api/secrets/unlock` — unlock the store with a password.
async fn unlock(
    State(state): State<AppState>,
    Json(req): Json<UnlockRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let mut session = state
        .secret_session
        .lock()
        .map_err(|e| ApiError::internal(e.to_string()))?;

    if !session.is_initialized() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::with_details(
                "secret store not initialized",
                "Initialize the store first via POST /api/secrets/init",
            )),
        ));
    }

    if !session.check_rate_limit() {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(ApiError::new(
                "too many unlock attempts — try again in a minute",
            )),
        ));
    }

    session.record_attempt();

    session
        .unlock(req.password.as_bytes())
        .map_err(|e| map_secret_error(e, ""))?;

    Ok(Json(serde_json::json!({ "unlocked": true })))
}

/// `POST /api/secrets/lock` — explicitly lock the store.
async fn lock(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let mut session = state
        .secret_session
        .lock()
        .map_err(|e| ApiError::internal(e.to_string()))?;

    session.lock();

    Ok(Json(serde_json::json!({ "locked": true })))
}

// ---------------------------------------------------------------------------
// CRUD handlers
// ---------------------------------------------------------------------------

/// `GET /api/secrets` — list secret names and metadata (never values).
async fn list_secrets(
    State(state): State<AppState>,
) -> Result<Json<Vec<SecretMetadataResponse>>, (StatusCode, Json<ApiError>)> {
    let mut session = state
        .secret_session
        .lock()
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let store = session.get_store().ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(ApiError::with_details(
                "secret store is locked",
                "Unlock the store first via POST /api/secrets/unlock",
            )),
        )
    })?;

    let secrets = store
        .list()
        .map_err(|e| ApiError::internal(e.to_string()))?;

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

    let mut session = state
        .secret_session
        .lock()
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let store = session.get_store().ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(ApiError::with_details(
                "secret store is locked",
                "Unlock the store first via POST /api/secrets/unlock",
            )),
        )
    })?;

    store
        .set(&req.name, req.value.as_bytes(), req.environment.as_deref())
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(StatusCode::CREATED)
}

/// `DELETE /api/secrets/:name` — delete a secret.
async fn delete_secret(
    State(state): State<AppState>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<DeleteSecretQuery>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    let mut session = state
        .secret_session
        .lock()
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let store = session.get_store().ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(ApiError::with_details(
                "secret store is locked",
                "Unlock the store first via POST /api/secrets/unlock",
            )),
        )
    })?;

    store
        .delete(&name, params.environment.as_deref())
        .map_err(|e| map_secret_error(e, &name))?;

    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/secrets/:name/environments` — list which environments have overrides.
async fn list_secret_environments(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<SecretEnvironmentsResponse>, (StatusCode, Json<ApiError>)> {
    let mut session = state
        .secret_session
        .lock()
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let store = session.get_store().ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(ApiError::with_details(
                "secret store is locked",
                "Unlock the store first via POST /api/secrets/unlock",
            )),
        )
    })?;

    let all = store
        .list()
        .map_err(|e| ApiError::internal(e.to_string()))?;
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
