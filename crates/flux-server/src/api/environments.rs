// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Environment management API routes.

use crate::api::ApiError;
use crate::state::AppState;
use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use flux_datafusion::error::EnvironmentError;
use serde::{Deserialize, Serialize};

/// Build the `/environments` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_environments).post(create_environment))
        .route("/{name}", delete(delete_environment))
        .route("/{name}/tables", get(list_table_overrides))
        .route(
            "/{name}/tables/{table}/override",
            post(create_table_override).delete(delete_table_override),
        )
        .route("/resolve/{table}", get(resolve_table))
}

// ---------------------------------------------------------------------------
// Request/Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct EnvironmentResponse {
    name: String,
    fallback: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateEnvironmentRequest {
    name: String,
    fallback: Option<String>,
}

#[derive(Debug, Serialize)]
struct TableOverrideResponse {
    environment: String,
    schema_name: String,
    table_name: String,
}

#[derive(Debug, Deserialize)]
struct CreateTableOverrideRequest {
    #[serde(default = "default_schema")]
    schema_name: String,
}

fn default_schema() -> String {
    "public".to_string()
}

#[derive(Debug, Serialize)]
struct ResolveResponse {
    table: String,
    chain: Vec<ResolveEntry>,
}

#[derive(Debug, Serialize)]
struct ResolveEntry {
    environment: String,
    has_override: bool,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /api/environments` — list all environments with metadata.
async fn list_environments(
    State(state): State<AppState>,
) -> Result<Json<Vec<EnvironmentResponse>>, (StatusCode, Json<ApiError>)> {
    let envs = state
        .environment_store
        .list()
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(
        envs.into_iter()
            .map(|e| EnvironmentResponse {
                name: e.name,
                fallback: e.fallback,
            })
            .collect(),
    ))
}

/// `POST /api/environments` — create a new environment.
async fn create_environment(
    State(state): State<AppState>,
    Json(req): Json<CreateEnvironmentRequest>,
) -> Result<(StatusCode, Json<EnvironmentResponse>), (StatusCode, Json<ApiError>)> {
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("environment name must not be empty"));
    }

    let env = state
        .environment_store
        .create(&req.name, req.fallback.as_deref())
        .map_err(|e| match &e {
            EnvironmentError::AlreadyExists(_) => ApiError::conflict(e.to_string()),
            EnvironmentError::FallbackNotFound(_) => ApiError::bad_request(e.to_string()),
            _ => ApiError::internal(e.to_string()),
        })?;

    Ok((
        StatusCode::CREATED,
        Json(EnvironmentResponse {
            name: env.name,
            fallback: env.fallback,
        }),
    ))
}

/// `DELETE /api/environments/:name` — delete an environment.
async fn delete_environment(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    state
        .environment_store
        .delete(&name)
        .map_err(|e| match &e {
            EnvironmentError::NotFound(_) => ApiError::not_found("environment", &name),
            EnvironmentError::CannotDeleteProd => ApiError::bad_request(e.to_string()),
            _ => ApiError::internal(e.to_string()),
        })?;

    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/environments/:name/tables` — list table overrides in an environment.
async fn list_table_overrides(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Vec<TableOverrideResponse>>, (StatusCode, Json<ApiError>)> {
    // Verify environment exists.
    state
        .environment_store
        .get(&name)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("environment", &name))?;

    let overrides = state
        .environment_store
        .list_table_overrides(&name)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(
        overrides
            .into_iter()
            .map(|o| TableOverrideResponse {
                environment: o.environment,
                schema_name: o.schema_name,
                table_name: o.table_name,
            })
            .collect(),
    ))
}

/// `POST /api/environments/:name/tables/:table/override` — create a table override.
async fn create_table_override(
    State(state): State<AppState>,
    Path((name, table)): Path<(String, String)>,
    Json(req): Json<CreateTableOverrideRequest>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    // Verify environment exists.
    state
        .environment_store
        .get(&name)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("environment", &name))?;

    state
        .environment_store
        .register_table_override(&name, &req.schema_name, &table)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(StatusCode::CREATED)
}

/// `DELETE /api/environments/:name/tables/:table/override` — remove a table override.
async fn delete_table_override(
    State(state): State<AppState>,
    Path((name, table)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    // Verify environment exists.
    state
        .environment_store
        .get(&name)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("environment", &name))?;

    let removed = state
        .environment_store
        .deregister_table_override(&name, "public", &table)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found("table override", &table))
    }
}

/// `GET /api/environments/resolve/:table` — show resolution chain for a table.
///
/// Walks the fallback chain from `dev` (or a query-param environment) and
/// reports which environments have an override for the given table.
async fn resolve_table(
    State(state): State<AppState>,
    Path(table): Path<String>,
    axum::extract::Query(params): axum::extract::Query<ResolveQuery>,
) -> Result<Json<ResolveResponse>, (StatusCode, Json<ApiError>)> {
    let env_name = params.environment.as_deref().unwrap_or("dev");

    let chain = state
        .environment_store
        .fallback_chain(env_name)
        .map_err(|e| match &e {
            EnvironmentError::NotFound(_) => ApiError::not_found("environment", env_name),
            _ => ApiError::internal(e.to_string()),
        })?;

    let mut entries = Vec::with_capacity(chain.len());
    for env in &chain {
        let overrides = state
            .environment_store
            .list_table_overrides(env)
            .map_err(|e| ApiError::internal(e.to_string()))?;
        let has_override = overrides
            .iter()
            .any(|o| o.table_name == table && o.schema_name == "public");
        entries.push(ResolveEntry {
            environment: env.clone(),
            has_override,
        });
    }

    Ok(Json(ResolveResponse {
        table,
        chain: entries,
    }))
}

#[derive(Debug, Deserialize)]
struct ResolveQuery {
    environment: Option<String>,
}
