// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Plugin discovery and inspection endpoints.
//!
//! Routes (mounted under `/api/plugins`):
//!
//! - `GET /`                          — list discovered plugins + status.
//! - `GET /:name/sinks/:type/schema`  — return a sink's JSON Schema for the
//!   config form renderer.
//! - `POST /reload`                   — rescan plugin directories.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Router, response::IntoResponse};
use flux_plugin_host::{DiscoveredPlugin, PluginStatus, discover_plugins};
use serde::Serialize;
use serde_json::Value;
use tracing::info;

use crate::api::ApiError;
use crate::state::AppState;

#[derive(Debug, Serialize)]
struct PluginListResponse {
    plugins: Vec<PluginEntry>,
}

#[derive(Debug, Serialize)]
struct PluginEntry {
    name: String,
    directory: String,
    status: PluginStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    manifest: Option<flux_plugin_host::Manifest>,
}

impl From<&DiscoveredPlugin> for PluginEntry {
    fn from(p: &DiscoveredPlugin) -> Self {
        Self {
            name: p.name.clone(),
            directory: p.directory.display().to_string(),
            status: p.status.clone(),
            manifest: p.manifest.clone(),
        }
    }
}

async fn list_plugins(State(state): State<AppState>) -> Json<PluginListResponse> {
    let registry = {
        let guard = state.plugin_registry.read().expect("plugin registry poisoned");
        Arc::clone(&*guard)
    };
    let plugins = registry.iter().map(PluginEntry::from).collect();
    Json(PluginListResponse { plugins })
}

async fn get_sink_schema(
    State(state): State<AppState>,
    Path((name, sink_type)): Path<(String, String)>,
) -> Result<Json<Value>, (StatusCode, Json<ApiError>)> {
    let registry = {
        let guard = state.plugin_registry.read().expect("plugin registry poisoned");
        Arc::clone(&*guard)
    };
    let plugin = registry
        .get(&name)
        .ok_or_else(|| ApiError::not_found("plugin", &name))?;

    let manifest = plugin.manifest.as_ref().ok_or_else(|| {
        ApiError::bad_request(format!("plugin `{name}` has no valid manifest"))
    })?;

    let sink = manifest
        .sinks
        .iter()
        .find(|s| s.ty == sink_type)
        .ok_or_else(|| ApiError::not_found("sink", &sink_type))?;

    let schema_path = plugin.directory.join(&sink.config_schema);
    let bytes = std::fs::read(&schema_path).map_err(|e| {
        ApiError::internal(format!(
            "failed to read schema file `{}`: {e}",
            schema_path.display()
        ))
    })?;
    let json: Value = serde_json::from_slice(&bytes).map_err(|e| {
        ApiError::internal(format!(
            "schema file `{}` is not valid JSON: {e}",
            schema_path.display()
        ))
    })?;
    Ok(Json(json))
}

async fn reload_plugins(State(state): State<AppState>) -> impl IntoResponse {
    info!("reloading plugin registry");
    let new_registry = Arc::new(discover_plugins(&state.plugin_cwd));
    let count = new_registry.len();
    {
        let mut guard = state.plugin_registry.write().expect("plugin registry poisoned");
        *guard = new_registry;
    }
    Json(serde_json::json!({ "reloaded": true, "count": count }))
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_plugins))
        .route("/{name}/sinks/{sink_type}/schema", get(get_sink_schema))
        .route("/reload", post(reload_plugins))
}
