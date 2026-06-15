// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! System information endpoint.

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::state::AppState;

#[derive(Debug, Serialize)]
struct SystemInfo {
    /// Server version from Cargo.toml.
    version: &'static str,
    /// Metadata backend: `"sqlite"` or `"postgresql"`.
    metadata_backend: String,
    /// Data directory path.
    data_dir: String,
    /// How the backend was configured.
    config_source: String,
    /// Redacted connection string (PostgreSQL only).
    #[serde(skip_serializing_if = "Option::is_none")]
    connection_string: Option<String>,
}

async fn get_system_info(State(state): State<AppState>) -> Json<SystemInfo> {
    let info = &state.metadata_info;
    Json(SystemInfo {
        version: crate::version(),
        metadata_backend: info.backend.clone(),
        data_dir: info.data_dir.display().to_string(),
        config_source: info.config_source.clone(),
        connection_string: info.connection_string.clone(),
    })
}

pub fn router() -> Router<AppState> {
    Router::new().route("/info", get(get_system_info))
}
