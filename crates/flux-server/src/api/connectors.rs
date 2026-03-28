// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Connector discovery and testing API routes.

use crate::api::ApiError;
use crate::state::AppState;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use flux_engine::node::{SinkConfig, SourceConfig};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tracing::debug;

/// Build the `/connectors` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_connectors))
        .route("/test", post(test_connector))
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ConnectorInfo {
    name: String,
    roles: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct ConnectorsResponse {
    connectors: Vec<ConnectorInfo>,
}

#[derive(Debug, Deserialize)]
struct TestRequest {
    /// `"source"` or `"sink"`.
    role: String,
    /// Connector type name (e.g. `"csv"`, `"postgresql"`).
    connector: String,
    /// Connector-specific configuration JSON.
    #[serde(default)]
    config: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct TestResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /api/connectors` — list available source and sink connectors.
async fn list_connectors(State(state): State<AppState>) -> Json<ConnectorsResponse> {
    let registry = &state.connector_registry;

    // Build a deduplicated map of name → roles.
    let mut role_map: BTreeMap<String, Vec<&'static str>> = BTreeMap::new();

    for name in registry.source_names() {
        role_map.entry(name.to_string()).or_default().push("source");
    }
    for name in registry.sink_names() {
        role_map.entry(name.to_string()).or_default().push("sink");
    }

    let connectors = role_map
        .into_iter()
        .map(|(name, roles)| ConnectorInfo { name, roles })
        .collect();

    Json(ConnectorsResponse { connectors })
}

/// `POST /api/connectors/test` — test a connector configuration.
///
/// For sources, attempts to create a `TableProvider` which validates the
/// configuration and (for database connectors) tests connectivity.
/// For sinks, runs config validation.
async fn test_connector(
    State(state): State<AppState>,
    Json(req): Json<TestRequest>,
) -> Result<Json<TestResponse>, (StatusCode, Json<ApiError>)> {
    let registry = &state.connector_registry;

    match req.role.as_str() {
        "source" => {
            let source = registry.get_source(&req.connector).ok_or_else(|| {
                ApiError::bad_request(format!("unknown source connector: {}", req.connector))
            })?;

            let source_config = SourceConfig {
                connector: req.connector.clone(),
                config: req.config,
            };

            debug!(connector = %req.connector, "testing source connector");

            match source.create_table_provider(&source_config) {
                Ok(_) => Ok(Json(TestResponse {
                    success: true,
                    error: None,
                })),
                Err(e) => Ok(Json(TestResponse {
                    success: false,
                    error: Some(e.to_string()),
                })),
            }
        }
        "sink" => {
            let sink = registry.get_sink(&req.connector).ok_or_else(|| {
                ApiError::bad_request(format!("unknown sink connector: {}", req.connector))
            })?;

            let sink_config = SinkConfig {
                connector: req.connector.clone(),
                config: req.config,
            };

            debug!(connector = %req.connector, "testing sink connector");

            match sink.validate_config(&sink_config) {
                Ok(()) => Ok(Json(TestResponse {
                    success: true,
                    error: None,
                })),
                Err(e) => Ok(Json(TestResponse {
                    success: false,
                    error: Some(e.to_string()),
                })),
            }
        }
        _ => Err(ApiError::bad_request("role must be \"source\" or \"sink\"")),
    }
}
