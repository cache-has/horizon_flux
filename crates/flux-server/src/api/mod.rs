// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! API route modules and shared types.

pub mod backfills;
pub mod catalog;
pub mod connectors;
pub mod environments;
pub mod lineage;
pub mod metrics;
pub mod pipelines;
pub mod plugins;
pub mod preview;
pub mod secrets;
pub mod snapshot;
pub mod system;
pub mod triggers;
pub mod webhook;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// Consistent JSON error response: `{ "error": "...", "details": "..." }`.
#[derive(Debug, Serialize)]
pub struct ApiError {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl ApiError {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            details: None,
        }
    }

    pub fn with_details(error: impl Into<String>, details: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            details: Some(details.into()),
        }
    }

    pub fn not_found(resource: &str, id: &str) -> (StatusCode, Json<Self>) {
        (
            StatusCode::NOT_FOUND,
            Json(Self::new(format!("{resource} `{id}` not found"))),
        )
    }

    pub fn bad_request(message: impl Into<String>) -> (StatusCode, Json<Self>) {
        (StatusCode::BAD_REQUEST, Json(Self::new(message)))
    }

    pub fn conflict(message: impl Into<String>) -> (StatusCode, Json<Self>) {
        (StatusCode::CONFLICT, Json(Self::new(message)))
    }

    pub fn internal(message: impl Into<String>) -> (StatusCode, Json<Self>) {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(Self::new(message)))
    }

    pub fn gateway_timeout(message: impl Into<String>) -> (StatusCode, Json<Self>) {
        (StatusCode::GATEWAY_TIMEOUT, Json(Self::new(message)))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(self)).into_response()
    }
}

/// Pagination query parameters.
#[derive(Debug, serde::Deserialize)]
pub struct Pagination {
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

fn default_limit() -> u32 {
    50
}

/// Paginated list response wrapper.
#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T: Serialize> {
    pub data: Vec<T>,
    pub total: u32,
    pub limit: u32,
    pub offset: u32,
}
