// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Prometheus metrics endpoint.
//!
//! Serves the `/metrics` path in Prometheus exposition format, scraped by
//! Prometheus, Datadog Agent, Grafana Agent, or any compatible collector.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// `GET /metrics` — render all Prometheus metrics.
pub async fn metrics_handler() -> Response {
    match armillary_observability::metrics::render() {
        Some(body) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        None => (StatusCode::SERVICE_UNAVAILABLE, "Metrics not enabled").into_response(),
    }
}
