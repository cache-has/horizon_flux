// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Reverse proxy to the Vite dev server for frontend development.
//!
//! When the server runs in dev mode, non-API requests are forwarded to the
//! Vite dev server (default `http://localhost:5173`) instead of serving
//! embedded static files. This gives developers HMR and fast refresh.

use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Default Vite dev server origin.
pub const DEFAULT_VITE_ORIGIN: &str = "http://localhost:5173";

/// Proxy an incoming request to the Vite dev server.
///
/// Forwards the method, path, query string, and body. Returns the Vite
/// response with its status, headers, and body intact.
pub async fn vite_proxy(request: Request) -> Response {
    let vite_origin = DEFAULT_VITE_ORIGIN;
    let uri = request.uri().clone();
    let path_and_query = uri.path_and_query().map_or("/", |pq| pq.as_str());
    let url = format!("{vite_origin}{path_and_query}");

    let client = reqwest::Client::new();
    let method = reqwest::Method::from_bytes(request.method().as_str().as_bytes())
        .unwrap_or(reqwest::Method::GET);

    let body = match axum::body::to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("dev proxy: failed to read request body: {e}");
            return (StatusCode::BAD_REQUEST, "Failed to read request body").into_response();
        }
    };

    match client.request(method, &url).body(body).send().await {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let mut builder = axum::http::Response::builder().status(status);
            for (name, value) in resp.headers() {
                if let (Ok(n), Ok(v)) = (
                    axum::http::header::HeaderName::from_bytes(name.as_str().as_bytes()),
                    axum::http::header::HeaderValue::from_bytes(value.as_bytes()),
                ) {
                    builder = builder.header(n, v);
                }
            }
            match resp.bytes().await {
                Ok(bytes) => builder
                    .body(axum::body::Body::from(bytes))
                    .unwrap_or_else(|_| {
                        (StatusCode::INTERNAL_SERVER_ERROR, "proxy error").into_response()
                    }),
                Err(e) => {
                    tracing::warn!("dev proxy: failed to read response body: {e}");
                    (StatusCode::BAD_GATEWAY, "Vite proxy read error").into_response()
                }
            }
        }
        Err(e) => {
            tracing::warn!("dev proxy: could not reach Vite at {url}: {e}");
            (
                StatusCode::BAD_GATEWAY,
                "Could not reach Vite dev server. Is it running on port 5173?",
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_vite_origin_is_correct() {
        assert_eq!(DEFAULT_VITE_ORIGIN, "http://localhost:5173");
    }
}
