// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Embedded static file serving for the React frontend.
//!
//! In release builds the frontend assets are compiled into the binary.
//! In debug builds `rust-embed` loads them from disk, so changes to
//! `frontend/dist/` are picked up without recompilation.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use rust_embed::Embed;

/// Embedded frontend assets from `frontend/dist/`.
#[derive(Embed)]
#[folder = "../../frontend/dist/"]
pub struct FrontendAssets;

/// Serve an embedded static file by path, with appropriate cache headers.
///
/// - `index.html`: `Cache-Control: no-cache` (always revalidate so deploys
///   pick up new asset hashes immediately).
/// - `assets/*` (content-hashed filenames): `Cache-Control: public,
///   max-age=31536000, immutable` — safe to cache forever since the hash
///   changes when the content does.
/// - Other files (favicon, icons): short cache with revalidation.
pub fn serve_embedded(path: &str) -> Response {
    match FrontendAssets::get(path) {
        Some(file) => {
            let mime = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();

            let cache_control = if path == "index.html" {
                "no-cache"
            } else if path.starts_with("assets/") {
                "public, max-age=31536000, immutable"
            } else {
                "public, max-age=3600, must-revalidate"
            };

            (
                [
                    (header::CONTENT_TYPE, mime),
                    (header::CACHE_CONTROL, cache_control.to_string()),
                ],
                file.data,
            )
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}

/// Handler for static asset requests (`/{*path}`).
pub async fn static_handler(axum::extract::Path(path): axum::extract::Path<String>) -> Response {
    serve_embedded(&path)
}

/// SPA fallback: serves `index.html` for any unmatched route so that
/// client-side routing works on page refresh.
pub async fn spa_fallback() -> Response {
    serve_embedded("index.html")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn index_html_exists_and_has_no_cache() {
        let resp = serve_embedded("index.html");
        assert_eq!(resp.status(), StatusCode::OK);
        let cache = resp.headers().get(header::CACHE_CONTROL).unwrap();
        assert_eq!(cache, "no-cache");
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap();
        assert!(ct.to_str().unwrap().contains("text/html"));
    }

    #[test]
    fn hashed_asset_gets_immutable_cache() {
        // Find any file under assets/ in the embedded data.
        let asset_path = FrontendAssets::iter()
            .find(|p| p.starts_with("assets/"))
            .expect("at least one hashed asset should be embedded");
        let resp = serve_embedded(&asset_path);
        assert_eq!(resp.status(), StatusCode::OK);
        let cache = resp
            .headers()
            .get(header::CACHE_CONTROL)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(cache.contains("immutable"));
        assert!(cache.contains("max-age=31536000"));
    }

    #[test]
    fn missing_file_returns_404() {
        let resp = serve_embedded("nonexistent.xyz");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
