// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Axum web server for Horizon Flux.
//!
//! Provides HTTP/WebSocket server that serves the React frontend and
//! exposes API routes for pipeline management. Handles single-instance
//! detection via lockfile and auto-opens the browser.

pub mod api;
pub mod dev_proxy;
pub mod error;
pub mod lockfile;
pub mod port;
pub mod shutdown;
pub mod state;
pub mod static_files;
pub mod ws;

pub use error::ServerError;
pub use state::AppState;

use std::process;

use axum::Router;
use axum::http::HeaderValue;
use axum::routing::get;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::info;

/// Configuration for the web server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Starting port number (default: 8080).
    pub port_start: u16,
    /// Ceiling for port scanning, exclusive (default: 8180).
    pub port_ceiling: u16,
    /// Whether to auto-open the browser.
    pub open_browser: bool,
    /// When true, proxy frontend requests to the Vite dev server instead
    /// of serving embedded static files.
    pub dev_mode: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port_start: port::DEFAULT_PORT,
            port_ceiling: port::DEFAULT_PORT_CEILING,
            open_browser: true,
            dev_mode: false,
        }
    }
}

/// Build the Axum router with frontend serving configured.
fn build_router(config: &ServerConfig, app_state: AppState) -> Router {
    let api_routes = Router::new()
        .nest("/pipelines", api::pipelines::router())
        .nest("/connectors", api::connectors::router())
        .nest("/preview", api::preview::router())
        .nest("/environments", api::environments::router())
        .nest("/secrets", api::secrets::router());

    // CORS: allow localhost origins for single-user mode.
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(|origin: &HeaderValue, _| {
            origin
                .to_str()
                .map(|s| s.starts_with("http://localhost") || s.starts_with("http://127.0.0.1"))
                .unwrap_or(false)
        }))
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .nest("/api", api_routes)
        .route("/ws", get(ws::ws_handler))
        .with_state(app_state)
        .layer(cors);

    if config.dev_mode {
        info!(
            "Dev mode: proxying frontend requests to Vite at {}",
            dev_proxy::DEFAULT_VITE_ORIGIN
        );
        app.fallback(get(dev_proxy::vite_proxy).post(dev_proxy::vite_proxy))
    } else {
        app.route("/{*path}", get(static_files::static_handler))
            .fallback(get(static_files::spa_fallback))
    }
}

/// Start the Horizon Flux web server.
///
/// 1. Checks for an existing running instance (opens browser + exits)
/// 2. Finds an available port and binds the listener
/// 3. Creates a lockfile
/// 4. Opens the browser
/// 5. Serves until shutdown signal
/// 6. Cleans up lockfile via RAII guard
pub async fn serve(
    config: ServerConfig,
    app_state: AppState,
    on_ready: Option<Box<dyn FnOnce(u16) + Send>>,
) -> Result<(), ServerError> {
    let lock_path = lockfile::default_path()?;

    // --- Instance detection ---
    if let Some(existing) = lockfile::check_existing(&lock_path)? {
        let url = format!("http://localhost:{}", existing.port);
        info!(
            "Existing instance found (PID {}, port {})",
            existing.pid, existing.port
        );
        println!("Horizon Flux is already running at {url}");
        if config.open_browser {
            let _ = open::that(&url);
        }
        return Ok(());
    }

    // --- Port selection + bind ---
    let (listener, port) = port::find_and_bind(config.port_start, config.port_ceiling).await?;

    // --- Lockfile ---
    let info = lockfile::InstanceInfo {
        pid: process::id(),
        port,
    };
    lockfile::write(&lock_path, &info)?;
    let _guard = shutdown::LockfileGuard::new(lock_path);

    // --- Build router ---
    let app = build_router(&config, app_state);

    let url = format!("http://localhost:{port}");
    info!("Horizon Flux listening on {url}");
    println!("Horizon Flux is running at {url}");

    if let Some(cb) = on_ready {
        cb(port);
    }

    // --- Open browser ---
    if config.open_browser {
        if let Err(e) = open::that(&url) {
            tracing::warn!("Could not open browser: {e}");
            println!("Open {url} in your browser");
        }
    }

    // --- Serve with graceful shutdown ---
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::shutdown_signal())
        .await
        .map_err(|e| ServerError::Serve(e.to_string()))?;

    info!("Server shut down gracefully");
    Ok(())
}

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
