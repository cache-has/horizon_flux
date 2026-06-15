// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Axum web server for Armillary.
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
pub mod sla_evaluator;
pub mod state;
pub mod static_files;
pub mod ws;

pub use error::ServerError;
pub use state::{AppState, CatalogEvent, ColumnLineageEvent, PluginEvent};

use std::net::IpAddr;
use std::process;

use axum::Router;
use axum::http::HeaderValue;
use axum::routing::get;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::info;

/// Configuration for the web server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind to (default: 127.0.0.1).
    pub host: IpAddr,
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
            host: port::DEFAULT_HOST,
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
        .nest(
            "/pipelines",
            api::pipelines::router().merge(api::snapshot::router()),
        )
        .nest("/connectors", api::connectors::router())
        .nest("/preview", api::preview::router())
        .nest("/environments", api::environments::router())
        .nest("/secrets", api::secrets::router())
        .nest("/system", api::system::router())
        .nest("/plugins", api::plugins::router())
        .nest("/lineage", api::lineage::router())
        .nest("/triggers", api::triggers::router())
        .nest("/backfills", api::backfills::router())
        .nest("/catalog", api::catalog::router())
        .nest("/runs", api::runs::router())
        .nest("/sla", api::sla::router())
        .nest("/health", api::health::router());

    // Webhook trigger endpoints live outside /api — they're called by
    // external systems, not the frontend.
    let webhook_routes = Router::new().nest("/webhook", api::webhook::router());

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
        .nest("/triggers", webhook_routes)
        .route("/ws", get(ws::ws_handler))
        .route("/metrics", get(api::metrics::metrics_handler))
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

/// Start the Armillary web server.
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
        println!("Armillary is already running at {url}");
        if config.open_browser {
            let _ = open::that(&url);
        }
        return Ok(());
    }

    // --- Port selection + bind ---
    let (listener, port) =
        port::find_and_bind(config.host, config.port_start, config.port_ceiling).await?;

    // --- Lockfile ---
    let info = lockfile::InstanceInfo {
        pid: process::id(),
        port,
    };
    lockfile::write(&lock_path, &info)?;
    let _guard = shutdown::LockfileGuard::new(lock_path);

    // --- Build router ---
    let app = build_router(&config, app_state);

    let display_host = if config.host.is_unspecified() {
        "localhost"
    } else {
        ""
    };
    let url = if display_host.is_empty() {
        format!("http://{}:{port}", config.host)
    } else {
        format!("http://{display_host}:{port}")
    };
    info!("Armillary listening on {}:{port}", config.host);
    println!("Armillary is running at {url}");

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
