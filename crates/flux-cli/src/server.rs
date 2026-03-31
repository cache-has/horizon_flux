// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI commands for managing the Horizon Flux server process.

use std::net::IpAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Subcommand;
use serde::Serialize;

use crate::OutputFormat;

#[derive(Subcommand)]
pub enum ServerAction {
    /// Start the Horizon Flux server (default if no command is given).
    Start {
        /// Port number for the web server.
        #[arg(long, short, default_value_t = 8080)]
        port: u16,

        /// Address to bind to (default: 127.0.0.1, use 0.0.0.0 for Docker).
        #[arg(long, default_value = "127.0.0.1", env = "HORIZON_FLUX_HOST")]
        host: IpAddr,

        /// Start without opening the browser.
        #[arg(long)]
        headless: bool,

        /// Proxy frontend requests to the Vite dev server.
        #[arg(long)]
        dev: bool,
    },
    /// Stop a running server instance.
    Stop,
    /// Show server status (running, port, PID).
    Status,
}

#[derive(Serialize)]
struct StatusOutput {
    running: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
}

pub fn handle(action: ServerAction, format: OutputFormat) -> Result<()> {
    match action {
        ServerAction::Start {
            port,
            host,
            headless,
            dev,
        } => start(port, host, headless, dev, None),
        ServerAction::Stop => stop(format),
        ServerAction::Status => status(format),
    }
}

/// Start the server — extracted from the previous default path in main.
pub fn start(
    port: u16,
    host: IpAddr,
    headless: bool,
    dev: bool,
    metadata_url: Option<&str>,
) -> Result<()> {
    let config = flux_server::ServerConfig {
        host,
        port_start: port,
        open_browser: !headless,
        dev_mode: dev,
        ..Default::default()
    };

    let data_dir = crate::config::data_dir()?;
    let backend = crate::config::MetadataBackend::resolve(metadata_url, &data_dir)?;
    let stores = crate::config::open_stores(&backend, &data_dir)?;

    let pipeline_store = stores.pipeline_store;
    let run_store = stores.run_store;
    let connector_registry = Arc::new(flux_connectors::default_registry());
    let environment_store = stores.environment_store;

    let secrets_path = data_dir.join("secrets.db");
    let secret_session = match std::env::var("HORIZON_FLUX_SECRET_PASSWORD") {
        Ok(password) if !password.is_empty() => {
            match flux_secrets::SecretStore::open_or_init(&secrets_path, password.as_bytes()) {
                Ok(store) => flux_server::state::SecretSession::new_unlocked(store, secrets_path),
                Err(e) => {
                    tracing::warn!("Could not open secret store: {e}");
                    flux_server::state::SecretSession::new(secrets_path)
                }
            }
        }
        _ => flux_server::state::SecretSession::new(secrets_path),
    };

    let event_tx = flux_server::AppState::new_event_channel();

    #[cfg(feature = "tray")]
    let tray_handle = flux_tray::spawn(
        flux_tray::TrayConfig {
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        event_tx.subscribe(),
    );

    let output_cache = Arc::new(flux_datafusion::OutputCache::new(&data_dir));

    let metadata_info = flux_server::state::MetadataInfo {
        backend: match &backend {
            crate::config::MetadataBackend::Sqlite => "sqlite".to_string(),
            crate::config::MetadataBackend::Postgresql { .. } => "postgresql".to_string(),
        },
        data_dir: data_dir.clone(),
        connection_string: match &backend {
            crate::config::MetadataBackend::Postgresql { connection_string } => Some(
                flux_server::state::redact_connection_string(connection_string),
            ),
            _ => None,
        },
        config_source: backend.display_source(metadata_url, &data_dir).to_string(),
    };

    let app_state = flux_server::AppState {
        pipeline_store,
        run_store,
        connector_registry,
        environment_store,
        secret_session: Arc::new(std::sync::Mutex::new(secret_session)),
        event_tx,
        output_cache,
        session_factory: Some(Arc::new(flux_datafusion::SessionFactory::default())),
        metadata_info,
    };

    #[cfg(feature = "tray")]
    let on_ready: Option<Box<dyn FnOnce(u16) + Send>> = match &tray_handle {
        Some(handle) => {
            let cmd_tx = handle.cmd_sender();
            Some(Box::new(move |port| {
                let url = format!("http://localhost:{port}");
                let _ = cmd_tx.send(flux_tray::TrayCommand::SetUrl(url));
            }))
        }
        None => None,
    };
    #[cfg(not(feature = "tray"))]
    let on_ready: Option<Box<dyn FnOnce(u16) + Send>> = None;

    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    rt.block_on(flux_server::serve(config, app_state, on_ready))
        .context("server failed")?;

    #[cfg(feature = "tray")]
    if let Some(handle) = tray_handle {
        handle.shutdown();
    }

    Ok(())
}

fn stop(format: OutputFormat) -> Result<()> {
    let lock_path =
        flux_server::lockfile::default_path().context("could not determine lockfile path")?;

    let info =
        flux_server::lockfile::check_existing(&lock_path).context("failed to read lockfile")?;

    match info {
        Some(instance) => {
            send_sigterm(instance.pid)?;
            // Clean up the lockfile after signaling.
            flux_server::lockfile::remove(&lock_path);
            match format {
                OutputFormat::Human => {
                    println!(
                        "Stopped Horizon Flux server (PID {}, port {})",
                        instance.pid, instance.port
                    );
                }
                OutputFormat::Json => {
                    let out = serde_json::json!({
                        "stopped": true,
                        "pid": instance.pid,
                        "port": instance.port,
                    });
                    println!("{}", serde_json::to_string_pretty(&out)?);
                }
            }
        }
        None => match format {
            OutputFormat::Human => {
                println!("No running Horizon Flux server found.");
            }
            OutputFormat::Json => {
                let out = serde_json::json!({ "stopped": false, "reason": "not running" });
                println!("{}", serde_json::to_string_pretty(&out)?);
            }
        },
    }
    Ok(())
}

fn status(format: OutputFormat) -> Result<()> {
    let lock_path =
        flux_server::lockfile::default_path().context("could not determine lockfile path")?;

    let info =
        flux_server::lockfile::check_existing(&lock_path).context("failed to read lockfile")?;

    let output = match info {
        Some(instance) => StatusOutput {
            running: true,
            pid: Some(instance.pid),
            port: Some(instance.port),
        },
        None => StatusOutput {
            running: false,
            pid: None,
            port: None,
        },
    };

    match format {
        OutputFormat::Human => {
            if output.running {
                println!("Horizon Flux is running");
                println!("  PID:  {}", output.pid.unwrap());
                println!("  Port: {}", output.port.unwrap());
            } else {
                println!("Horizon Flux is not running.");
            }
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
    }

    Ok(())
}

/// Send SIGTERM to a process.
fn send_sigterm(pid: u32) -> Result<()> {
    #[cfg(unix)]
    {
        // SAFETY: SIGTERM is a standard termination signal.
        let ret = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            anyhow::bail!("failed to send SIGTERM to PID {pid}: {err}");
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        anyhow::bail!("stopping the server is only supported on Unix systems");
    }
}
