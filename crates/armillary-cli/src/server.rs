// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI commands for managing the Armillary server process.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use armillary_scheduler::{Clock, RunDispatcher, Scheduler, SystemClock};
use clap::Subcommand;
use serde::Serialize;

use crate::OutputFormat;

#[derive(Subcommand)]
pub enum ServerAction {
    /// Start the Armillary server (default if no command is given).
    Start {
        /// Port number for the web server.
        #[arg(long, short, default_value_t = 8080)]
        port: u16,

        /// Address to bind to (default: 127.0.0.1, use 0.0.0.0 for Docker).
        #[arg(long, default_value = "127.0.0.1", env = "ARMILLARY_HOST")]
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

/// A no-op run dispatcher used during server startup. The real dispatch
/// happens via the executor; this placeholder satisfies the `Scheduler`
/// constructor until the full dispatch loop is wired (doc 32 — API tasks).
struct NoopDispatcher;

impl RunDispatcher for NoopDispatcher {
    fn dispatch_run(
        &self,
        _pipeline_id: &str,
        _environment: &str,
        _variables: Option<&HashMap<String, serde_json::Value>>,
        _trigger_id: &str,
    ) -> Result<String, String> {
        Err("dispatch not yet wired".to_string())
    }

    fn is_pipeline_running(&self, _pipeline_id: &str, _environment: &str) -> bool {
        false
    }
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
    let config = armillary_server::ServerConfig {
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
    let incremental_state_store = stores.incremental_state_store;
    let lineage_store = stores.lineage_store;
    let trigger_store = stores.trigger_store;
    let backfill_store = stores.backfill_store;
    let column_lineage_store = stores.column_lineage_store;
    let sla_store = stores.sla_store;
    let connector_registry = Arc::new(armillary_connectors::default_registry());
    let environment_store = stores.environment_store;

    let secrets_path = data_dir.join("secrets.db");
    let secret_session = match std::env::var("ARMILLARY_SECRET_PASSWORD") {
        Ok(password) if !password.is_empty() => {
            match armillary_secrets::SecretStore::open_or_init(&secrets_path, password.as_bytes()) {
                Ok(store) => {
                    armillary_server::state::SecretSession::new_unlocked(store, secrets_path)
                }
                Err(e) => {
                    tracing::warn!("Could not open secret store: {e}");
                    armillary_server::state::SecretSession::new(secrets_path)
                }
            }
        }
        _ => armillary_server::state::SecretSession::new(secrets_path),
    };

    let event_tx = armillary_server::AppState::new_event_channel();

    let plugin_cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    let plugin_registry = Arc::new(std::sync::RwLock::new(Arc::new(
        armillary_plugin_host::discover_plugins(&plugin_cwd),
    )));

    #[cfg(feature = "tray")]
    let tray_handle = armillary_tray::spawn(
        armillary_tray::TrayConfig {
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        event_tx.subscribe(),
    );

    let output_cache = Arc::new(armillary_datafusion::OutputCache::new(&data_dir));

    let metadata_info = armillary_server::state::MetadataInfo {
        backend: match &backend {
            crate::config::MetadataBackend::Sqlite => "sqlite".to_string(),
            crate::config::MetadataBackend::Postgresql { .. } => "postgresql".to_string(),
        },
        data_dir: data_dir.clone(),
        connection_string: match &backend {
            crate::config::MetadataBackend::Postgresql { connection_string } => Some(
                armillary_server::state::redact_connection_string(connection_string),
            ),
            _ => None,
        },
        config_source: backend.display_source(metadata_url, &data_dir).to_string(),
    };

    // Build the scheduler for trigger evaluation (sensors, webhooks,
    // pipeline-completion). The NoopDispatcher is a placeholder until the
    // full trigger → executor dispatch path is wired in the API tasks.
    let scheduler = Arc::new(Scheduler::new(
        trigger_store.clone(),
        Arc::new(SystemClock) as Arc<dyn Clock>,
        Arc::new(NoopDispatcher) as Arc<dyn RunDispatcher>,
    ));

    // Build the OpenLineage client from config (if enabled).
    let openlineage_client = crate::config::resolve_openlineage_config(&data_dir)
        .and_then(|cfg| armillary_observability::openlineage::OpenLineageClient::new(&cfg));

    let app_state = armillary_server::AppState {
        pipeline_store,
        run_store: run_store.clone(),
        incremental_state_store,
        lineage_store,
        connector_registry,
        environment_store,
        secret_session: Arc::new(std::sync::Mutex::new(secret_session)),
        event_tx: event_tx.clone(),
        plugin_event_tx: armillary_server::AppState::new_plugin_event_channel(),
        output_cache,
        session_factory: Some(Arc::new(armillary_datafusion::SessionFactory::default())),
        metadata_info,
        plugin_registry,
        backfill_store,
        trigger_store,
        scheduler: Some(scheduler.clone()),
        plugin_cwd: plugin_cwd.clone(),
        plugin_scan_roots: None,
        metadata_dir: Some(plugin_cwd.join("metadata")),
        catalog_event_tx: armillary_server::AppState::new_catalog_event_channel(),
        column_lineage_store,
        column_lineage_event_tx: armillary_server::AppState::new_column_lineage_event_channel(),
        openlineage_client,
        sla_store,
    };

    #[cfg(feature = "tray")]
    let on_ready: Option<Box<dyn FnOnce(u16) + Send>> = match &tray_handle {
        Some(handle) => {
            let cmd_tx = handle.cmd_sender();
            Some(Box::new(move |port| {
                let url = format!("http://localhost:{port}");
                let _ = cmd_tx.send(armillary_tray::TrayCommand::SetUrl(url));
            }))
        }
        None => None,
    };
    #[cfg(not(feature = "tray"))]
    let on_ready: Option<Box<dyn FnOnce(u16) + Send>> = None;

    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    rt.block_on(async {
        // Spawn the scheduler tick loop (evaluates cron, interval, and
        // file-arrival triggers every 15 seconds).
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let sla_shutdown_rx = shutdown_rx.clone();
        let sched = scheduler.clone();
        tokio::spawn(async move {
            armillary_scheduler::run_scheduler_loop(
                sched,
                std::time::Duration::from_secs(15),
                shutdown_rx,
            )
            .await;
        });

        // Spawn the SLA evaluator: periodically checks resource freshness
        // against declared SLAs and emits events/metrics (planning doc 37).
        let sla_state = app_state.clone();
        tokio::spawn(async move {
            armillary_server::sla_evaluator::run_sla_evaluator_loop(
                sla_state,
                std::time::Duration::from_secs(60),
                sla_shutdown_rx,
            )
            .await;
        });

        // Spawn the pipeline-completion listener: subscribes to execution
        // events and notifies the scheduler when a run finishes so that
        // PipelineCompletion triggers can fire.
        let completion_scheduler = scheduler.clone();
        let completion_run_store = run_store.clone();
        let mut event_rx = event_tx.subscribe();
        tokio::spawn(async move {
            use armillary_datafusion::ExecutionEvent;
            loop {
                match event_rx.recv().await {
                    Ok(ExecutionEvent::RunCompleted { run_id, status, .. }) => {
                        // Look up pipeline name and environment from the run store.
                        if let Ok(Some(run)) = completion_run_store.get_run(&run_id) {
                            let status_str = status.as_str();
                            completion_scheduler.notify_run_completed(
                                &run.pipeline_name,
                                &run.environment,
                                status_str,
                            );
                        }
                    }
                    Ok(_) => {} // Ignore other events.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("completion listener lagged by {n} events");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        let result = armillary_server::serve(config, app_state, on_ready).await;

        // Signal scheduler shutdown.
        let _ = shutdown_tx.send(true);

        result
    })
    .context("server failed")?;

    #[cfg(feature = "tray")]
    if let Some(handle) = tray_handle {
        handle.shutdown();
    }

    Ok(())
}

fn stop(format: OutputFormat) -> Result<()> {
    let lock_path =
        armillary_server::lockfile::default_path().context("could not determine lockfile path")?;

    let info = armillary_server::lockfile::check_existing(&lock_path)
        .context("failed to read lockfile")?;

    match info {
        Some(instance) => {
            send_sigterm(instance.pid)?;
            // Clean up the lockfile after signaling.
            armillary_server::lockfile::remove(&lock_path);
            match format {
                OutputFormat::Human => {
                    println!(
                        "Stopped Armillary server (PID {}, port {})",
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
                println!("No running Armillary server found.");
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
        armillary_server::lockfile::default_path().context("could not determine lockfile path")?;

    let info = armillary_server::lockfile::check_existing(&lock_path)
        .context("failed to read lockfile")?;

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
                println!("Armillary is running");
                println!("  PID:  {}", output.pid.unwrap());
                println!("  Port: {}", output.port.unwrap());
            } else {
                println!("Armillary is not running.");
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
        let _ = pid;
        anyhow::bail!("stopping the server is only supported on Unix systems");
    }
}
