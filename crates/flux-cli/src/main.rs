// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

mod secret;

#[derive(Parser)]
#[command(
    name = "horizon-flux",
    version,
    about = "Horizon Flux — visual data pipeline builder"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Starting port number for the web server.
    #[arg(long, default_value_t = 8080)]
    port: u16,

    /// Do not auto-open the browser.
    #[arg(long)]
    no_browser: bool,

    /// Proxy frontend requests to the Vite dev server instead of serving
    /// embedded static files.
    #[arg(long)]
    dev: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Manage encrypted secrets.
    Secret {
        #[command(subcommand)]
        action: secret::SecretAction,
    },
    /// Export a pipeline definition to a JSON file.
    Export {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Output file path (defaults to `{pipeline_name}.json` in the current directory).
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },
    /// Import a pipeline definition from a JSON file.
    Import {
        /// Path to the JSON pipeline file.
        file: std::path::PathBuf,
        /// How to handle name conflicts: reject, rename, or overwrite.
        #[arg(long, default_value = "reject")]
        on_conflict: String,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Some(Command::Secret { action }) => {
            secret::handle(action).context("secret command failed")?;
        }
        Some(Command::Export { pipeline, output }) => {
            let data_dir = dirs::home_dir()
                .context("could not determine home directory")?
                .join(".horizon-flux");
            let pipelines_dir = data_dir.join("pipelines");
            let pipeline_store = flux_engine::PipelineStore::open(
                &data_dir.join("pipelines.db"),
                &pipelines_dir,
            )
            .context("failed to open pipeline store")?;

            // Try UUID first, then name.
            let record = if let Ok(id) = pipeline.parse::<flux_engine::PipelineId>() {
                pipeline_store
                    .get(&id)
                    .context("failed to read pipeline")?
            } else {
                pipeline_store
                    .get_by_name(&pipeline)
                    .context("failed to read pipeline")?
            }
            .ok_or_else(|| anyhow::anyhow!("pipeline `{pipeline}` not found"))?;

            let json = record.pipeline.to_json().context("failed to serialize pipeline")?;
            let out_path = output.unwrap_or_else(|| {
                let name: String = record
                    .pipeline
                    .name
                    .chars()
                    .map(|c| {
                        if c.is_alphanumeric() || c == '-' || c == '_' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect();
                std::path::PathBuf::from(format!("{name}.json"))
            });
            std::fs::write(&out_path, &json)
                .with_context(|| format!("failed to write {}", out_path.display()))?;
            println!("Exported `{}` → {}", record.pipeline.name, out_path.display());
        }
        Some(Command::Import { file, on_conflict }) => {
            let data_dir = dirs::home_dir()
                .context("could not determine home directory")?
                .join(".horizon-flux");
            let pipelines_dir = data_dir.join("pipelines");
            std::fs::create_dir_all(&data_dir).context("failed to create data directory")?;
            let pipeline_store = flux_engine::PipelineStore::open(
                &data_dir.join("pipelines.db"),
                &pipelines_dir,
            )
            .context("failed to open pipeline store")?;

            let json = std::fs::read_to_string(&file)
                .with_context(|| format!("failed to read {}", file.display()))?;

            let (mut pipeline, warnings) = flux_engine::Pipeline::from_json_with_warnings(&json)
                .context("failed to parse pipeline")?;

            for w in &warnings.undefined_variables {
                eprintln!("warning: {w}");
            }

            // Handle name conflicts.
            let existing = pipeline_store
                .get_by_name(&pipeline.name)
                .context("failed to check for name conflict")?;

            let record = if let Some(existing_record) = existing {
                match on_conflict.as_str() {
                    "rename" => {
                        let base_name = pipeline.name.clone();
                        let mut counter = 2u32;
                        loop {
                            let candidate = format!("{base_name} ({counter})");
                            if pipeline_store.get_by_name(&candidate)?.is_none() {
                                pipeline.name = candidate;
                                break;
                            }
                            counter += 1;
                            anyhow::ensure!(counter <= 100, "could not find a unique name");
                        }
                        pipeline_store
                            .create(pipeline)
                            .context("failed to create pipeline")?
                    }
                    "overwrite" => pipeline_store
                        .update(&existing_record.id, pipeline)
                        .context("failed to overwrite pipeline")?,
                    _ => {
                        anyhow::bail!(
                            "pipeline `{}` already exists (use --on-conflict rename or overwrite)",
                            pipeline.name
                        );
                    }
                }
            } else {
                pipeline_store
                    .create(pipeline)
                    .context("failed to create pipeline")?
            };

            println!(
                "Imported `{}` (id: {})",
                record.pipeline.name, record.id
            );
        }
        None => {
            let config = flux_server::ServerConfig {
                port_start: cli.port,
                open_browser: !cli.no_browser,
                dev_mode: cli.dev,
                ..Default::default()
            };

            let data_dir = dirs::home_dir()
                .context("could not determine home directory")?
                .join(".horizon-flux");
            std::fs::create_dir_all(&data_dir).context("failed to create data directory")?;

            let pipelines_dir = data_dir.join("pipelines");
            let pipeline_store = Arc::new(
                flux_engine::PipelineStore::open(
                    &data_dir.join("pipelines.db"),
                    &pipelines_dir,
                )
                .context("failed to open pipeline store")?,
            );
            let run_store = Arc::new(
                flux_datafusion::RunStore::open(&data_dir.join("runs.db"))
                    .context("failed to open run store")?,
            );
            let connector_registry = Arc::new(flux_connectors::default_registry());
            let environment_store = Arc::new(
                flux_datafusion::EnvironmentStore::open(&data_dir.join("environments.db"))
                    .context("failed to open environment store")?,
            );

            // Open the secret store if a password is available via env var.
            let secret_store = match std::env::var("HORIZON_FLUX_SECRET_PASSWORD") {
                Ok(password) if !password.is_empty() => {
                    let secrets_path = data_dir.join("secrets.db");
                    match flux_secrets::SecretStore::open_or_init(
                        &secrets_path,
                        password.as_bytes(),
                    ) {
                        Ok(store) => Some(Arc::new(std::sync::Mutex::new(store))),
                        Err(e) => {
                            tracing::warn!("Could not open secret store: {e}");
                            None
                        }
                    }
                }
                _ => None,
            };

            let event_tx = flux_server::AppState::new_event_channel();

            // Spawn the system tray (degrades gracefully if unavailable).
            let tray_handle = flux_tray::spawn(
                flux_tray::TrayConfig {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                event_tx.subscribe(),
            );

            let app_state = flux_server::AppState {
                pipeline_store,
                run_store,
                connector_registry,
                environment_store,
                secret_store,
                event_tx,
            };

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

            let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
            rt.block_on(flux_server::serve(config, app_state, on_ready))
                .context("server failed")?;

            // Clean up the tray on server exit.
            if let Some(handle) = tray_handle {
                handle.shutdown();
            }
        }
    }

    Ok(())
}
