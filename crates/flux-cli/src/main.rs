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

            let pipeline_store = Arc::new(
                flux_engine::PipelineStore::open(&data_dir.join("pipelines.db"))
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
                    match flux_secrets::SecretStore::open_or_init(&secrets_path, password.as_bytes())
                    {
                        Ok(store) => Some(Arc::new(std::sync::Mutex::new(store))),
                        Err(e) => {
                            tracing::warn!("Could not open secret store: {e}");
                            None
                        }
                    }
                }
                _ => None,
            };

            let app_state = flux_server::AppState {
                pipeline_store,
                run_store,
                connector_registry,
                environment_store,
                secret_store,
            };

            let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
            rt.block_on(flux_server::serve(config, app_state))
                .context("server failed")?;
        }
    }

    Ok(())
}
