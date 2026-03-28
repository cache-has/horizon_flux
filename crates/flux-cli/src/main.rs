// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
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
            tracing::info!("Horizon Flux v{}", flux_engine::version());
        }
    }

    Ok(())
}
