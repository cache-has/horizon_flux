// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI commands for managing environments.

use anyhow::{Context, Result};
use clap::Subcommand;
use flux_datafusion::EnvironmentStore;

use crate::OutputFormat;

#[derive(Subcommand)]
pub enum EnvAction {
    /// List all environments with their fallback chains.
    List,
    /// Create a new environment.
    Create {
        /// Environment name.
        name: String,
        /// Parent environment to fall back to.
        #[arg(long)]
        fallback: Option<String>,
    },
    /// Delete an environment.
    Delete {
        /// Environment name.
        name: String,
    },
    /// Show environment details (fallback chain, table overrides).
    Show {
        /// Environment name.
        name: String,
    },
}

pub fn handle(action: EnvAction, format: OutputFormat) -> Result<()> {
    match action {
        EnvAction::List => list(format),
        EnvAction::Create { name, fallback } => create(&name, fallback.as_deref(), format),
        EnvAction::Delete { name } => delete(&name, format),
        EnvAction::Show { name } => show(&name, format),
    }
}

fn open_store() -> Result<EnvironmentStore> {
    let data_dir = dirs::home_dir()
        .context("could not determine home directory")?
        .join(".horizon-flux");
    std::fs::create_dir_all(&data_dir).context("failed to create data directory")?;
    EnvironmentStore::open(&data_dir.join("environments.db"))
        .context("failed to open environment store")
}

fn list(format: OutputFormat) -> Result<()> {
    let store = open_store()?;
    let envs = store.list().context("failed to list environments")?;

    match format {
        OutputFormat::Human => {
            if envs.is_empty() {
                println!("No environments configured.");
                return Ok(());
            }
            println!(
                "{}",
                crate::color::bold(&format!("{:<20} FALLBACK CHAIN", "NAME"))
            );
            println!("{}", crate::color::dim(&"-".repeat(60)));
            for env in &envs {
                let chain = store.fallback_chain(&env.name).unwrap_or_default();
                let chain_str = chain.join(" → ");
                println!("{:<20} {chain_str}", env.name);
            }
        }
        OutputFormat::Json => {
            let items: Vec<_> = envs
                .iter()
                .map(|env| {
                    let chain = store.fallback_chain(&env.name).unwrap_or_default();
                    serde_json::json!({
                        "name": env.name,
                        "fallback": env.fallback,
                        "chain": chain,
                    })
                })
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "environments": items }))?
            );
        }
    }
    Ok(())
}

fn create(name: &str, fallback: Option<&str>, format: OutputFormat) -> Result<()> {
    let store = open_store()?;
    let env = store
        .create(name, fallback)
        .with_context(|| format!("failed to create environment '{name}'"))?;

    match format {
        OutputFormat::Human => {
            let fb = env.fallback.as_deref().unwrap_or("(none)");
            println!("Created environment '{name}' (fallback: {fb})");
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "name": env.name,
                    "fallback": env.fallback,
                }))?
            );
        }
    }
    Ok(())
}

fn delete(name: &str, format: OutputFormat) -> Result<()> {
    let store = open_store()?;
    store
        .delete(name)
        .with_context(|| format!("failed to delete environment '{name}'"))?;

    match format {
        OutputFormat::Human => println!("Deleted environment '{name}'"),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "deleted": name }))?
            );
        }
    }
    Ok(())
}

fn show(name: &str, format: OutputFormat) -> Result<()> {
    let store = open_store()?;
    let env = store
        .get(name)
        .context("failed to read environment")?
        .ok_or_else(|| anyhow::anyhow!("environment '{name}' not found"))?;

    let chain = store.fallback_chain(name).unwrap_or_default();
    let overrides = store
        .list_table_overrides(name)
        .context("failed to list table overrides")?;

    match format {
        OutputFormat::Human => {
            println!("Environment: {}", env.name);
            println!(
                "Fallback:    {}",
                env.fallback.as_deref().unwrap_or("(none)")
            );
            println!("Chain:       {}", chain.join(" → "));

            if overrides.is_empty() {
                println!("\nNo table overrides.");
            } else {
                println!("\nTable overrides:");
                for o in &overrides {
                    println!("  {}.{}", o.schema_name, o.table_name);
                }
            }
        }
        OutputFormat::Json => {
            let override_items: Vec<_> = overrides
                .iter()
                .map(|o| {
                    serde_json::json!({
                        "schema": o.schema_name,
                        "table": o.table_name,
                    })
                })
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "name": env.name,
                    "fallback": env.fallback,
                    "chain": chain,
                    "table_overrides": override_items,
                }))?
            );
        }
    }
    Ok(())
}
