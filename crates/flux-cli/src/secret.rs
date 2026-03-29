// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI commands for managing the encrypted secret store.

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use flux_secrets::SecretStore;

#[derive(Subcommand)]
pub enum SecretAction {
    /// Initialize the secret store (first-time setup).
    Init,
    /// Set (create or update) a secret.
    Set {
        /// Secret name.
        name: String,
        /// Secret value. If omitted, reads from stdin.
        value: Option<String>,
        /// Environment scope (e.g. prod, dev). Omit for a default secret.
        #[arg(long, short)]
        env: Option<String>,
        /// Read the secret value from the named environment variable.
        #[arg(long, conflicts_with = "value")]
        from_env: Option<String>,
    },
    /// List all secret names (never shows values).
    List,
    /// Delete a secret.
    Delete {
        /// Secret name.
        name: String,
        /// Environment scope. Omit to delete the default (unscoped) secret.
        #[arg(long, short)]
        env: Option<String>,
    },
}

pub fn handle(action: SecretAction) -> Result<()> {
    match action {
        SecretAction::Init => init(),
        SecretAction::Set {
            name,
            value,
            env,
            from_env,
        } => set(&name, value.as_deref(), env.as_deref(), from_env.as_deref()),
        SecretAction::List => list(),
        SecretAction::Delete { name, env } => delete(&name, env.as_deref()),
    }
}

fn store_path() -> Result<std::path::PathBuf> {
    SecretStore::default_path().context("could not determine home directory")
}

fn prompt_password(prompt: &str) -> Result<String> {
    rpassword::prompt_password(prompt).context("failed to read password")
}

fn open_store() -> Result<SecretStore> {
    let path = store_path()?;
    if !SecretStore::is_initialized(&path) {
        bail!("Secret store not initialized. Run `horizon-flux secret init` first.");
    }
    let password = prompt_password("Secret store password: ")?;
    SecretStore::open(&path, password.as_bytes()).context("failed to open secret store")
}

fn init() -> Result<()> {
    let path = store_path()?;
    if SecretStore::is_initialized(&path) {
        bail!("Secret store already initialized at {}", path.display());
    }

    let password = prompt_password("Choose a password for the secret store: ")?;
    let confirm = prompt_password("Confirm password: ")?;
    if password != confirm {
        bail!("Passwords do not match");
    }

    SecretStore::init(&path, password.as_bytes())?;
    println!("Secret store initialized at {}", path.display());
    Ok(())
}

fn set(name: &str, value: Option<&str>, env: Option<&str>, from_env: Option<&str>) -> Result<()> {
    let store = open_store()?;

    let secret_value = if let Some(var_name) = from_env {
        std::env::var(var_name)
            .with_context(|| format!("environment variable '{var_name}' is not set"))?
    } else {
        match value {
            Some(v) => v.to_string(),
            None => {
                // Read from stdin (allows piping).
                let mut buf = String::new();
                std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)?;
                buf.trim_end().to_string()
            }
        }
    };

    store.set(name, secret_value.as_bytes(), env)?;

    let scope = env.unwrap_or("default");
    println!("Secret '{name}' set (environment: {scope})");
    Ok(())
}

fn list() -> Result<()> {
    let store = open_store()?;
    let secrets = store.list()?;

    if secrets.is_empty() {
        println!("No secrets stored.");
        return Ok(());
    }

    println!(
        "{}",
        crate::color::bold(&format!("{:<30} {:<15} UPDATED", "NAME", "ENVIRONMENT"))
    );
    println!("{}", crate::color::dim(&"-".repeat(65)));
    for s in &secrets {
        let env = s.environment.as_deref().unwrap_or("(default)");
        println!("{:<30} {:<15} {}", s.name, env, s.updated_at);
    }

    Ok(())
}

fn delete(name: &str, env: Option<&str>) -> Result<()> {
    let store = open_store()?;
    store.delete(name, env)?;

    let scope = env.unwrap_or("default");
    println!("Secret '{name}' deleted (environment: {scope})");
    Ok(())
}
