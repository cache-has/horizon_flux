// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `armillary plugin` subcommands — discovery, inspection, and smoke-testing of
//! installed plugins. See `planning/24-plugin-system.md` § armillary-cli.

use std::path::PathBuf;

use anyhow::{Context, Result};
use armillary_plugin_host::{
    DiscoveredPlugin, PROTOCOL_VERSION, PluginProcess, PluginSession, PluginStatus, SpawnOptions,
    discover_plugins, discovery::scan_roots,
};
use clap::Subcommand;
use serde_json::json;

use crate::{OutputFormat, color};

#[derive(Subcommand)]
pub enum PluginAction {
    /// List discovered plugins and the sinks they provide.
    List,
    /// Show manifest details for a single plugin.
    Info {
        /// Plugin name (matches the directory and manifest `name`).
        name: String,
    },
    /// Spawn a plugin, complete the protocol handshake, and exit.
    Check {
        /// Plugin name to spawn.
        name: String,
    },
    /// Print the directories armillary scans for plugins, in priority order.
    Path,
}

pub fn handle(action: PluginAction, format: OutputFormat) -> Result<()> {
    match action {
        PluginAction::List => list(format),
        PluginAction::Info { name } => info(&name, format),
        PluginAction::Check { name } => check(&name, format),
        PluginAction::Path => path(format),
    }
}

fn cwd() -> Result<PathBuf> {
    std::env::current_dir().context("failed to read current directory")
}

fn list(format: OutputFormat) -> Result<()> {
    let cwd = cwd()?;
    let registry = discover_plugins(&cwd);
    let plugins: Vec<&DiscoveredPlugin> = registry.iter().collect();

    match format {
        OutputFormat::Human => {
            if plugins.is_empty() {
                println!("No plugins discovered.");
                return Ok(());
            }
            for p in &plugins {
                let status = match &p.status {
                    PluginStatus::Ok => color::green("ok").to_string(),
                    PluginStatus::Invalid { error } => {
                        format!("{}: {error}", color::red("invalid"))
                    }
                };
                let version = p
                    .manifest
                    .as_ref()
                    .map(|m| m.version.as_str())
                    .unwrap_or("?");
                println!("{}  v{}  [{}]", p.name, version, status);
                println!("  dir: {}", p.directory.display());
                if let Some(m) = &p.manifest {
                    for s in &m.sinks {
                        println!("  sink: {} — {}", s.ty, s.display_name);
                    }
                }
            }
        }
        OutputFormat::Json => {
            let items: Vec<_> = plugins
                .iter()
                .map(|p| {
                    json!({
                        "name": p.name,
                        "directory": p.directory.display().to_string(),
                        "status": &p.status,
                        "manifest": p.manifest,
                    })
                })
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({ "plugins": items }))?
            );
        }
    }
    Ok(())
}

fn info(name: &str, format: OutputFormat) -> Result<()> {
    let cwd = cwd()?;
    let registry = discover_plugins(&cwd);
    let plugin = registry
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("plugin `{name}` not found"))?;

    match format {
        OutputFormat::Human => {
            println!("name:      {}", plugin.name);
            println!("directory: {}", plugin.directory.display());
            match &plugin.status {
                PluginStatus::Ok => println!("status:    ok"),
                PluginStatus::Invalid { error } => println!("status:    invalid ({error})"),
            }
            if let Some(m) = &plugin.manifest {
                println!("version:   {}", m.version);
                if let Some(a) = &m.author {
                    println!("author:    {a}");
                }
                if let Some(d) = &m.description {
                    println!("about:     {d}");
                }
                if let Some(l) = &m.license {
                    println!("license:   {l}");
                }
                if let Some(h) = &m.homepage {
                    println!("homepage:  {h}");
                }
                println!("protocol:  {}", m.armillary_plugin_protocol);
                println!("min armillary:  {}", m.armillary_min_version);
                println!("exec:      {}", m.executable);
                println!("sinks:");
                for s in &m.sinks {
                    println!("  - {} ({})", s.ty, s.display_name);
                    if let Some(d) = &s.description {
                        println!("      {d}");
                    }
                    println!("      schema: {}", s.config_schema);
                    let caps = &s.capabilities;
                    println!(
                        "      caps:   transactional={} upsert={} schema_validation={}",
                        caps.transactional, caps.upsert, caps.schema_validation
                    );
                }
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "name": plugin.name,
                    "directory": plugin.directory.display().to_string(),
                    "status": &plugin.status,
                    "manifest": plugin.manifest,
                }))?
            );
        }
    }
    Ok(())
}

fn check(name: &str, format: OutputFormat) -> Result<()> {
    let cwd = cwd()?;
    let registry = discover_plugins(&cwd);
    let plugin = registry
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("plugin `{name}` not found"))?;

    if let PluginStatus::Invalid { error } = &plugin.status {
        anyhow::bail!("plugin `{name}` has an invalid manifest: {error}");
    }

    let process = PluginProcess::spawn(plugin, SpawnOptions::default())
        .with_context(|| format!("failed to spawn plugin `{name}`"))?;
    let mut session = PluginSession::new(process, PROTOCOL_VERSION, env!("CARGO_PKG_VERSION"));
    let ack = session
        .handshake()
        .with_context(|| format!("handshake with plugin `{name}` failed"))?
        .clone();
    let _ = session.shutdown();

    match format {
        OutputFormat::Human => {
            println!(
                "{} plugin `{}` v{} (protocol {})",
                color::green("ok"),
                name,
                ack.plugin_version,
                ack.protocol
            );
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "ok": true,
                    "name": name,
                    "plugin_version": ack.plugin_version,
                    "protocol": ack.protocol,
                    "capabilities": ack.capabilities,
                }))?
            );
        }
    }
    Ok(())
}

fn path(format: OutputFormat) -> Result<()> {
    let cwd = cwd()?;
    let roots = scan_roots(&cwd);
    match format {
        OutputFormat::Human => {
            if roots.is_empty() {
                println!("(no plugin scan roots configured)");
            } else {
                for r in &roots {
                    println!("{}", r.display());
                }
            }
        }
        OutputFormat::Json => {
            let items: Vec<_> = roots.iter().map(|p| p.display().to_string()).collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({ "roots": items }))?
            );
        }
    }
    Ok(())
}
