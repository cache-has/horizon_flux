// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `flux lineage` subcommands — cross-pipeline lineage graph queries
//! (planning doc 31).

use anyhow::{Context, Result};
use clap::Subcommand;
use flux_engine::lineage::{BindingDirection, LineageGraph, ResourceBinding, ResourceFingerprint};
use flux_engine::pipeline_store::PipelineId;

use crate::OutputFormat;
use crate::pipeline::open_stores;

const DEFAULT_ENV: &str = "default";

#[derive(Subcommand)]
pub enum LineageAction {
    /// Print the full cross-pipeline lineage graph.
    Graph {
        /// Environment scope (default: "default").
        #[arg(long, short)]
        env: Option<String>,
        /// Output as DOT (Graphviz) format instead of a table.
        #[arg(long)]
        dot: bool,
    },
    /// Show pipelines upstream of a given pipeline.
    Upstream {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Environment scope.
        #[arg(long, short)]
        env: Option<String>,
    },
    /// Show pipelines downstream of a given pipeline.
    Downstream {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Environment scope.
        #[arg(long, short)]
        env: Option<String>,
    },
    /// Impact analysis: what breaks if a pipeline's output changes?
    Impact {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Environment scope.
        #[arg(long, short)]
        env: Option<String>,
    },
    /// Detect circular dependencies in the lineage graph.
    Cycles {
        /// Environment scope.
        #[arg(long, short)]
        env: Option<String>,
    },
    /// Find dangling sources and orphaned sinks.
    Orphans {
        /// Environment scope.
        #[arg(long, short)]
        env: Option<String>,
    },
}

pub fn handle(
    action: LineageAction,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    match action {
        LineageAction::Graph { env, dot } => graph(env.as_deref(), dot, format, metadata_url),
        LineageAction::Upstream { pipeline, env } => {
            upstream(&pipeline, env.as_deref(), format, metadata_url)
        }
        LineageAction::Downstream { pipeline, env } => {
            downstream(&pipeline, env.as_deref(), format, metadata_url)
        }
        LineageAction::Impact { pipeline, env } => {
            impact(&pipeline, env.as_deref(), format, metadata_url)
        }
        LineageAction::Cycles { env } => cycles(env.as_deref(), format, metadata_url),
        LineageAction::Orphans { env } => orphans(env.as_deref(), format, metadata_url),
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Build a lineage graph from stored bindings for the given environment.
fn build_graph(
    lineage_store: &dyn flux_datafusion::LineageStorage,
    environment: &str,
) -> Result<LineageGraph> {
    let stored = lineage_store
        .all_bindings(environment)
        .context("failed to load lineage bindings")?;

    let bindings: Vec<ResourceBinding> = stored
        .into_iter()
        .filter_map(|sb| {
            let pipeline_id = sb.pipeline_id.parse::<PipelineId>().ok()?;
            Some(ResourceBinding {
                pipeline_id,
                node_id: sb.node_id,
                direction: sb.direction,
                fingerprint: sb.resource_fingerprint,
            })
        })
        .collect();

    let edges = derive_edges(&bindings);
    Ok(LineageGraph { edges, bindings })
}

/// Derive static edges by matching sink fingerprints to source fingerprints.
fn derive_edges(bindings: &[ResourceBinding]) -> Vec<flux_engine::lineage::LineageEdge> {
    use std::collections::HashMap;

    let mut sinks_by_fp: HashMap<&ResourceFingerprint, Vec<&ResourceBinding>> = HashMap::new();
    for b in bindings {
        if b.direction == BindingDirection::Sink {
            sinks_by_fp.entry(&b.fingerprint).or_default().push(b);
        }
    }

    let mut edges = Vec::new();
    for b in bindings {
        if b.direction != BindingDirection::Source {
            continue;
        }
        if let Some(sinks) = sinks_by_fp.get(&b.fingerprint) {
            for sink in sinks {
                if sink.pipeline_id == b.pipeline_id && sink.node_id == b.node_id {
                    continue;
                }
                edges.push(flux_engine::lineage::LineageEdge {
                    upstream_pipeline_id: sink.pipeline_id.clone(),
                    upstream_node_id: sink.node_id.clone(),
                    downstream_pipeline_id: b.pipeline_id.clone(),
                    downstream_node_id: b.node_id.clone(),
                    fingerprint: b.fingerprint.clone(),
                    source: flux_engine::lineage::EdgeSource::Static,
                });
            }
        }
    }

    edges
}

/// Build a lookup from pipeline ID → pipeline name.
fn pipeline_names(
    store: &dyn flux_engine::PipelineStorage,
) -> Result<std::collections::HashMap<PipelineId, String>> {
    let count = store.count().context("failed to count pipelines")?;
    let records = store.list(count, 0).context("failed to list pipelines")?;
    Ok(records
        .into_iter()
        .map(|r| (r.id, r.pipeline.name))
        .collect())
}

/// Resolve a display name for a pipeline ID, falling back to the UUID string.
fn display_name(names: &std::collections::HashMap<PipelineId, String>, id: &PipelineId) -> String {
    names.get(id).cloned().unwrap_or_else(|| id.to_string())
}

fn resolve_env(env: Option<&str>) -> String {
    env.unwrap_or(DEFAULT_ENV).to_string()
}

// ---------------------------------------------------------------------------
// graph
// ---------------------------------------------------------------------------

fn graph(
    env: Option<&str>,
    dot: bool,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let lg = build_graph(&*stores.lineage_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;

    if dot {
        print_dot(&lg, &names);
        return Ok(());
    }

    let pipeline_ids = lg.pipeline_ids();

    match format {
        OutputFormat::Human => {
            if lg.edges.is_empty() {
                println!("No cross-pipeline lineage edges found (env `{environment}`).");
                return Ok(());
            }
            println!(
                "{} pipelines, {} edges (env `{environment}`)\n",
                pipeline_ids.len(),
                lg.edges.len(),
            );
            println!(
                "{}",
                crate::color::bold(&format!(
                    "{:<30} {:<16} {:<30} {:<16} {:<8} {}",
                    "UPSTREAM", "NODE", "DOWNSTREAM", "NODE", "SOURCE", "RESOURCE"
                ))
            );
            println!("{}", crate::color::dim(&"-".repeat(120)));
            for e in &lg.edges {
                println!(
                    "{:<30} {:<16} {:<30} {:<16} {:<8} {}",
                    truncate(&display_name(&names, &e.upstream_pipeline_id), 30),
                    truncate(&e.upstream_node_id, 16),
                    truncate(&display_name(&names, &e.downstream_pipeline_id), 30),
                    truncate(&e.downstream_node_id, 16),
                    format!("{:?}", e.source).to_lowercase(),
                    e.fingerprint,
                );
            }
        }
        OutputFormat::Json => {
            let edges: Vec<serde_json::Value> = lg
                .edges
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "upstream_pipeline_id": e.upstream_pipeline_id.to_string(),
                        "upstream_pipeline_name": display_name(&names, &e.upstream_pipeline_id),
                        "upstream_node_id": e.upstream_node_id,
                        "downstream_pipeline_id": e.downstream_pipeline_id.to_string(),
                        "downstream_pipeline_name": display_name(&names, &e.downstream_pipeline_id),
                        "downstream_node_id": e.downstream_node_id,
                        "resource": e.fingerprint.to_string(),
                        "source": format!("{:?}", e.source).to_lowercase(),
                    })
                })
                .collect();
            let pipelines: Vec<serde_json::Value> = pipeline_ids
                .iter()
                .map(|id| {
                    serde_json::json!({
                        "id": id.to_string(),
                        "name": display_name(&names, id),
                    })
                })
                .collect();
            let out = serde_json::json!({
                "environment": environment,
                "pipelines": pipelines,
                "edges": edges,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn print_dot(lg: &LineageGraph, names: &std::collections::HashMap<PipelineId, String>) {
    println!("digraph lineage {{");
    println!("  rankdir=LR;");
    println!("  node [shape=box];");

    for id in lg.pipeline_ids() {
        let label = display_name(names, &id).replace('"', "\\\"");
        println!("  \"{}\" [label=\"{}\"];", id, label);
    }

    for e in &lg.edges {
        let label = e.fingerprint.to_string().replace('"', "\\\"");
        println!(
            "  \"{}\" -> \"{}\" [label=\"{}\"];",
            e.upstream_pipeline_id, e.downstream_pipeline_id, label
        );
    }

    println!("}}");
}

// ---------------------------------------------------------------------------
// upstream / downstream
// ---------------------------------------------------------------------------

fn upstream(
    pipeline_name: &str,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let record = crate::pipeline::resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
    let lg = build_graph(&*stores.lineage_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;

    let direct = lg.upstream_of(&record.id);
    let transitive = lg.all_upstream(&record.id);

    match format {
        OutputFormat::Human => {
            if direct.is_empty() {
                println!(
                    "No upstream pipelines for `{}` (env `{environment}`).",
                    record.pipeline.name
                );
                return Ok(());
            }
            println!(
                "Upstream of `{}` (env `{environment}`):\n",
                record.pipeline.name
            );
            println!(
                "{}",
                crate::color::bold(&format!(
                    "  {:<30} {:<16} {}",
                    "PIPELINE", "NODE", "RESOURCE"
                ))
            );
            for e in &direct {
                println!(
                    "  {:<30} {:<16} {}",
                    display_name(&names, &e.upstream_pipeline_id),
                    e.upstream_node_id,
                    e.fingerprint,
                );
            }
            if transitive.len() > direct.len() {
                println!(
                    "\n  Transitive: {}",
                    transitive
                        .iter()
                        .map(|id| display_name(&names, id))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "pipeline_id": record.id.to_string(),
                "pipeline_name": record.pipeline.name,
                "environment": environment,
                "direct": direct.iter().map(|e| serde_json::json!({
                    "upstream_pipeline_id": e.upstream_pipeline_id.to_string(),
                    "upstream_pipeline_name": display_name(&names, &e.upstream_pipeline_id),
                    "upstream_node_id": e.upstream_node_id,
                    "resource": e.fingerprint.to_string(),
                })).collect::<Vec<_>>(),
                "transitive": transitive.iter().map(|id| serde_json::json!({
                    "id": id.to_string(),
                    "name": display_name(&names, id),
                })).collect::<Vec<_>>(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn downstream(
    pipeline_name: &str,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let record = crate::pipeline::resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
    let lg = build_graph(&*stores.lineage_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;

    let direct = lg.downstream_of(&record.id);
    let transitive = lg.all_downstream(&record.id);

    match format {
        OutputFormat::Human => {
            if direct.is_empty() {
                println!(
                    "No downstream pipelines for `{}` (env `{environment}`).",
                    record.pipeline.name
                );
                return Ok(());
            }
            println!(
                "Downstream of `{}` (env `{environment}`):\n",
                record.pipeline.name
            );
            println!(
                "{}",
                crate::color::bold(&format!(
                    "  {:<30} {:<16} {}",
                    "PIPELINE", "NODE", "RESOURCE"
                ))
            );
            for e in &direct {
                println!(
                    "  {:<30} {:<16} {}",
                    display_name(&names, &e.downstream_pipeline_id),
                    e.downstream_node_id,
                    e.fingerprint,
                );
            }
            if transitive.len() > direct.len() {
                println!(
                    "\n  Transitive: {}",
                    transitive
                        .iter()
                        .map(|id| display_name(&names, id))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "pipeline_id": record.id.to_string(),
                "pipeline_name": record.pipeline.name,
                "environment": environment,
                "direct": direct.iter().map(|e| serde_json::json!({
                    "downstream_pipeline_id": e.downstream_pipeline_id.to_string(),
                    "downstream_pipeline_name": display_name(&names, &e.downstream_pipeline_id),
                    "downstream_node_id": e.downstream_node_id,
                    "resource": e.fingerprint.to_string(),
                })).collect::<Vec<_>>(),
                "transitive": transitive.iter().map(|id| serde_json::json!({
                    "id": id.to_string(),
                    "name": display_name(&names, id),
                })).collect::<Vec<_>>(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// impact
// ---------------------------------------------------------------------------

fn impact(
    pipeline_name: &str,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let record = crate::pipeline::resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
    let lg = build_graph(&*stores.lineage_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;

    let direct = lg.downstream_of(&record.id);
    let all_affected = lg.all_downstream(&record.id);

    match format {
        OutputFormat::Human => {
            if all_affected.is_empty() {
                println!(
                    "No downstream impact from `{}` (env `{environment}`).",
                    record.pipeline.name
                );
                return Ok(());
            }
            println!(
                "Impact of `{}` (env `{environment}`):\n",
                record.pipeline.name
            );
            println!("  {} affected pipeline(s):", all_affected.len());
            for id in &all_affected {
                let marker = if direct.iter().any(|e| &e.downstream_pipeline_id == id) {
                    "direct"
                } else {
                    "transitive"
                };
                println!("    {} ({})", display_name(&names, id), marker);
            }
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "pipeline_id": record.id.to_string(),
                "pipeline_name": record.pipeline.name,
                "environment": environment,
                "affected_pipelines": all_affected.iter().map(|id| {
                    let is_direct = direct.iter().any(|e| &e.downstream_pipeline_id == id);
                    serde_json::json!({
                        "id": id.to_string(),
                        "name": display_name(&names, id),
                        "relationship": if is_direct { "direct" } else { "transitive" },
                    })
                }).collect::<Vec<_>>(),
                "direct_edges": direct.iter().map(|e| serde_json::json!({
                    "downstream_pipeline_id": e.downstream_pipeline_id.to_string(),
                    "downstream_pipeline_name": display_name(&names, &e.downstream_pipeline_id),
                    "downstream_node_id": e.downstream_node_id,
                    "resource": e.fingerprint.to_string(),
                })).collect::<Vec<_>>(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// cycles
// ---------------------------------------------------------------------------

fn cycles(env: Option<&str>, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let lg = build_graph(&*stores.lineage_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;
    let detected = lg.detect_cycles();

    match format {
        OutputFormat::Human => {
            if detected.is_empty() {
                println!("No cycles detected (env `{environment}`).");
            } else {
                println!(
                    "{} cycle(s) detected (env `{environment}`):\n",
                    detected.len()
                );
                for (i, cycle) in detected.iter().enumerate() {
                    let names_str: Vec<String> =
                        cycle.iter().map(|id| display_name(&names, id)).collect();
                    println!("  Cycle {}: {}", i + 1, names_str.join(" → "));
                }
            }
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "environment": environment,
                "cycles": detected.iter().map(|cycle| {
                    cycle.iter().map(|id| serde_json::json!({
                        "id": id.to_string(),
                        "name": display_name(&names, id),
                    })).collect::<Vec<_>>()
                }).collect::<Vec<_>>(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// orphans
// ---------------------------------------------------------------------------

fn orphans(env: Option<&str>, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let lg = build_graph(&*stores.lineage_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;

    let dangling = lg.dangling_sources();
    let orphaned = lg.orphaned_sinks();

    match format {
        OutputFormat::Human => {
            if dangling.is_empty() && orphaned.is_empty() {
                println!("No orphans or dangling references (env `{environment}`).");
                return Ok(());
            }
            if !dangling.is_empty() {
                println!("Dangling sources (no known producer):\n");
                println!(
                    "{}",
                    crate::color::bold(&format!(
                        "  {:<30} {:<16} {}",
                        "PIPELINE", "NODE", "RESOURCE"
                    ))
                );
                for b in &dangling {
                    println!(
                        "  {:<30} {:<16} {}",
                        display_name(&names, &b.pipeline_id),
                        b.node_id,
                        b.fingerprint,
                    );
                }
                println!();
            }
            if !orphaned.is_empty() {
                println!("Orphaned sinks (no known consumer):\n");
                println!(
                    "{}",
                    crate::color::bold(&format!(
                        "  {:<30} {:<16} {}",
                        "PIPELINE", "NODE", "RESOURCE"
                    ))
                );
                for b in &orphaned {
                    println!(
                        "  {:<30} {:<16} {}",
                        display_name(&names, &b.pipeline_id),
                        b.node_id,
                        b.fingerprint,
                    );
                }
            }
        }
        OutputFormat::Json => {
            let to_json = |b: &&ResourceBinding| {
                serde_json::json!({
                    "pipeline_id": b.pipeline_id.to_string(),
                    "pipeline_name": display_name(&names, &b.pipeline_id),
                    "node_id": b.node_id,
                    "resource": b.fingerprint.to_string(),
                })
            };
            let out = serde_json::json!({
                "environment": environment,
                "dangling_sources": dangling.iter().map(to_json).collect::<Vec<_>>(),
                "orphaned_sinks": orphaned.iter().map(to_json).collect::<Vec<_>>(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max - 1).collect();
        out.push('…');
        out
    }
}
