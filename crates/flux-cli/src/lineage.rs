// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `flux lineage` subcommands — cross-pipeline lineage graph queries
//! (planning doc 31) and column-level lineage queries (planning doc 35).

use std::collections::HashMap;

use anyhow::{Context, Result};
use clap::Subcommand;
use flux_engine::column_lineage::{
    BoundaryColumn, ColumnLineageGraph, Confidence, RelationshipKind, TraceOptions,
    derive_cross_pipeline_column_lineage,
};
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
    /// Column-level lineage queries (doc 35).
    Column {
        #[command(subcommand)]
        action: ColumnAction,
    },
}

#[derive(Subcommand)]
pub enum ColumnAction {
    /// Trace upstream columns that contribute to a given column.
    Upstream {
        /// Resource fingerprint identifying the column's resource.
        fingerprint: String,
        /// Column name.
        column: String,
        /// Environment scope (default: "default").
        #[arg(long, short)]
        env: Option<String>,
        /// Maximum traversal depth (default: 10).
        #[arg(long, default_value_t = 10)]
        max_depth: usize,
        /// Comma-separated relationship kinds to include (e.g. "direct,derived").
        #[arg(long)]
        relationships: Option<String>,
        /// Comma-separated confidence levels to include (e.g. "exact,annotation").
        #[arg(long)]
        confidence: Option<String>,
    },
    /// Trace downstream columns that depend on a given column.
    Downstream {
        /// Resource fingerprint identifying the column's resource.
        fingerprint: String,
        /// Column name.
        column: String,
        /// Environment scope (default: "default").
        #[arg(long, short)]
        env: Option<String>,
        /// Maximum traversal depth (default: 10).
        #[arg(long, default_value_t = 10)]
        max_depth: usize,
        /// Comma-separated relationship kinds to include (e.g. "direct,derived").
        #[arg(long)]
        relationships: Option<String>,
        /// Comma-separated confidence levels to include (e.g. "exact,annotation").
        #[arg(long)]
        confidence: Option<String>,
    },
    /// Impact analysis: what breaks if a column is renamed or dropped?
    Impact {
        /// Resource fingerprint identifying the column's resource.
        fingerprint: String,
        /// Column name.
        column: String,
        /// Environment scope (default: "default").
        #[arg(long, short)]
        env: Option<String>,
        /// Maximum traversal depth (default: 10).
        #[arg(long, default_value_t = 10)]
        max_depth: usize,
    },
    /// Search for columns by name across all resources.
    Search {
        /// Search query (substring match on column name).
        query: String,
        /// Environment scope (default: "default").
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
        LineageAction::Column { action } => handle_column(action, format, metadata_url),
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
// Column-level lineage (doc 35)
// ---------------------------------------------------------------------------

fn handle_column(
    action: ColumnAction,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    match action {
        ColumnAction::Upstream {
            fingerprint,
            column,
            env,
            max_depth,
            relationships,
            confidence,
        } => column_trace(
            &fingerprint,
            &column,
            env.as_deref(),
            max_depth,
            relationships.as_deref(),
            confidence.as_deref(),
            true,
            format,
            metadata_url,
        ),
        ColumnAction::Downstream {
            fingerprint,
            column,
            env,
            max_depth,
            relationships,
            confidence,
        } => column_trace(
            &fingerprint,
            &column,
            env.as_deref(),
            max_depth,
            relationships.as_deref(),
            confidence.as_deref(),
            false,
            format,
            metadata_url,
        ),
        ColumnAction::Impact {
            fingerprint,
            column,
            env,
            max_depth,
        } => column_impact(
            &fingerprint,
            &column,
            env.as_deref(),
            max_depth,
            format,
            metadata_url,
        ),
        ColumnAction::Search { query, env } => {
            column_search(&query, env.as_deref(), format, metadata_url)
        }
    }
}

/// Build a [`ColumnLineageGraph`] from stored column edges.
fn build_column_graph(
    store: &dyn flux_datafusion::ColumnLineageStorage,
    environment: &str,
) -> Result<ColumnLineageGraph> {
    let stored_edges = store
        .all_column_edges(environment)
        .context("failed to load column lineage edges")?;

    let mut by_pipeline: HashMap<PipelineId, Vec<flux_engine::ColumnEdge>> = HashMap::new();
    let mut boundary_columns = Vec::new();

    for se in &stored_edges {
        let pipeline_id: PipelineId = se
            .pipeline_id
            .parse()
            .context("invalid pipeline ID in stored edges")?;
        by_pipeline
            .entry(pipeline_id.clone())
            .or_default()
            .push(se.edge.clone());

        if let Some(ref fp) = se.edge.downstream_resource {
            if let Some(ref node) = se.edge.downstream_node {
                boundary_columns.push(BoundaryColumn {
                    pipeline_id: pipeline_id.clone(),
                    node_id: node.clone(),
                    column: se.edge.downstream_column.clone(),
                    fingerprint: fp.clone(),
                    direction: BindingDirection::Sink,
                });
            }
        }
        if let Some(ref fp) = se.edge.upstream_resource {
            if let Some(ref node) = se.edge.upstream_node {
                boundary_columns.push(BoundaryColumn {
                    pipeline_id,
                    node_id: node.clone(),
                    column: se.edge.upstream_column.clone(),
                    fingerprint: fp.clone(),
                    direction: BindingDirection::Source,
                });
            }
        }
    }

    let pipeline_edges: Vec<(PipelineId, Vec<flux_engine::ColumnEdge>)> =
        by_pipeline.into_iter().collect();
    let pipeline_edge_refs: Vec<(PipelineId, &[flux_engine::ColumnEdge])> = pipeline_edges
        .iter()
        .map(|(id, edges)| (id.clone(), edges.as_slice()))
        .collect();

    let cross_pipeline = derive_cross_pipeline_column_lineage(&boundary_columns);

    Ok(ColumnLineageGraph::new(
        &pipeline_edge_refs,
        &cross_pipeline.edges,
    ))
}

/// Parse comma-separated relationship kind strings.
fn parse_relationships(s: &str) -> std::collections::HashSet<RelationshipKind> {
    s.split(',')
        .filter_map(|r| match r.trim() {
            "direct" => Some(RelationshipKind::Direct),
            "derived" => Some(RelationshipKind::Derived),
            "cast" => Some(RelationshipKind::Cast),
            "filter" => Some(RelationshipKind::Filter),
            "join_key" => Some(RelationshipKind::JoinKey),
            "join_passthrough" => Some(RelationshipKind::JoinPassthrough),
            "group_by" => Some(RelationshipKind::GroupBy),
            "aggregate_input" => Some(RelationshipKind::AggregateInput),
            "window_partition" => Some(RelationshipKind::WindowPartition),
            "window_order" => Some(RelationshipKind::WindowOrder),
            "window_input" => Some(RelationshipKind::WindowInput),
            "opaque" => Some(RelationshipKind::Opaque),
            _ => None,
        })
        .collect()
}

/// Parse comma-separated confidence level strings.
fn parse_confidences(s: &str) -> std::collections::HashSet<Confidence> {
    s.split(',')
        .filter_map(|c| match c.trim() {
            "exact" => Some(Confidence::Exact),
            "lazyframe" => Some(Confidence::LazyFrame),
            "annotation" => Some(Confidence::Annotation),
            "opaque" => Some(Confidence::Opaque),
            _ => None,
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn column_trace(
    fingerprint: &str,
    column: &str,
    env: Option<&str>,
    max_depth: usize,
    relationships: Option<&str>,
    confidence: Option<&str>,
    is_upstream: bool,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let col_store = stores
        .column_lineage_store
        .as_ref()
        .context("column lineage storage not available")?;
    let graph = build_column_graph(&**col_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;

    let fp = ResourceFingerprint(fingerprint.to_string());
    let keys = graph.resolve_by_fingerprint(&fp, column);
    if keys.is_empty() {
        println!("No column `{column}` found for resource `{fingerprint}` (env `{environment}`).");
        return Ok(());
    }

    let opts = TraceOptions {
        max_depth,
        relationship_filter: relationships.map(parse_relationships).unwrap_or_default(),
        confidence_filter: confidence.map(parse_confidences).unwrap_or_default(),
    };

    let direction_label = if is_upstream {
        "Upstream"
    } else {
        "Downstream"
    };

    // Collect traces from all matching keys.
    let mut all_edges = Vec::new();
    let mut any_truncated = false;
    for key in &keys {
        let result = if is_upstream {
            graph.upstream_trace(key, &opts)
        } else {
            graph.downstream_trace(key, &opts)
        };
        any_truncated |= result.truncated;
        all_edges.extend(result.edges);
    }

    match format {
        OutputFormat::Human => {
            if all_edges.is_empty() {
                println!(
                    "No {lower} columns for `{column}` on `{fingerprint}` (env `{environment}`).",
                    lower = direction_label.to_lowercase(),
                );
                return Ok(());
            }
            println!("{direction_label} of `{column}` on `{fingerprint}` (env `{environment}`):\n");
            println!(
                "{}",
                crate::color::bold(&format!(
                    "  {:<30} {:<16} {:<20} {:<14} {:<10} {}",
                    "PIPELINE", "NODE", "COLUMN", "RELATIONSHIP", "CONFIDENCE", "DEPTH"
                ))
            );
            println!("{}", crate::color::dim(&format!("  {}", "-".repeat(100))));
            for e in &all_edges {
                let (target_key, _) = if is_upstream {
                    (&e.upstream, &e.downstream)
                } else {
                    (&e.downstream, &e.upstream)
                };
                println!(
                    "  {:<30} {:<16} {:<20} {:<14} {:<10} {}",
                    truncate(&display_name(&names, &target_key.pipeline_id), 30),
                    truncate(&target_key.node_id.0, 16),
                    truncate(&target_key.column, 20),
                    format!("{:?}", e.relationship).to_lowercase(),
                    format!("{:?}", e.confidence).to_lowercase(),
                    e.depth,
                );
            }
            if any_truncated {
                println!(
                    "\n  {} Results truncated at depth {max_depth}. Use --max-depth to increase.",
                    crate::color::dim("(truncated)"),
                );
            }
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "fingerprint": fingerprint,
                "column": column,
                "environment": environment,
                "direction": direction_label.to_lowercase(),
                "truncated": any_truncated,
                "edges": all_edges.iter().map(|e| serde_json::json!({
                    "upstream": {
                        "pipeline_id": e.upstream.pipeline_id.to_string(),
                        "pipeline_name": display_name(&names, &e.upstream.pipeline_id),
                        "node_id": e.upstream.node_id.0,
                        "column": e.upstream.column,
                    },
                    "downstream": {
                        "pipeline_id": e.downstream.pipeline_id.to_string(),
                        "pipeline_name": display_name(&names, &e.downstream.pipeline_id),
                        "node_id": e.downstream.node_id.0,
                        "column": e.downstream.column,
                    },
                    "relationship": format!("{:?}", e.relationship).to_lowercase(),
                    "confidence": format!("{:?}", e.confidence).to_lowercase(),
                    "expression": e.expression_text,
                    "depth": e.depth,
                })).collect::<Vec<_>>(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn column_impact(
    fingerprint: &str,
    column: &str,
    env: Option<&str>,
    max_depth: usize,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let col_store = stores
        .column_lineage_store
        .as_ref()
        .context("column lineage storage not available")?;
    let graph = build_column_graph(&**col_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;

    let fp = ResourceFingerprint(fingerprint.to_string());
    let keys = graph.resolve_by_fingerprint(&fp, column);
    if keys.is_empty() {
        println!("No column `{column}` found for resource `{fingerprint}` (env `{environment}`).");
        return Ok(());
    }

    let opts = TraceOptions {
        max_depth,
        ..TraceOptions::default()
    };

    // Gather downstream edges from all matching keys.
    let mut all_edges = Vec::new();
    let mut any_truncated = false;
    for key in &keys {
        let result = graph.downstream_trace(key, &opts);
        any_truncated |= result.truncated;
        all_edges.extend(result.edges);
    }

    // Group affected columns by pipeline.
    let mut by_pipeline: HashMap<PipelineId, Vec<&flux_engine::TraceEdge>> = HashMap::new();
    for e in &all_edges {
        by_pipeline
            .entry(e.downstream.pipeline_id.clone())
            .or_default()
            .push(e);
    }

    match format {
        OutputFormat::Human => {
            if all_edges.is_empty() {
                println!(
                    "No downstream impact from `{column}` on `{fingerprint}` (env `{environment}`)."
                );
                return Ok(());
            }
            println!("Impact of `{column}` on `{fingerprint}` (env `{environment}`):\n");
            println!(
                "  {} affected column(s) across {} pipeline(s):",
                all_edges.len(),
                by_pipeline.len(),
            );
            for (pid, edges) in &by_pipeline {
                println!("\n  {}", crate::color::bold(&display_name(&names, pid)),);
                for e in edges {
                    let marker = if e.depth == 1 { "direct" } else { "transitive" };
                    println!(
                        "    {} ({}, {:?}, depth {})",
                        e.downstream.column, marker, e.relationship, e.depth,
                    );
                }
            }
            if any_truncated {
                println!(
                    "\n  {} Results truncated at depth {max_depth}.",
                    crate::color::dim("(truncated)"),
                );
            }
        }
        OutputFormat::Json => {
            let affected: Vec<serde_json::Value> = by_pipeline
                .iter()
                .map(|(pid, edges)| {
                    serde_json::json!({
                        "pipeline_id": pid.to_string(),
                        "pipeline_name": display_name(&names, pid),
                        "columns": edges.iter().map(|e| serde_json::json!({
                            "column": e.downstream.column,
                            "node_id": e.downstream.node_id.0,
                            "relationship": format!("{:?}", e.relationship).to_lowercase(),
                            "confidence": format!("{:?}", e.confidence).to_lowercase(),
                            "depth": e.depth,
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect();
            let out = serde_json::json!({
                "fingerprint": fingerprint,
                "column": column,
                "environment": environment,
                "truncated": any_truncated,
                "total_affected": all_edges.len(),
                "affected_pipelines": affected,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn column_search(
    query: &str,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let col_store = stores
        .column_lineage_store
        .as_ref()
        .context("column lineage storage not available")?;
    let graph = build_column_graph(&**col_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;

    let query_lower = query.to_lowercase();
    let matches: Vec<_> = graph
        .all_columns()
        .into_iter()
        .filter(|k| k.column.to_lowercase().contains(&query_lower))
        .collect();

    match format {
        OutputFormat::Human => {
            if matches.is_empty() {
                println!("No columns matching `{query}` (env `{environment}`).");
                return Ok(());
            }
            println!(
                "{} column(s) matching `{query}` (env `{environment}`):\n",
                matches.len(),
            );
            println!(
                "{}",
                crate::color::bold(&format!("  {:<30} {:<16} {}", "PIPELINE", "NODE", "COLUMN"))
            );
            println!("{}", crate::color::dim(&format!("  {}", "-".repeat(70))));
            for k in &matches {
                println!(
                    "  {:<30} {:<16} {}",
                    truncate(&display_name(&names, &k.pipeline_id), 30),
                    truncate(&k.node_id.0, 16),
                    k.column,
                );
            }
        }
        OutputFormat::Json => {
            let results: Vec<serde_json::Value> = matches
                .iter()
                .map(|k| {
                    serde_json::json!({
                        "pipeline_id": k.pipeline_id.to_string(),
                        "pipeline_name": display_name(&names, &k.pipeline_id),
                        "node_id": k.node_id.0,
                        "column": k.column,
                    })
                })
                .collect();
            let out = serde_json::json!({
                "query": query,
                "environment": environment,
                "results": results,
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
