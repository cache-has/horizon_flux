// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `armillary catalog` subcommands — resource catalog: list, show, search,
//! describe (scaffold), and validate (planning doc 34).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use armillary_engine::catalog::{self, Catalog, CatalogEntry, CatalogWarning, DiscoveredResource};
use armillary_engine::lineage::{
    BindingDirection, LineageGraph, ResourceBinding, ResourceFingerprint,
};
use armillary_engine::pipeline_store::PipelineId;
use clap::Subcommand;

use crate::OutputFormat;
use crate::pipeline::open_stores;

const DEFAULT_ENV: &str = "default";

#[derive(Subcommand)]
pub enum CatalogAction {
    /// List all resources in the catalog.
    List {
        /// Filter by tag.
        #[arg(long)]
        tag: Option<String>,
        /// Filter by owner team.
        #[arg(long)]
        owner: Option<String>,
        /// Environment scope (default: "default").
        #[arg(long, short)]
        env: Option<String>,
    },
    /// Show full details for a resource.
    Show {
        /// Resource fingerprint (e.g. postgres://host:5432/db/public.orders).
        fingerprint: String,
        /// Environment scope (default: "default").
        #[arg(long, short)]
        env: Option<String>,
    },
    /// Search resources by name, description, tag, or column name.
    Search {
        /// Search query.
        query: String,
        /// Environment scope (default: "default").
        #[arg(long, short)]
        env: Option<String>,
    },
    /// Scaffold a metadata annotation file for a resource.
    Describe {
        /// Resource fingerprint (omit when using --all).
        fingerprint: Option<String>,
        /// Scaffold metadata files for every resource that doesn't have one yet.
        #[arg(long)]
        all: bool,
        /// Environment scope (default: "default").
        #[arg(long, short)]
        env: Option<String>,
    },
    /// Validate all metadata annotation files.
    Validate {
        /// Environment scope (default: "default").
        #[arg(long, short)]
        env: Option<String>,
    },
}

pub fn handle(
    action: CatalogAction,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    match action {
        CatalogAction::List { tag, owner, env } => list(
            tag.as_deref(),
            owner.as_deref(),
            env.as_deref(),
            format,
            metadata_url,
        ),
        CatalogAction::Show { fingerprint, env } => {
            show(&fingerprint, env.as_deref(), format, metadata_url)
        }
        CatalogAction::Search { query, env } => {
            search(&query, env.as_deref(), format, metadata_url)
        }
        CatalogAction::Describe {
            fingerprint,
            all,
            env,
        } => describe(
            fingerprint.as_deref(),
            all,
            env.as_deref(),
            format,
            metadata_url,
        ),
        CatalogAction::Validate { env } => validate(env.as_deref(), format, metadata_url),
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn resolve_env(env: Option<&str>) -> String {
    env.unwrap_or(DEFAULT_ENV).to_string()
}

/// The metadata directory for annotation files (project root).
fn metadata_dir() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("metadata")
}

/// Build a lineage graph from stored bindings for the given environment.
fn build_graph(
    lineage_store: &dyn armillary_datafusion::LineageStorage,
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

fn derive_edges(bindings: &[ResourceBinding]) -> Vec<armillary_engine::lineage::LineageEdge> {
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
                edges.push(armillary_engine::lineage::LineageEdge {
                    upstream_pipeline_id: sink.pipeline_id.clone(),
                    upstream_node_id: sink.node_id.clone(),
                    downstream_pipeline_id: b.pipeline_id.clone(),
                    downstream_node_id: b.node_id.clone(),
                    fingerprint: b.fingerprint.clone(),
                    source: armillary_engine::lineage::EdgeSource::Static,
                });
            }
        }
    }

    edges
}

/// Build a lookup from pipeline ID to pipeline name.
fn pipeline_names(
    store: &dyn armillary_engine::PipelineStorage,
) -> Result<std::collections::HashMap<PipelineId, String>> {
    let count = store.count().context("failed to count pipelines")?;
    let records = store.list(count, 0).context("failed to list pipelines")?;
    Ok(records
        .into_iter()
        .map(|r| (r.id, r.pipeline.name))
        .collect())
}

fn display_name(names: &std::collections::HashMap<PipelineId, String>, id: &PipelineId) -> String {
    names.get(id).cloned().unwrap_or_else(|| id.to_string())
}

fn build_catalog(
    lineage_store: &dyn armillary_datafusion::LineageStorage,
    environment: &str,
) -> Result<Catalog> {
    let graph = build_graph(lineage_store, environment)?;
    let md_dir = metadata_dir();
    Ok(Catalog::build(&graph, &md_dir))
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max - 1).collect();
        out.push('…');
        out
    }
}

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

fn list(
    tag: Option<&str>,
    owner: Option<&str>,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let catalog = build_catalog(&*stores.lineage_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;

    let entries: Vec<&CatalogEntry> = if let Some(tag) = tag {
        catalog.filter_by_tag(tag)
    } else if let Some(owner) = owner {
        catalog.filter_by_owner(owner)
    } else {
        catalog.entries.iter().collect()
    };

    match format {
        OutputFormat::Human => {
            if entries.is_empty() {
                println!("No resources in catalog (env `{environment}`).");
                return Ok(());
            }
            println!("{} resource(s) (env `{environment}`)\n", entries.len(),);
            println!(
                "{}",
                crate::color::bold(&format!(
                    "{:<40} {:<12} {:<16} {:<16} {}",
                    "RESOURCE", "TYPE", "OWNER", "TAGS", "PRODUCERS"
                ))
            );
            println!("{}", crate::color::dim(&"-".repeat(100)));
            for entry in &entries {
                let rtype = entry.derived.resource_type.as_deref().unwrap_or("-");
                let owner_str = entry
                    .owner
                    .as_ref()
                    .and_then(|o| o.team.as_deref())
                    .unwrap_or("-");
                let tags = if entry.tags.is_empty() {
                    "-".to_string()
                } else {
                    entry.tags.join(", ")
                };
                let producers: Vec<String> = entry
                    .derived
                    .producers
                    .iter()
                    .map(|p| display_name(&names, &p.pipeline_id))
                    .collect();
                let producers_str = if producers.is_empty() {
                    "-".to_string()
                } else {
                    producers.join(", ")
                };
                println!(
                    "{:<40} {:<12} {:<16} {:<16} {}",
                    truncate(&entry.name, 40),
                    truncate(rtype, 12),
                    truncate(owner_str, 16),
                    truncate(&tags, 16),
                    truncate(&producers_str, 40),
                );
            }
        }
        OutputFormat::Json => {
            let items: Vec<serde_json::Value> = entries
                .iter()
                .map(|e| entry_to_summary_json(e, &names))
                .collect();
            let out = serde_json::json!({
                "environment": environment,
                "count": items.len(),
                "resources": items,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn entry_to_summary_json(
    entry: &CatalogEntry,
    names: &std::collections::HashMap<PipelineId, String>,
) -> serde_json::Value {
    serde_json::json!({
        "fingerprint": entry.fingerprint.to_string(),
        "name": entry.name,
        "resource_type": entry.derived.resource_type,
        "owner": entry.owner.as_ref().and_then(|o| o.team.clone()),
        "tags": entry.tags,
        "producers": entry.derived.producers.iter().map(|p| serde_json::json!({
            "pipeline_id": p.pipeline_id.to_string(),
            "pipeline_name": display_name(names, &p.pipeline_id),
            "node_id": p.node_id,
        })).collect::<Vec<_>>(),
        "consumers": entry.derived.consumers.iter().map(|c| serde_json::json!({
            "pipeline_id": c.pipeline_id.to_string(),
            "pipeline_name": display_name(names, &c.pipeline_id),
            "node_id": c.node_id,
        })).collect::<Vec<_>>(),
        "has_annotation": entry.annotation_file.is_some(),
    })
}

// ---------------------------------------------------------------------------
// show
// ---------------------------------------------------------------------------

fn show(
    fingerprint: &str,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let catalog = build_catalog(&*stores.lineage_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;

    let fp = ResourceFingerprint::new(fingerprint);
    let entry = catalog
        .get(&fp)
        .ok_or_else(|| anyhow::anyhow!("resource `{fingerprint}` not found in catalog"))?;

    match format {
        OutputFormat::Human => {
            println!("{}", crate::color::bold(&entry.name));
            println!("  Fingerprint:  {}", entry.fingerprint);
            if let Some(desc) = &entry.description {
                println!("  Description:  {desc}");
            }
            if let Some(rtype) = &entry.derived.resource_type {
                println!("  Type:         {rtype}");
            }
            if let Some(owner) = &entry.owner {
                if let Some(team) = &owner.team {
                    print!("  Owner:        {team}");
                    if let Some(contact) = &owner.contact {
                        print!(" ({contact})");
                    }
                    println!();
                }
            }
            if !entry.tags.is_empty() {
                println!("  Tags:         {}", entry.tags.join(", "));
            }
            if let Some(env) = &entry.environment {
                println!("  Environment:  {env}");
            }
            if let Some(updated) = &entry.derived.last_updated {
                println!("  Last updated: {updated}");
            }
            if let Some(rows) = entry.derived.row_count {
                println!("  Row count:    {rows}");
            }
            if let Some(size) = entry.derived.size_bytes {
                println!("  Size:         {} bytes", size);
            }

            // Producers / consumers
            if !entry.derived.producers.is_empty() {
                println!("\n  {}:", crate::color::bold("Producers"));
                for p in &entry.derived.producers {
                    println!(
                        "    {} (node: {})",
                        display_name(&names, &p.pipeline_id),
                        p.node_id,
                    );
                }
            }
            if !entry.derived.consumers.is_empty() {
                println!("\n  {}:", crate::color::bold("Consumers"));
                for c in &entry.derived.consumers {
                    println!(
                        "    {} (node: {})",
                        display_name(&names, &c.pipeline_id),
                        c.node_id,
                    );
                }
            }

            // Columns
            if !entry.columns.is_empty() {
                println!("\n  {}:", crate::color::bold("Columns"));
                println!(
                    "    {:<24} {:<16} {:<10} DESCRIPTION",
                    "NAME", "TYPE", "NULLABLE"
                );
                println!("    {}", crate::color::dim(&"-".repeat(76)));
                for col in &entry.columns {
                    let dtype = col.data_type.as_deref().unwrap_or("-");
                    let nullable = col
                        .nullable
                        .map(|n| if n { "yes" } else { "no" })
                        .unwrap_or("-");
                    let desc = col.description.as_deref().unwrap_or("");
                    println!(
                        "    {:<24} {:<16} {:<10} {}",
                        truncate(&col.name, 24),
                        truncate(dtype, 16),
                        nullable,
                        truncate(desc, 40),
                    );
                }
            }

            if let Some(path) = &entry.annotation_file {
                println!(
                    "\n  Annotation:   {}",
                    crate::color::dim(&path.display().to_string())
                );
            }
        }
        OutputFormat::Json => {
            let out = entry_to_detail_json(entry, &names);
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn entry_to_detail_json(
    entry: &CatalogEntry,
    names: &std::collections::HashMap<PipelineId, String>,
) -> serde_json::Value {
    serde_json::json!({
        "fingerprint": entry.fingerprint.to_string(),
        "name": entry.name,
        "description": entry.description,
        "resource_type": entry.derived.resource_type,
        "owner": entry.owner.as_ref().map(|o| serde_json::json!({
            "team": o.team,
            "contact": o.contact,
        })),
        "tags": entry.tags,
        "environment": entry.environment,
        "last_updated": entry.derived.last_updated,
        "row_count": entry.derived.row_count,
        "size_bytes": entry.derived.size_bytes,
        "producers": entry.derived.producers.iter().map(|p| serde_json::json!({
            "pipeline_id": p.pipeline_id.to_string(),
            "pipeline_name": display_name(names, &p.pipeline_id),
            "node_id": p.node_id,
        })).collect::<Vec<_>>(),
        "consumers": entry.derived.consumers.iter().map(|c| serde_json::json!({
            "pipeline_id": c.pipeline_id.to_string(),
            "pipeline_name": display_name(names, &c.pipeline_id),
            "node_id": c.node_id,
        })).collect::<Vec<_>>(),
        "columns": entry.columns.iter().map(|c| serde_json::json!({
            "name": c.name,
            "data_type": c.data_type,
            "nullable": c.nullable,
            "description": c.description,
            "accepted_values": c.accepted_values,
        })).collect::<Vec<_>>(),
        "custom": entry.custom,
        "annotation_file": entry.annotation_file.as_ref().map(|p| p.display().to_string()),
    })
}

// ---------------------------------------------------------------------------
// search
// ---------------------------------------------------------------------------

fn search(
    query: &str,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let catalog = build_catalog(&*stores.lineage_store, &environment)?;
    let names = pipeline_names(&*stores.pipeline_store)?;

    let results = catalog.search(query);

    match format {
        OutputFormat::Human => {
            if results.is_empty() {
                println!("No results for `{query}` (env `{environment}`).");
                return Ok(());
            }
            println!(
                "{} result(s) for `{query}` (env `{environment}`)\n",
                results.len(),
            );
            println!(
                "{}",
                crate::color::bold(&format!(
                    "{:<40} {:<12} {}",
                    "RESOURCE", "TYPE", "FINGERPRINT"
                ))
            );
            println!("{}", crate::color::dim(&"-".repeat(80)));
            for entry in &results {
                let rtype = entry.derived.resource_type.as_deref().unwrap_or("-");
                println!(
                    "{:<40} {:<12} {}",
                    truncate(&entry.name, 40),
                    truncate(rtype, 12),
                    entry.fingerprint,
                );
            }
        }
        OutputFormat::Json => {
            let items: Vec<serde_json::Value> = results
                .iter()
                .map(|e| entry_to_summary_json(e, &names))
                .collect();
            let out = serde_json::json!({
                "environment": environment,
                "query": query,
                "count": items.len(),
                "results": items,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// describe (scaffold)
// ---------------------------------------------------------------------------

fn describe(
    fingerprint: Option<&str>,
    all: bool,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    if !all && fingerprint.is_none() {
        anyhow::bail!("provide a resource fingerprint, or use --all to scaffold all resources");
    }

    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let graph = build_graph(&*stores.lineage_store, &environment)?;
    let md_dir = metadata_dir();
    let discovered = catalog::discover_resources(&graph);
    let (existing_annotations, _errors) = catalog::load_annotations(&md_dir);

    if all {
        describe_all(&discovered, &existing_annotations, &md_dir, format)
    } else {
        let fp = ResourceFingerprint::new(fingerprint.unwrap());
        let resource = discovered
            .get(&fp)
            .ok_or_else(|| anyhow::anyhow!("resource `{}` not found in lineage", fp))?;
        describe_one(resource, &md_dir, format)
    }
}

fn describe_one(
    resource: &DiscoveredResource,
    metadata_dir: &Path,
    format: OutputFormat,
) -> Result<()> {
    let rel_path = catalog::fingerprint_to_filename(&resource.fingerprint);
    let full_path = metadata_dir.join(&rel_path);

    if full_path.exists() {
        anyhow::bail!("annotation file already exists: {}", full_path.display());
    }

    let yaml = catalog::scaffold_annotation(resource);
    if let Some(parent) = full_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    std::fs::write(&full_path, &yaml)
        .with_context(|| format!("failed to write {}", full_path.display()))?;

    match format {
        OutputFormat::Human => {
            println!("Created {}", full_path.display());
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "fingerprint": resource.fingerprint.to_string(),
                "path": full_path.display().to_string(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn describe_all(
    discovered: &std::collections::HashMap<ResourceFingerprint, DiscoveredResource>,
    existing: &std::collections::HashMap<ResourceFingerprint, catalog::AnnotationFile>,
    metadata_dir: &Path,
    format: OutputFormat,
) -> Result<()> {
    let mut created = Vec::new();
    let mut skipped = 0usize;

    // Sort by fingerprint for deterministic output.
    let mut fps: Vec<_> = discovered.keys().collect();
    fps.sort_by(|a, b| a.0.cmp(&b.0));

    for fp in fps {
        if existing.contains_key(fp) {
            skipped += 1;
            continue;
        }
        let resource = &discovered[fp];
        let rel_path = catalog::fingerprint_to_filename(&resource.fingerprint);
        let full_path = metadata_dir.join(&rel_path);

        if full_path.exists() {
            skipped += 1;
            continue;
        }

        let yaml = catalog::scaffold_annotation(resource);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }
        std::fs::write(&full_path, &yaml)
            .with_context(|| format!("failed to write {}", full_path.display()))?;
        created.push((fp.clone(), full_path));
    }

    match format {
        OutputFormat::Human => {
            if created.is_empty() {
                println!("All resources already have annotation files ({skipped} skipped).");
            } else {
                println!(
                    "Created {} annotation file(s), skipped {skipped}:\n",
                    created.len(),
                );
                for (fp, path) in &created {
                    println!("  {} → {}", fp, path.display());
                }
            }
        }
        OutputFormat::Json => {
            let items: Vec<serde_json::Value> = created
                .iter()
                .map(|(fp, path)| {
                    serde_json::json!({
                        "fingerprint": fp.to_string(),
                        "path": path.display().to_string(),
                    })
                })
                .collect();
            let out = serde_json::json!({
                "created": items,
                "skipped": skipped,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn validate(env: Option<&str>, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let environment = resolve_env(env);
    let stores = open_stores(metadata_url)?;
    let catalog = build_catalog(&*stores.lineage_store, &environment)?;

    let has_errors = !catalog.errors.is_empty();
    let has_warnings = !catalog.warnings.is_empty();

    match format {
        OutputFormat::Human => {
            if !has_errors && !has_warnings {
                println!("Catalog valid: {} resource(s), no issues.", catalog.len());
                return Ok(());
            }

            if !catalog.errors.is_empty() {
                println!(
                    "{} error(s):\n",
                    crate::color::red(&catalog.errors.len().to_string())
                );
                for err in &catalog.errors {
                    println!("  {} {err}", crate::color::red("error:"));
                }
                println!();
            }

            if !catalog.warnings.is_empty() {
                println!(
                    "{} warning(s):\n",
                    crate::color::yellow(&catalog.warnings.len().to_string())
                );
                for w in &catalog.warnings {
                    match w {
                        CatalogWarning::DanglingAnnotation { fingerprint, path } => {
                            println!(
                                "  {} dangling annotation for `{}` at {}",
                                crate::color::yellow("warn:"),
                                fingerprint,
                                path.display(),
                            );
                        }
                        CatalogWarning::DuplicateAnnotation { fingerprint, paths } => {
                            println!(
                                "  {} duplicate annotations for `{}`:",
                                crate::color::yellow("warn:"),
                                fingerprint,
                            );
                            for p in paths {
                                println!("    - {}", p.display());
                            }
                        }
                    }
                }
            }
        }
        OutputFormat::Json => {
            let errors: Vec<String> = catalog.errors.iter().map(|e| e.to_string()).collect();
            let warnings: Vec<serde_json::Value> = catalog
                .warnings
                .iter()
                .map(|w| match w {
                    CatalogWarning::DanglingAnnotation { fingerprint, path } => {
                        serde_json::json!({
                            "type": "dangling_annotation",
                            "fingerprint": fingerprint.to_string(),
                            "path": path.display().to_string(),
                        })
                    }
                    CatalogWarning::DuplicateAnnotation { fingerprint, paths } => {
                        serde_json::json!({
                            "type": "duplicate_annotation",
                            "fingerprint": fingerprint.to_string(),
                            "paths": paths.iter().map(|p| p.display().to_string()).collect::<Vec<_>>(),
                        })
                    }
                })
                .collect();
            let out = serde_json::json!({
                "environment": environment,
                "resource_count": catalog.len(),
                "valid": !has_errors && !has_warnings,
                "errors": errors,
                "warnings": warnings,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }

    if has_errors {
        anyhow::bail!("catalog validation failed with errors");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// validate
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use armillary_engine::catalog;

    #[test]
    fn describe_one_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let resource = catalog::DiscoveredResource {
            fingerprint: ResourceFingerprint::new("postgres://h:5432/db/public.orders"),
            resource_type: Some("postgres".to_string()),
            producers: Vec::new(),
            consumers: Vec::new(),
        };

        describe_one(&resource, dir.path(), OutputFormat::Human).unwrap();

        let expected = dir.path().join("postgres/h__5432__db__public.orders.yaml");
        assert!(expected.exists(), "scaffold file should exist");

        // Parse the written file to confirm it's valid YAML.
        let contents = std::fs::read_to_string(&expected).unwrap();
        let ann = catalog::parse_annotation(&contents).unwrap();
        assert_eq!(
            ann.resource.fingerprint,
            "postgres://h:5432/db/public.orders"
        );
    }

    #[test]
    fn describe_one_rejects_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let resource = catalog::DiscoveredResource {
            fingerprint: ResourceFingerprint::new("postgres://h:5432/db/public.orders"),
            resource_type: Some("postgres".to_string()),
            producers: Vec::new(),
            consumers: Vec::new(),
        };

        // First call succeeds.
        describe_one(&resource, dir.path(), OutputFormat::Human).unwrap();
        // Second call fails (file exists).
        let err = describe_one(&resource, dir.path(), OutputFormat::Human);
        assert!(err.is_err());
        assert!(
            err.unwrap_err().to_string().contains("already exists"),
            "should report file already exists"
        );
    }

    #[test]
    fn describe_all_skips_existing() {
        let dir = tempfile::tempdir().unwrap();
        let fp1 = ResourceFingerprint::new("file:///data/a.csv");
        let fp2 = ResourceFingerprint::new("file:///data/b.csv");

        let mut discovered = std::collections::HashMap::new();
        discovered.insert(
            fp1.clone(),
            catalog::DiscoveredResource {
                fingerprint: fp1.clone(),
                resource_type: Some("csv".to_string()),
                producers: Vec::new(),
                consumers: Vec::new(),
            },
        );
        discovered.insert(
            fp2.clone(),
            catalog::DiscoveredResource {
                fingerprint: fp2.clone(),
                resource_type: Some("csv".to_string()),
                producers: Vec::new(),
                consumers: Vec::new(),
            },
        );

        // Pre-create annotation for fp1.
        let mut existing = std::collections::HashMap::new();
        existing.insert(
            fp1.clone(),
            catalog::AnnotationFile {
                annotation: armillary_engine::ResourceAnnotation {
                    resource: armillary_engine::AnnotationResource {
                        fingerprint: fp1.0.clone(),
                        environment: None,
                    },
                    name: None,
                    description: None,
                    owner: None,
                    tags: Vec::new(),
                    columns: std::collections::BTreeMap::new(),
                    custom: std::collections::BTreeMap::new(),
                    sla: None,
                },
                path: dir.path().join("files/data__a.csv.yaml"),
            },
        );

        describe_all(&discovered, &existing, dir.path(), OutputFormat::Human).unwrap();

        // Only fp2 should have been created.
        let b_path = dir.path().join("files/data__b.csv.yaml");
        assert!(b_path.exists(), "b.csv annotation should be created");

        let a_path = dir.path().join("files/data__a.csv.yaml");
        assert!(
            !a_path.exists(),
            "a.csv should have been skipped (existing in map)"
        );
    }
}
