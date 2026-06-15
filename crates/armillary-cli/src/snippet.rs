// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `armillary snippet` — inspect and expand reusable pipeline snippets declared in
//! a pipeline's `snippets_dir` (planning doc 29, Layer 2).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use armillary_engine::{Pipeline, SnippetRegistry};
use clap::Subcommand;

use crate::OutputFormat;

#[derive(Subcommand)]
pub enum SnippetAction {
    /// List the snippets declared in a pipeline's `snippets_dir`.
    List {
        /// Path to a pipeline JSON file.
        pipeline: PathBuf,
    },
    /// Print the pipeline with all snippet call sites expanded.
    Expand {
        /// Path to a pipeline JSON file.
        pipeline: PathBuf,
    },
}

pub fn handle(action: SnippetAction, format: OutputFormat) -> Result<()> {
    match action {
        SnippetAction::List { pipeline } => list(&pipeline, format),
        SnippetAction::Expand { pipeline } => expand(&pipeline, format),
    }
}

fn resolve_snippets_dir(pipeline: &Pipeline, pipeline_path: &Path) -> Option<PathBuf> {
    let dir = pipeline.snippets_dir.as_deref()?;
    let raw = Path::new(dir);
    if raw.is_absolute() {
        Some(raw.to_path_buf())
    } else {
        let base = pipeline_path.parent().unwrap_or_else(|| Path::new("."));
        Some(base.join(raw))
    }
}

fn list(pipeline_path: &Path, format: OutputFormat) -> Result<()> {
    let json = std::fs::read_to_string(pipeline_path)
        .with_context(|| format!("could not read pipeline file `{}`", pipeline_path.display()))?;
    // Parse directly without expansion so we can see the raw `snippets_dir`.
    let pipeline: Pipeline = serde_json::from_str(&json)
        .with_context(|| format!("invalid pipeline JSON `{}`", pipeline_path.display()))?;

    let Some(dir) = resolve_snippets_dir(&pipeline, pipeline_path) else {
        match format {
            OutputFormat::Human => println!(
                "Pipeline `{}` has no `snippets_dir` configured.",
                pipeline.name
            ),
            OutputFormat::Json => println!("{}", serde_json::json!({"snippets": []})),
        }
        return Ok(());
    };

    let registry = SnippetRegistry::load_from_dir(&dir)
        .with_context(|| format!("failed to load snippets from `{}`", dir.display()))?;

    match format {
        OutputFormat::Human => {
            if registry.is_empty() {
                println!("No snippets found in `{}`.", dir.display());
            } else {
                println!("Snippets in `{}`:", dir.display());
                for (name, def, path) in registry.iter() {
                    let params = def
                        .params
                        .iter()
                        .map(|(k, t)| format!("{k}: {}", t.as_str()))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let outputs = def.outputs.join(", ");
                    println!(
                        "  {name}({params}) → [{outputs}]    [{}]",
                        path.file_name()
                            .map(|n| n.to_string_lossy().into_owned())
                            .unwrap_or_default()
                    );
                }
            }
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "snippets": registry
                    .iter()
                    .map(|(name, def, path)| serde_json::json!({
                        "name": name,
                        "params": def.params.iter()
                            .map(|(k, t)| serde_json::json!({"name": k, "type": t.as_str()}))
                            .collect::<Vec<_>>(),
                        "outputs": def.outputs,
                        "source": path.display().to_string(),
                    }))
                    .collect::<Vec<_>>()
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn expand(pipeline_path: &Path, format: OutputFormat) -> Result<()> {
    let json = std::fs::read_to_string(pipeline_path)
        .with_context(|| format!("could not read pipeline file `{}`", pipeline_path.display()))?;
    let base = pipeline_path.parent().unwrap_or_else(|| Path::new("."));
    let pipeline = Pipeline::from_json_at_path(&json, base)
        .with_context(|| format!("failed to load `{}`", pipeline_path.display()))?;
    match format {
        OutputFormat::Human | OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&pipeline)?);
        }
    }
    Ok(())
}
