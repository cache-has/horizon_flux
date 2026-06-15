// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `armillary udf` — inspect reusable SQL UDFs declared in a pipeline's
//! `udfs_dir` (planning doc 29, Layer 1).

use std::path::PathBuf;

use anyhow::{Context, Result};
use armillary_datafusion::UdfRegistry;
use clap::Subcommand;

use crate::OutputFormat;

#[derive(Subcommand)]
pub enum UdfAction {
    /// List the UDFs declared in a pipeline's `udfs_dir`.
    List {
        /// Path to a pipeline JSON file.
        pipeline: PathBuf,
    },
}

pub fn handle(action: UdfAction, format: OutputFormat) -> Result<()> {
    match action {
        UdfAction::List { pipeline } => list(&pipeline, format),
    }
}

fn list(pipeline_path: &std::path::Path, format: OutputFormat) -> Result<()> {
    let json = std::fs::read_to_string(pipeline_path)
        .with_context(|| format!("could not read pipeline file `{}`", pipeline_path.display()))?;
    let pipeline = armillary_engine::Pipeline::from_json(&json)
        .with_context(|| format!("invalid pipeline file `{}`", pipeline_path.display()))?;

    let dir = match pipeline.udfs_dir.as_deref() {
        Some(d) => d,
        None => {
            match format {
                OutputFormat::Human => {
                    println!("Pipeline `{}` has no `udfs_dir` configured.", pipeline.name)
                }
                OutputFormat::Json => println!("{}", serde_json::json!({"udfs": []})),
            }
            return Ok(());
        }
    };
    let registry = UdfRegistry::load_from_dir(std::path::Path::new(dir))
        .with_context(|| format!("failed to load UDFs from `{dir}`"))?;

    match format {
        OutputFormat::Human => {
            if registry.is_empty() {
                println!("No UDFs found in `{dir}`.");
            } else {
                println!("UDFs in `{dir}`:");
                for def in registry.iter() {
                    println!(
                        "  {}    [{}]",
                        def.signature(),
                        def.source_path
                            .file_name()
                            .map(|n| n.to_string_lossy().into_owned())
                            .unwrap_or_default()
                    );
                }
            }
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "udfs": registry
                    .iter()
                    .map(|d| serde_json::json!({
                        "name": d.name,
                        "signature": d.signature(),
                        "params": d.params.iter()
                            .map(|p| serde_json::json!({"name": p.name, "type": p.data_type}))
                            .collect::<Vec<_>>(),
                        "return_type": d.return_type,
                        "source": d.source_path.display().to_string(),
                    }))
                    .collect::<Vec<_>>()
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}
