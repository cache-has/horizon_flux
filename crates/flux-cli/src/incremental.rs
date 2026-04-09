// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `flux incremental` subcommands — reset/status/list/plan for incremental
//! sink materialization state (planning doc 27).

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Subcommand;

use crate::OutputFormat;
use crate::pipeline::{open_stores, resolve_pipeline};

const DEFAULT_ENV: &str = "default";

#[derive(Subcommand)]
pub enum IncrementalAction {
    /// Reset the stored incremental state for one node, forcing the next run
    /// to be a first run.
    Reset {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Node id (the sink whose state to clear).
        node_id: String,
        /// Environment scope.
        #[arg(long, short)]
        env: Option<String>,
    },
    /// Show stored incremental state for every node in a pipeline.
    Status {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Environment scope.
        #[arg(long, short)]
        env: Option<String>,
    },
    /// List incremental state across all pipelines.
    List {
        /// Optional environment filter.
        #[arg(long, short)]
        env: Option<String>,
    },
    /// Dry-run an incremental pipeline: print the projected materialization
    /// plan per sink without executing anything.
    Plan {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Environment scope.
        #[arg(long, short)]
        env: Option<String>,
    },
}

pub fn handle(
    action: IncrementalAction,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    match action {
        IncrementalAction::Reset {
            pipeline,
            node_id,
            env,
        } => reset(&pipeline, &node_id, env.as_deref(), format, metadata_url),
        IncrementalAction::Status { pipeline, env } => {
            status(&pipeline, env.as_deref(), format, metadata_url)
        }
        IncrementalAction::List { env } => list(env.as_deref(), format, metadata_url),
        IncrementalAction::Plan { pipeline, env } => {
            plan(&pipeline, env.as_deref(), format, metadata_url)
        }
    }
}

// ---------------------------------------------------------------------------
// reset
// ---------------------------------------------------------------------------

fn reset(
    pipeline_name: &str,
    node_id: &str,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let stores = open_stores(metadata_url)?;
    let record = resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
    let environment = effective_env(env, &record);

    let removed = stores
        .incremental_state_store
        .reset_state(&record.pipeline.name, node_id, &environment)
        .context("failed to reset incremental state")?;

    match format {
        OutputFormat::Human => {
            if removed {
                println!(
                    "Reset incremental state for `{}` node `{}` (env `{}`).",
                    record.pipeline.name, node_id, environment
                );
            } else {
                println!(
                    "No stored incremental state for `{}` node `{}` (env `{}`).",
                    record.pipeline.name, node_id, environment
                );
            }
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "pipeline": record.pipeline.name,
                "node_id": node_id,
                "environment": environment,
                "removed": removed,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

fn status(
    pipeline_name: &str,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let stores = open_stores(metadata_url)?;
    let record = resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
    let environment = effective_env(env, &record);

    let states = stores
        .incremental_state_store
        .list_states(Some(&environment))
        .context("failed to load incremental state")?
        .into_iter()
        .filter(|s| s.pipeline_id == record.pipeline.name)
        .collect::<Vec<_>>();

    match format {
        OutputFormat::Human => {
            if states.is_empty() {
                println!(
                    "No incremental state stored for `{}` (env `{}`).",
                    record.pipeline.name, environment
                );
                return Ok(());
            }
            print_state_table(&states);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&states)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

fn list(env: Option<&str>, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let stores = open_stores(metadata_url)?;
    let states = stores
        .incremental_state_store
        .list_states(env)
        .context("failed to load incremental state")?;

    match format {
        OutputFormat::Human => {
            if states.is_empty() {
                println!("No incremental state stored.");
                return Ok(());
            }
            print_state_table(&states);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&states)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// plan
// ---------------------------------------------------------------------------

fn plan(
    pipeline_name: &str,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let stores = open_stores(metadata_url)?;
    let record = resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
    let environment = effective_env(env, &record);

    // Validate the DAG before planning so we surface validation errors with
    // the same shape `flux run` would.
    if let Err(errors) = flux_engine::dag::validate(&record.pipeline) {
        let msgs: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        anyhow::bail!("DAG validation failed:\n{}", msgs.join("\n"));
    }

    let state_store: Arc<dyn flux_datafusion::IncrementalStateStorage> =
        Arc::clone(&stores.incremental_state_store);

    let plans = flux_datafusion::incremental_coordinator::build_plans(
        &record.pipeline,
        &record.pipeline.name,
        &environment,
        Some(&state_store),
        false,
        false,
    )
    .map_err(|e| anyhow::anyhow!("incremental plan failed: {e}"))?;

    if plans.sink_plans.is_empty() {
        match format {
            OutputFormat::Human => {
                println!(
                    "Pipeline `{}` has no incremental sinks (env `{}`).",
                    record.pipeline.name, environment
                );
            }
            OutputFormat::Json => {
                let out = serde_json::json!({
                    "pipeline": record.pipeline.name,
                    "environment": environment,
                    "incremental_sinks": [],
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            }
        }
        return Ok(());
    }

    // Build per-sink projections.
    let mut sink_views = Vec::new();
    for (sink_id, sink_plan) in &plans.sink_plans {
        let policy = &sink_plan.policy;
        let watermark = policy.watermark.as_ref();
        let stored_value = sink_plan.state.as_ref().map(|s| s.watermark_value.clone());
        let last_run_at = sink_plan.state.as_ref().map(|s| s.last_run_at_ms);
        let projected_filter = match (watermark, &stored_value) {
            (Some(wm), Some(v)) => Some(format!("{} > {}", wm.column, v)),
            (Some(wm), None) => Some(format!(
                "(first run — full read; future filter on `{}`)",
                wm.column
            )),
            _ => None,
        };
        sink_views.push(SinkView {
            node_id: sink_id.0.clone(),
            read_mode: serde_plain(&policy.read_mode),
            write_strategy: serde_plain(&policy.write_strategy),
            unique_keys: policy.unique_keys.clone().unwrap_or_default(),
            partition_column: policy.partition_column.clone(),
            watermark_column: watermark.map(|w| w.column.clone()),
            watermark_type: watermark.map(|w| serde_plain(&w.watermark_type)),
            stored_watermark: stored_value,
            last_run_at_ms: last_run_at,
            lookback: policy.lookback.clone(),
            on_schema_change: serde_plain(&policy.on_schema_change),
            first_run: serde_plain(&policy.first_run),
            projected_filter,
        });
    }
    sink_views.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    let source_views: Vec<SourceView> = plans
        .source_plans
        .iter()
        .map(|(src_id, plan)| SourceView {
            source_node_id: src_id.0.clone(),
            sink_node_id: plan.sink_node_id.0.clone(),
            column: plan.column.clone(),
            wtype: format!("{:?}", plan.wtype).to_lowercase(),
            stored_watermark: plan.state.as_ref().map(|s| s.watermark_value.clone()),
        })
        .collect();

    match format {
        OutputFormat::Human => {
            println!(
                "Incremental plan for `{}` (env `{}`):\n",
                record.pipeline.name, environment
            );
            for sink in &sink_views {
                println!("  sink `{}`", sink.node_id);
                println!("    read_mode      : {}", sink.read_mode);
                println!("    write_strategy : {}", sink.write_strategy);
                if !sink.unique_keys.is_empty() {
                    println!("    unique_keys    : {}", sink.unique_keys.join(", "));
                }
                if let Some(p) = &sink.partition_column {
                    println!("    partition_col  : {}", p);
                }
                if let Some(c) = &sink.watermark_column {
                    println!(
                        "    watermark      : {} ({})",
                        c,
                        sink.watermark_type.as_deref().unwrap_or("?")
                    );
                }
                if let Some(v) = &sink.stored_watermark {
                    println!("    stored value   : {}", v);
                } else {
                    println!("    stored value   : (none — first run)");
                }
                println!("    lookback       : {}", sink.lookback);
                println!("    first_run      : {}", sink.first_run);
                println!("    on_schema_chg  : {}", sink.on_schema_change);
                if let Some(f) = &sink.projected_filter {
                    println!("    projected filt : {}", f);
                }
                println!();
            }
            if !source_views.is_empty() {
                println!("  source filter pushdown targets:");
                for s in &source_views {
                    println!(
                        "    source `{}` ← sink `{}` ({} {}; stored: {})",
                        s.source_node_id,
                        s.sink_node_id,
                        s.column,
                        s.wtype,
                        s.stored_watermark.as_deref().unwrap_or("none"),
                    );
                }
            }
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "pipeline": record.pipeline.name,
                "environment": environment,
                "incremental_sinks": sink_views,
                "source_filter_targets": source_views,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn effective_env(env: Option<&str>, record: &flux_engine::PipelineRecord) -> String {
    env.map(String::from).unwrap_or_else(|| {
        if record.pipeline.default_environment.is_empty() {
            DEFAULT_ENV.to_string()
        } else {
            record.pipeline.default_environment.clone()
        }
    })
}

fn print_state_table(states: &[flux_datafusion::IncrementalState]) {
    println!(
        "{}",
        crate::color::bold(&format!(
            "{:<30} {:<24} {:<10} {:<18} {:<28} {:>10}",
            "PIPELINE", "NODE", "ENV", "WATERMARK COL", "WATERMARK VALUE", "ROWS"
        ))
    );
    println!("{}", crate::color::dim(&"-".repeat(120)));
    for s in states {
        println!(
            "{:<30} {:<24} {:<10} {:<18} {:<28} {:>10}",
            truncate(&s.pipeline_id, 30),
            truncate(&s.node_id, 24),
            truncate(&s.environment, 10),
            truncate(&s.watermark_column, 18),
            truncate(&s.watermark_value, 28),
            s.rows_processed,
        );
    }
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

/// Render a serde-serializable enum to its `snake_case` string form by going
/// through serde_json. Avoids hand-maintaining a second match statement that
/// can drift from the canonical serde rename.
fn serde_plain<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

#[derive(serde::Serialize)]
struct SinkView {
    node_id: String,
    read_mode: String,
    write_strategy: String,
    unique_keys: Vec<String>,
    partition_column: Option<String>,
    watermark_column: Option<String>,
    watermark_type: Option<String>,
    stored_watermark: Option<String>,
    last_run_at_ms: Option<i64>,
    lookback: String,
    on_schema_change: String,
    first_run: String,
    projected_filter: Option<String>,
}

#[derive(serde::Serialize)]
struct SourceView {
    source_node_id: String,
    sink_node_id: String,
    column: String,
    wtype: String,
    stored_watermark: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use flux_engine::materialization::{ReadMode, WriteStrategy};

    #[test]
    fn serde_plain_renders_snake_case() {
        assert_eq!(serde_plain(&ReadMode::Incremental), "incremental");
        assert_eq!(serde_plain(&WriteStrategy::DeleteInsert), "delete_insert");
    }

    #[test]
    fn truncate_handles_long_strings() {
        let r = truncate("a-very-long-identifier-string", 10);
        assert_eq!(r.chars().count(), 10);
        assert!(r.ends_with('…'));
    }
}
