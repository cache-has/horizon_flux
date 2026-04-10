// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI commands for managing backfills (planning doc 33).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context, Result};
use clap::Subcommand;
use flux_datafusion::BackfillStorage;
use flux_engine::backfill::{
    Backfill, BackfillId, BackfillProgress, BackfillStatus, DateGranularity, RangeDefinition,
};
use tokio::sync::mpsc;

use crate::OutputFormat;

#[derive(Subcommand)]
pub enum BackfillAction {
    /// Start a backfill: rerun a pipeline across a range of parameter values.
    Start {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Environment to execute in (e.g., dev, prod).
        #[arg(long, short, default_value = "dev")]
        env: String,
        /// Date range in START..END format (e.g. 2024-01-01..2024-01-31).
        #[arg(long, group = "range_kind")]
        date_range: Option<String>,
        /// Date granularity: hour, day, week, month (default: day).
        #[arg(long, default_value = "day")]
        granularity: String,
        /// Comma-separated list of values to iterate over.
        #[arg(long, group = "range_kind")]
        list: Option<String>,
        /// Variable mapping in key=$iteration.placeholder format (repeatable).
        #[arg(long, value_parser = parse_var_mapping)]
        var_mapping: Vec<(String, String)>,
        /// Maximum concurrent iterations (default: 1).
        #[arg(long, default_value_t = 1)]
        concurrency: u32,
        /// Abort on first iteration failure.
        #[arg(long)]
        fail_fast: bool,
        /// Force full refresh (default: true for backfills).
        #[arg(long, default_value_t = true)]
        full_refresh: bool,
    },
    /// List recent backfills.
    List {
        /// Filter by pipeline name or UUID.
        #[arg(long)]
        pipeline: Option<String>,
        /// Filter by status (pending, running, completed, cancelled, failed).
        #[arg(long)]
        status: Option<String>,
        /// Maximum number of backfills to show.
        #[arg(long, default_value_t = 50)]
        limit: u32,
    },
    /// Show a backfill's status with per-iteration detail.
    Status {
        /// Backfill ID (UUID).
        backfill_id: String,
    },
    /// Resume a failed or partial backfill — skips already-succeeded iterations.
    Resume {
        /// Backfill ID (UUID).
        backfill_id: String,
    },
    /// Cancel a running backfill.
    Cancel {
        /// Backfill ID (UUID).
        backfill_id: String,
    },
    /// Delete backfill history.
    Delete {
        /// Backfill ID (UUID).
        backfill_id: String,
    },
}

pub fn handle(
    action: BackfillAction,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    match action {
        BackfillAction::Start {
            pipeline,
            env,
            date_range,
            granularity,
            list,
            var_mapping,
            concurrency,
            fail_fast,
            full_refresh,
        } => start(
            &pipeline,
            &env,
            date_range.as_deref(),
            &granularity,
            list.as_deref(),
            var_mapping,
            concurrency,
            fail_fast,
            full_refresh,
            format,
            metadata_url,
        ),
        BackfillAction::List {
            pipeline,
            status,
            limit,
        } => list_backfills(
            pipeline.as_deref(),
            status.as_deref(),
            limit,
            format,
            metadata_url,
        ),
        BackfillAction::Status { backfill_id } => show_status(&backfill_id, format, metadata_url),
        BackfillAction::Resume { backfill_id } => resume(&backfill_id, format, metadata_url),
        BackfillAction::Cancel { backfill_id } => cancel(&backfill_id, format, metadata_url),
        BackfillAction::Delete { backfill_id } => delete(&backfill_id, format, metadata_url),
    }
}

/// Parse a `key=$iteration.placeholder` string into a `(key, $iteration.placeholder)` tuple.
fn parse_var_mapping(s: &str) -> Result<(String, String), String> {
    let (key, value) = s
        .split_once('=')
        .ok_or_else(|| format!("expected KEY=$iteration.PLACEHOLDER, got `{s}`"))?;
    if key.is_empty() {
        return Err("variable name cannot be empty".into());
    }
    if !value.starts_with("$iteration.") {
        return Err(format!(
            "mapping value must start with $iteration., got `{value}`"
        ));
    }
    Ok((key.to_string(), value.to_string()))
}

fn parse_granularity(s: &str) -> Result<DateGranularity> {
    match s {
        "hour" => Ok(DateGranularity::Hour),
        "day" => Ok(DateGranularity::Day),
        "week" => Ok(DateGranularity::Week),
        "month" => Ok(DateGranularity::Month),
        other => {
            anyhow::bail!("unknown granularity `{other}` (expected hour, day, week, or month)")
        }
    }
}

fn open_backfill_store(metadata_url: Option<&str>) -> Result<Arc<dyn BackfillStorage>> {
    let data_dir = crate::config::data_dir()?;
    let backend = crate::config::MetadataBackend::resolve(metadata_url, &data_dir)?;
    let stores = crate::config::open_stores(&backend, &data_dir)?;
    Ok(stores.backfill_store)
}

fn build_range_definition(
    date_range: Option<&str>,
    granularity: &str,
    list: Option<&str>,
    var_mapping: Vec<(String, String)>,
) -> Result<RangeDefinition> {
    let mapping: HashMap<String, String> = var_mapping.into_iter().collect();

    if let Some(dr) = date_range {
        let (start, end) = dr.split_once("..").ok_or_else(|| {
            anyhow::anyhow!("date range must be in START..END format, got `{dr}`")
        })?;
        let gran = parse_granularity(granularity)?;
        Ok(RangeDefinition::DateRange {
            start: start.to_string(),
            end: end.to_string(),
            granularity: gran,
            variable_mapping: mapping,
        })
    } else if let Some(vals) = list {
        let values: Vec<String> = vals.split(',').map(|s| s.trim().to_string()).collect();
        Ok(RangeDefinition::List {
            values,
            variable_mapping: mapping,
        })
    } else {
        anyhow::bail!("either --date-range or --list is required")
    }
}

#[allow(clippy::too_many_arguments)]
fn start(
    pipeline_name: &str,
    env: &str,
    date_range: Option<&str>,
    granularity: &str,
    list: Option<&str>,
    var_mapping: Vec<(String, String)>,
    concurrency: u32,
    fail_fast: bool,
    full_refresh: bool,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let stores = crate::pipeline::open_stores(metadata_url)?;
    let record = crate::pipeline::resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
    let range_definition = build_range_definition(date_range, granularity, list, var_mapping)?;

    // Validate the range expands successfully before committing.
    if !matches!(range_definition, RangeDefinition::Sql { .. }) {
        let expanded = flux_engine::backfill::expand_range(&range_definition)
            .context("failed to expand range")?;
        if expanded.is_empty() {
            anyhow::bail!("range produced zero iterations");
        }
        if matches!(format, OutputFormat::Human) {
            eprintln!(
                "Starting backfill: {} iterations, concurrency {}",
                expanded.len(),
                concurrency
            );
        }
    }

    let data_dir = crate::config::data_dir()?;
    let backend = crate::config::MetadataBackend::resolve(metadata_url, &data_dir)?;
    let meta_stores = crate::config::open_stores(&backend, &data_dir)?;
    let backfill_store = meta_stores.backfill_store;

    let now = chrono::Utc::now().to_rfc3339();
    let backfill = Backfill {
        id: BackfillId::new(),
        pipeline_id: record.pipeline.name.clone(),
        environment: env.to_string(),
        range_definition,
        concurrency,
        fail_fast,
        full_refresh,
        status: BackfillStatus::Pending,
        created_at: now,
        started_at: None,
        completed_at: None,
        created_by: Some("cli".to_string()),
    };

    let cancel = Arc::new(AtomicBool::new(false));
    let provider_registry = stores.connector_registry.to_provider_registry();

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
    let is_human = matches!(format, OutputFormat::Human);

    let base_options = flux_datafusion::ExecutionOptions {
        environment: env.to_string(),
        run_store: Some(Arc::clone(&stores.run_store)),
        cancel: Arc::clone(&cancel),
        environment_resolver: None,
        progress: None,
        variable_overrides: HashMap::new(),
        secret_resolver: None,
        session_factory: Some(Arc::new(flux_datafusion::SessionFactory::default())),
        incremental_state_store: Some(Arc::clone(&stores.incremental_state_store)),
        full_refresh,
        bootstrap_incremental: false,
        dry_run_no_sinks: false,
        lineage_store: Some(Arc::clone(&stores.lineage_store)),
        fingerprint_fn: Some(flux_connectors::fingerprint::fingerprint),
        pipeline_id: Some(record.id.0.to_string()),
        column_lineage_store: stores.column_lineage_store.clone(),
        on_column_lineage_updated: None,
    };

    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    spawn_cancel_handler(&rt, Arc::clone(&cancel));

    let opts = flux_datafusion::BackfillRunOptions {
        pipeline: record.pipeline.clone(),
        registry: Arc::new(provider_registry),
        base_options,
        backfill_store: Arc::clone(&backfill_store),
        progress: Some(progress_tx),
        cancel,
    };

    let progress_handle = rt.spawn(async move {
        while let Some(event) = progress_rx.recv().await {
            if is_human {
                print_backfill_event(&event);
            }
        }
    });

    let result = rt.block_on(flux_datafusion::backfill::start_backfill(backfill, opts));
    let _ = rt.block_on(progress_handle);
    rt.shutdown_background();

    match result {
        Ok((id, progress)) => {
            print_completion(&id, &progress, format);
            Ok(())
        }
        Err(e) => {
            anyhow::bail!("backfill failed: {e}");
        }
    }
}

fn resume(backfill_id: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let data_dir = crate::config::data_dir()?;
    let backend = crate::config::MetadataBackend::resolve(metadata_url, &data_dir)?;
    let meta_stores = crate::config::open_stores(&backend, &data_dir)?;
    let backfill_store = meta_stores.backfill_store;

    let bf_id = BackfillId(backfill_id.to_string());
    let backfill = backfill_store
        .get_backfill(&bf_id)
        .context("failed to read backfill")?
        .ok_or_else(|| anyhow::anyhow!("backfill `{backfill_id}` not found"))?;

    let stores = crate::pipeline::open_stores(metadata_url)?;
    let record = crate::pipeline::resolve_pipeline(&*stores.pipeline_store, &backfill.pipeline_id)?;

    let cancel = Arc::new(AtomicBool::new(false));
    let provider_registry = stores.connector_registry.to_provider_registry();

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
    let is_human = matches!(format, OutputFormat::Human);

    if is_human {
        eprintln!("Resuming backfill {backfill_id}...");
    }

    let base_options = flux_datafusion::ExecutionOptions {
        environment: backfill.environment.clone(),
        run_store: Some(Arc::clone(&stores.run_store)),
        cancel: Arc::clone(&cancel),
        environment_resolver: None,
        progress: None,
        variable_overrides: HashMap::new(),
        secret_resolver: None,
        session_factory: Some(Arc::new(flux_datafusion::SessionFactory::default())),
        incremental_state_store: Some(Arc::clone(&stores.incremental_state_store)),
        full_refresh: backfill.full_refresh,
        bootstrap_incremental: false,
        dry_run_no_sinks: false,
        lineage_store: Some(Arc::clone(&stores.lineage_store)),
        fingerprint_fn: Some(flux_connectors::fingerprint::fingerprint),
        pipeline_id: Some(record.id.0.to_string()),
        column_lineage_store: stores.column_lineage_store.clone(),
        on_column_lineage_updated: None,
    };

    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    spawn_cancel_handler(&rt, Arc::clone(&cancel));

    let opts = flux_datafusion::BackfillRunOptions {
        pipeline: record.pipeline.clone(),
        registry: Arc::new(provider_registry),
        base_options,
        backfill_store: Arc::clone(&backfill_store),
        progress: Some(progress_tx),
        cancel,
    };

    let progress_handle = rt.spawn(async move {
        while let Some(event) = progress_rx.recv().await {
            if is_human {
                print_backfill_event(&event);
            }
        }
    });

    let result = rt.block_on(flux_datafusion::backfill::resume_backfill(&bf_id, opts));
    let _ = rt.block_on(progress_handle);
    rt.shutdown_background();

    match result {
        Ok((id, progress)) => {
            print_completion(&id, &progress, format);
            Ok(())
        }
        Err(e) => {
            anyhow::bail!("backfill resume failed: {e}");
        }
    }
}

fn cancel(backfill_id: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let store = open_backfill_store(metadata_url)?;
    let bf_id = BackfillId(backfill_id.to_string());

    let backfill = store
        .get_backfill(&bf_id)
        .context("failed to read backfill")?
        .ok_or_else(|| anyhow::anyhow!("backfill `{backfill_id}` not found"))?;

    match backfill.status {
        BackfillStatus::Pending | BackfillStatus::Running => {}
        other => {
            anyhow::bail!("cannot cancel backfill in '{}' status", other.as_str());
        }
    }

    flux_datafusion::backfill::cancel_backfill(&bf_id, &*store)
        .context("failed to cancel backfill")?;

    match format {
        OutputFormat::Human => println!("Cancelled backfill {backfill_id}"),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "id": backfill_id, "status": "cancelled"
                }))?
            );
        }
    }
    Ok(())
}

fn delete(backfill_id: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let store = open_backfill_store(metadata_url)?;
    let bf_id = BackfillId(backfill_id.to_string());

    // Refuse to delete a running backfill.
    if let Some(backfill) = store
        .get_backfill(&bf_id)
        .context("failed to read backfill")?
    {
        if backfill.status == BackfillStatus::Running {
            anyhow::bail!("cannot delete a running backfill — cancel it first");
        }
    }

    let deleted = store
        .delete_backfill(&bf_id)
        .context("failed to delete backfill")?;

    if !deleted {
        anyhow::bail!("backfill `{backfill_id}` not found");
    }

    match format {
        OutputFormat::Human => println!("Deleted backfill {backfill_id}"),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "deleted": backfill_id }))?
            );
        }
    }
    Ok(())
}

fn list_backfills(
    pipeline: Option<&str>,
    status: Option<&str>,
    limit: u32,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let store = open_backfill_store(metadata_url)?;

    let status_filter = status
        .map(|s| {
            BackfillStatus::parse(s)
                .ok_or_else(|| anyhow::anyhow!("unknown status `{s}` (expected pending, running, completed, cancelled, or failed)"))
        })
        .transpose()?;

    let backfills = store
        .list_backfills(pipeline, status_filter, limit)
        .context("failed to list backfills")?;

    match format {
        OutputFormat::Human => {
            if backfills.is_empty() {
                println!("No backfills found.");
                return Ok(());
            }
            println!(
                "{}",
                crate::color::bold(&format!(
                    "{:<38} {:<20} {:<12} {:<10} {:<12} {}",
                    "ID", "PIPELINE", "STATUS", "ENV", "CONCURRENCY", "CREATED"
                ))
            );
            println!("{}", crate::color::dim(&"-".repeat(110)));
            for bf in &backfills {
                println!(
                    "{:<38} {:<20} {:<12} {:<10} {:<12} {}",
                    bf.id,
                    truncate(&bf.pipeline_id, 19),
                    bf.status.as_str(),
                    truncate(&bf.environment, 9),
                    bf.concurrency,
                    &bf.created_at[..19.min(bf.created_at.len())],
                );
            }
        }
        OutputFormat::Json => {
            let items: Vec<_> = backfills
                .iter()
                .map(|bf| serde_json::to_value(bf).unwrap())
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "backfills": items }))?
            );
        }
    }
    Ok(())
}

fn show_status(backfill_id: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let store = open_backfill_store(metadata_url)?;
    let bf_id = BackfillId(backfill_id.to_string());

    let backfill = store
        .get_backfill(&bf_id)
        .context("failed to read backfill")?
        .ok_or_else(|| anyhow::anyhow!("backfill `{backfill_id}` not found"))?;

    let progress = store
        .get_progress(&bf_id)
        .context("failed to read backfill progress")?;

    let iterations = store
        .list_iterations(&bf_id)
        .context("failed to read iterations")?;

    match format {
        OutputFormat::Human => {
            println!("Backfill:     {}", backfill.id);
            println!("Pipeline:     {}", backfill.pipeline_id);
            println!("Environment:  {}", backfill.environment);
            println!("Status:       {}", backfill.status.as_str());
            println!("Concurrency:  {}", backfill.concurrency);
            println!("Fail fast:    {}", backfill.fail_fast);
            println!("Full refresh: {}", backfill.full_refresh);
            println!("Created:      {}", backfill.created_at);
            if let Some(ref t) = backfill.started_at {
                println!("Started:      {}", t);
            }
            if let Some(ref t) = backfill.completed_at {
                println!("Completed:    {}", t);
            }

            println!();
            println!(
                "Progress: {}/{} succeeded, {} failed, {} running, {} pending, {} skipped",
                progress.succeeded,
                progress.total,
                progress.failed,
                progress.running,
                progress.pending,
                progress.skipped,
            );

            if !iterations.is_empty() {
                println!();
                println!(
                    "{}",
                    crate::color::bold(&format!(
                        "{:<6} {:<24} {:<12} {:<38} {}",
                        "INDEX", "KEY", "STATUS", "RUN ID", "ERROR"
                    ))
                );
                println!("{}", crate::color::dim(&"-".repeat(100)));
                for iter in &iterations {
                    println!(
                        "{:<6} {:<24} {:<12} {:<38} {}",
                        iter.iteration_index,
                        truncate(&iter.iteration_key, 23),
                        iter.status.as_str(),
                        iter.run_id.as_deref().unwrap_or("-"),
                        iter.error.as_deref().unwrap_or(""),
                    );
                }
            }
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "backfill": serde_json::to_value(&backfill)?,
                "progress": serde_json::to_value(&progress)?,
                "iterations": iterations.iter()
                    .map(|i| serde_json::to_value(i).unwrap())
                    .collect::<Vec<_>>(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn print_backfill_event(event: &flux_datafusion::BackfillEvent) {
    use flux_datafusion::BackfillEvent;
    match event {
        BackfillEvent::BackfillStarted {
            total_iterations, ..
        } => {
            eprintln!(
                "  {} Backfill started ({total_iterations} iterations)",
                crate::color::bold("▶"),
            );
        }
        BackfillEvent::IterationStarted {
            iteration_key,
            iteration_index,
            ..
        } => {
            eprintln!(
                "  {} [{iteration_index}] {iteration_key}",
                crate::color::dim("⟳"),
            );
        }
        BackfillEvent::IterationCompleted {
            iteration_key,
            iteration_index,
            run_id,
            ..
        } => {
            eprintln!(
                "  {} [{iteration_index}] {iteration_key} → run {run_id}",
                crate::color::green("✓"),
            );
        }
        BackfillEvent::IterationFailed {
            iteration_key,
            iteration_index,
            error,
            ..
        } => {
            eprintln!(
                "  {} [{iteration_index}] {iteration_key}: {error}",
                crate::color::red("✗"),
            );
        }
        BackfillEvent::IterationSkipped {
            iteration_key,
            iteration_index,
            ..
        } => {
            eprintln!(
                "  {} [{iteration_index}] {iteration_key} (already succeeded)",
                crate::color::dim("⊘"),
            );
        }
        BackfillEvent::BackfillCompleted { progress, .. } => {
            eprintln!(
                "  {} Backfill complete: {}/{} succeeded, {} failed",
                crate::color::green("■"),
                progress.succeeded,
                progress.total,
                progress.failed,
            );
        }
        BackfillEvent::BackfillCancelled { .. } => {
            eprintln!("  {} Backfill cancelled", crate::color::yellow("■"),);
        }
    }
}

fn print_completion(id: &BackfillId, progress: &BackfillProgress, format: OutputFormat) {
    match format {
        OutputFormat::Human => {
            println!(
                "\nBackfill {}: {}/{} succeeded, {} failed, {} skipped",
                id, progress.succeeded, progress.total, progress.failed, progress.skipped,
            );
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "backfill_id": id.0,
                "progress": serde_json::to_value(progress).unwrap(),
            });
            println!("{}", serde_json::to_string_pretty(&out).unwrap());
        }
    }
}

/// Spawn a tokio task that sets the cancellation flag on Ctrl-C.
fn spawn_cancel_handler(rt: &tokio::runtime::Runtime, cancel: Arc<AtomicBool>) {
    rt.spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!(
            "\nReceived interrupt — cancelling backfill (in-flight iterations will complete)..."
        );
        cancel.store(true, std::sync::atomic::Ordering::Relaxed);
    });
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max - 1])
    }
}
