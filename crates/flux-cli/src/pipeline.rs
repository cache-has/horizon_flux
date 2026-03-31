// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI commands for pipeline operations: run, list, show, history, preview.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::OutputFormat;

/// Parse a `key=value` string into a tuple. Used by clap's `value_parser`.
pub fn parse_var(s: &str) -> Result<(String, String), String> {
    let (key, value) = s
        .split_once('=')
        .ok_or_else(|| format!("expected KEY=VALUE, got `{s}`"))?;
    if key.is_empty() {
        return Err("variable name cannot be empty".into());
    }
    Ok((key.to_string(), value.to_string()))
}

/// Convert CLI `--var` pairs into a `HashMap<String, serde_json::Value>`.
///
/// Tries to parse values as JSON literals (numbers, booleans, null) first,
/// falling back to string.
fn vars_to_map(vars: Vec<(String, String)>) -> HashMap<String, serde_json::Value> {
    vars.into_iter()
        .map(|(k, v)| {
            let json_val = serde_json::from_str::<serde_json::Value>(&v)
                .ok()
                .filter(|val| !val.is_object() && !val.is_array())
                .unwrap_or(serde_json::Value::String(v));
            (k, json_val)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

struct Stores {
    pipeline_store: flux_engine::PipelineStore,
    run_store: Arc<flux_datafusion::RunStore>,
    connector_registry: flux_connectors::ConnectorRegistry,
    output_cache: flux_datafusion::OutputCache,
}

fn open_stores() -> Result<Stores> {
    let data_dir = dirs::home_dir()
        .context("could not determine home directory")?
        .join(".horizon-flux");
    std::fs::create_dir_all(&data_dir).context("failed to create data directory")?;
    let pipelines_dir = data_dir.join("pipelines");

    let pipeline_store =
        flux_engine::PipelineStore::open(&data_dir.join("pipelines.db"), &pipelines_dir)
            .context("failed to open pipeline store")?;

    let run_store = Arc::new(
        flux_datafusion::RunStore::open(&data_dir.join("runs.db"))
            .context("failed to open run store")?,
    );

    let connector_registry = flux_connectors::default_registry();
    let output_cache = flux_datafusion::OutputCache::new(&data_dir);

    Ok(Stores {
        pipeline_store,
        run_store,
        connector_registry,
        output_cache,
    })
}

fn resolve_pipeline(
    store: &flux_engine::PipelineStore,
    name_or_id: &str,
) -> Result<flux_engine::PipelineRecord> {
    let record = if let Ok(id) = name_or_id.parse::<flux_engine::PipelineId>() {
        store.get(&id).context("failed to read pipeline")?
    } else {
        store
            .get_by_name(name_or_id)
            .context("failed to read pipeline")?
    };
    record.ok_or_else(|| anyhow::anyhow!("pipeline `{name_or_id}` not found"))
}

fn format_timestamp(ts: std::time::SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Local> = ts.into();
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn format_duration_ms(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms}ms")
    } else if ms < 60_000 {
        format!("{:.1}s", ms as f64 / 1_000.0)
    } else {
        let mins = ms / 60_000;
        let secs = (ms % 60_000) / 1_000;
        format!("{mins}m {secs}s")
    }
}

// ---------------------------------------------------------------------------
// `flux run`
// ---------------------------------------------------------------------------

pub fn run(
    pipeline_name: &str,
    env: Option<&str>,
    vars: Vec<(String, String)>,
    dry_run: bool,
    format: OutputFormat,
) -> Result<()> {
    let stores = open_stores()?;
    let record = resolve_pipeline(&stores.pipeline_store, pipeline_name)?;
    let variable_overrides = vars_to_map(vars);

    // Validate variable overrides against declared types.
    let override_errors =
        flux_engine::variables::validate_overrides(&record.pipeline, &variable_overrides);
    if !override_errors.is_empty() {
        anyhow::bail!("{}", override_errors.join("\n"));
    }

    // Validate connectors.
    if let Err(errors) = stores
        .connector_registry
        .validate_pipeline(&record.pipeline)
    {
        anyhow::bail!("connector validation failed:\n{}", errors.join("\n"));
    }

    // Validate the DAG.
    if let Err(errors) = flux_engine::dag::validate(&record.pipeline) {
        let msgs: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        anyhow::bail!("DAG validation failed:\n{}", msgs.join("\n"));
    }

    if dry_run {
        match format {
            OutputFormat::Human => {
                println!(
                    "Pipeline `{}` is valid ({} nodes, {} edges)",
                    record.pipeline.name,
                    record.pipeline.nodes.len(),
                    record.pipeline.edges.len(),
                );
            }
            OutputFormat::Json => {
                let out = serde_json::json!({
                    "valid": true,
                    "pipeline": record.pipeline.name,
                    "nodes": record.pipeline.nodes.len(),
                    "edges": record.pipeline.edges.len(),
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            }
        }
        return Ok(());
    }

    // Execute for real.
    let environment = env
        .map(String::from)
        .unwrap_or_else(|| record.pipeline.default_environment.clone());

    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    let result = rt.block_on(execute_pipeline(
        &record,
        &stores,
        environment,
        variable_overrides,
        format,
    ));
    // Force-shutdown the runtime — background tasks (tokio-postgres connections,
    // etc.) can keep the process alive indefinitely otherwise.
    rt.shutdown_background();
    result
}

async fn execute_pipeline(
    record: &flux_engine::PipelineRecord,
    stores: &Stores,
    environment: String,
    variable_overrides: HashMap<String, serde_json::Value>,
    format: OutputFormat,
) -> Result<()> {
    let provider_registry = stores.connector_registry.to_provider_registry();

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();

    // Spawn a task that prints progress events to stderr.
    let is_human = matches!(format, OutputFormat::Human);
    let progress_handle = tokio::spawn(async move {
        while let Some(event) = progress_rx.recv().await {
            if is_human {
                print_progress_event(&event);
            }
        }
    });

    let options = flux_datafusion::ExecutionOptions {
        environment: environment.clone(),
        run_store: Some(Arc::clone(&stores.run_store)),
        cancel: Arc::new(AtomicBool::new(false)),
        environment_resolver: None,
        progress: Some(progress_tx),
        variable_overrides,
    };

    let result =
        flux_datafusion::PipelineExecutor::execute(&record.pipeline, &provider_registry, &options)
            .await;

    // Drop the progress sender so the receiver loop terminates.
    drop(options);

    // Wait for progress printer to finish.
    let _ = progress_handle.await;

    // Update run metadata — best effort.
    if let Err(e) = stores.pipeline_store.record_run(&record.id) {
        tracing::warn!("failed to record run metadata: {e}");
    }

    match result {
        Ok((pipeline_result, run)) => {
            // Cache node outputs for preview — best-effort.
            if let Err(e) = stores
                .output_cache
                .cache_pipeline_outputs(&record.pipeline, &pipeline_result.node_outputs)
            {
                tracing::warn!("failed to cache node outputs: {e}");
            }

            match format {
                OutputFormat::Human => {
                    let status = run.status.as_str();
                    let duration = run
                        .duration_ms()
                        .map(format_duration_ms)
                        .unwrap_or_else(|| "n/a".into());
                    println!("\nRun {}: {} ({})", run.id, status, duration);
                }
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&run)?);
                }
            }

            if run.status == flux_datafusion::RunStatus::Failed {
                std::process::exit(crate::EXIT_PIPELINE_FAILURE as i32);
            }
            Ok(())
        }
        Err(e) => {
            // The executor itself failed (not a pipeline-level failure).
            anyhow::bail!("execution failed: {e}");
        }
    }
}

fn print_progress_event(event: &flux_datafusion::ExecutionEvent) {
    use crate::color;
    use flux_datafusion::ExecutionEvent;
    match event {
        ExecutionEvent::RunStarted { pipeline_name, .. } => {
            eprintln!("Running {}...", color::bold(&format!("`{pipeline_name}`")));
        }
        ExecutionEvent::NodeStarted { node_id, .. } => {
            eprintln!("  ▶ {node_id}");
        }
        ExecutionEvent::NodeCompleted {
            node_id,
            rows_out,
            duration_ms,
            ..
        } => {
            eprintln!(
                "  {} {node_id} — {rows_out} rows ({})",
                color::green("✓"),
                color::dim(&format_duration_ms(*duration_ms))
            );
        }
        ExecutionEvent::NodeFailed { node_id, error, .. } => {
            eprintln!("  {} {node_id} — {error}", color::red("✗"));
        }
        ExecutionEvent::RunCompleted {
            status,
            duration_ms,
            ..
        } => {
            let status_str = status.as_str();
            let colored_status = if *status == flux_datafusion::RunStatus::Failed {
                color::red(status_str).to_string()
            } else {
                color::green(status_str).to_string()
            };
            eprintln!(
                "Finished: {} ({})",
                colored_status,
                color::dim(&format_duration_ms(*duration_ms))
            );
        }
    }
}

// ---------------------------------------------------------------------------
// `flux list`
// ---------------------------------------------------------------------------

pub fn list(format: OutputFormat) -> Result<()> {
    let stores = open_stores()?;
    let records = stores
        .pipeline_store
        .list(1000, 0)
        .context("failed to list pipelines")?;

    match format {
        OutputFormat::Human => {
            if records.is_empty() {
                println!("No pipelines found.");
                return Ok(());
            }
            // Table header
            println!(
                "{}",
                crate::color::bold(&format!(
                    "{:<30} {:>5} {:>5} {:>20} {:>6}",
                    "NAME", "NODES", "EDGES", "LAST RUN", "RUNS"
                ))
            );
            println!("{}", crate::color::dim(&"-".repeat(70)));
            for r in &records {
                let last_run = r
                    .last_run_at
                    .map(format_timestamp)
                    .unwrap_or_else(|| "never".into());
                println!(
                    "{:<30} {:>5} {:>5} {:>20} {:>6}",
                    truncate(&r.pipeline.name, 30),
                    r.pipeline.nodes.len(),
                    r.pipeline.edges.len(),
                    last_run,
                    r.run_count,
                );
            }
        }
        OutputFormat::Json => {
            let out: Vec<serde_json::Value> = records
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "id": r.id.to_string(),
                        "name": r.pipeline.name,
                        "nodes": r.pipeline.nodes.len(),
                        "edges": r.pipeline.edges.len(),
                        "last_run_at": r.last_run_at.map(system_time_to_ms),
                        "run_count": r.run_count,
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `flux show`
// ---------------------------------------------------------------------------

pub fn show(pipeline_name: &str, format: OutputFormat) -> Result<()> {
    let stores = open_stores()?;
    let record = resolve_pipeline(&stores.pipeline_store, pipeline_name)?;
    let p = &record.pipeline;

    match format {
        OutputFormat::Human => {
            println!("Pipeline: {}", p.name);
            println!("ID:       {}", record.id);
            println!("Version:  {}", p.version);
            println!("Env:      {}", p.default_environment);
            println!(
                "Last run: {}",
                record
                    .last_run_at
                    .map(format_timestamp)
                    .unwrap_or_else(|| "never".into())
            );
            println!("Runs:     {}", record.run_count);

            // Variables
            if !p.variables.is_empty() {
                println!("\nVariables:");
                for (name, var) in &p.variables {
                    let default = var
                        .default
                        .as_ref()
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "(none)".into());
                    println!("  {name}: {:?} (default: {default})", var.var_type);
                }
            }

            // Nodes
            println!("\nNodes ({}):", p.nodes.len());
            for node in &p.nodes {
                let kind_str = match &node.kind {
                    flux_engine::NodeKind::Source(cfg) => {
                        format!("source ({})", cfg.connector)
                    }
                    flux_engine::NodeKind::Transform(cfg) => {
                        format!("transform ({:?})", cfg.mode)
                    }
                    flux_engine::NodeKind::Sink(cfg) => {
                        format!("sink ({})", cfg.connector)
                    }
                };
                println!("  {} — {}", node.name, kind_str);
            }

            // Edges
            if !p.edges.is_empty() {
                println!("\nConnections ({}):", p.edges.len());
                for edge in &p.edges {
                    println!("  {} → {}", edge.from, edge.to);
                }
            }

            // Environment overrides
            if !p.environment_overrides.is_empty() {
                println!("\nEnvironment overrides:");
                for (env_name, overrides) in &p.environment_overrides {
                    let node_ids: Vec<&String> = overrides.keys().collect();
                    println!("  {env_name}: {} node(s)", node_ids.len());
                }
            }
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "id": record.id.to_string(),
                "pipeline": p,
                "created_at": system_time_to_ms(record.created_at),
                "updated_at": system_time_to_ms(record.updated_at),
                "last_run_at": record.last_run_at.map(system_time_to_ms),
                "run_count": record.run_count,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `flux history`
// ---------------------------------------------------------------------------

pub fn history(pipeline_name: &str, limit: u32, format: OutputFormat) -> Result<()> {
    let stores = open_stores()?;
    // Resolve the pipeline to get its canonical name.
    let record = resolve_pipeline(&stores.pipeline_store, pipeline_name)?;

    let runs = stores
        .run_store
        .list_runs(Some(&record.pipeline.name), limit)
        .context("failed to list runs")?;

    match format {
        OutputFormat::Human => {
            if runs.is_empty() {
                println!("No runs found for `{}`.", record.pipeline.name);
                return Ok(());
            }
            println!(
                "{}",
                crate::color::bold(&format!(
                    "{:<36}  {:<10}  {:<20}  {:>10}",
                    "RUN ID", "STATUS", "STARTED", "DURATION"
                ))
            );
            println!("{}", crate::color::dim(&"-".repeat(80)));
            for run in &runs {
                let started = run
                    .start_time
                    .map(format_timestamp)
                    .unwrap_or_else(|| "pending".into());
                let duration = run
                    .duration_ms()
                    .map(format_duration_ms)
                    .unwrap_or_else(|| "-".into());
                let status_str = run.status.as_str();
                let colored_status = if run.status == flux_datafusion::RunStatus::Failed {
                    crate::color::red(status_str).to_string()
                } else {
                    crate::color::green(status_str).to_string()
                };
                println!(
                    "{:<36}  {:<10}  {:<20}  {:>10}",
                    run.id, colored_status, started, duration,
                );
            }
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&runs)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `flux preview`
// ---------------------------------------------------------------------------

pub fn preview(
    pipeline_name: &str,
    vars: Vec<(String, String)>,
    format: OutputFormat,
) -> Result<()> {
    let stores = open_stores()?;
    let record = resolve_pipeline(&stores.pipeline_store, pipeline_name)?;
    let variable_overrides = vars_to_map(vars);

    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    let result = rt.block_on(execute_preview(
        &record,
        &stores,
        variable_overrides,
        format,
    ));
    rt.shutdown_background();
    result
}

async fn execute_preview(
    record: &flux_engine::PipelineRecord,
    stores: &Stores,
    variable_overrides: HashMap<String, serde_json::Value>,
    format: OutputFormat,
) -> Result<()> {
    let provider_registry = stores.connector_registry.to_provider_registry();

    let sample = record.pipeline.sample_config.clone().unwrap_or_default();

    let options = flux_datafusion::PreviewOptions {
        sample,
        cancel: Arc::new(AtomicBool::new(false)),
        progress: None,
        variable_overrides,
        re_execute_node: None,
    };

    let preview = flux_datafusion::PipelineExecutor::preview(
        &record.pipeline,
        &stores.output_cache,
        &provider_registry,
        &options,
    )
    .await
    .context("preview failed")?;

    match format {
        OutputFormat::Human => {
            for node_id in &preview.execution_order {
                if let Some(nr) = preview.nodes.get(node_id) {
                    let node_name = record
                        .pipeline
                        .node(node_id)
                        .map(|n| n.name.as_str())
                        .unwrap_or(&node_id.0);

                    match &nr.status {
                        flux_datafusion::PreviewStatus::Skipped => {
                            eprintln!("--- {} (skipped) ---", node_name);
                            continue;
                        }
                        flux_datafusion::PreviewStatus::NoCache => {
                            eprintln!(
                                "--- {} (no cached data — run the pipeline first) ---",
                                node_name
                            );
                            continue;
                        }
                        _ => {}
                    }

                    eprintln!(
                        "--- {} ({} rows, {}) ---",
                        node_name,
                        nr.row_count,
                        format_duration_ms(nr.duration.as_millis() as u64),
                    );
                    if nr.batches.is_empty() {
                        println!("(no data)");
                    } else {
                        let formatted = arrow::util::pretty::pretty_format_batches(&nr.batches)
                            .context("failed to format preview data")?;
                        println!("{formatted}");
                    }
                }
            }
            eprintln!(
                "\nPreview completed in {}",
                format_duration_ms(preview.duration.as_millis() as u64)
            );
        }
        OutputFormat::Json => {
            let nodes: Vec<serde_json::Value> = preview
                .execution_order
                .iter()
                .filter_map(|nid| {
                    preview.nodes.get(nid).map(|nr| {
                        let rows = batches_to_json_rows(&nr.batches);
                        let columns: Vec<serde_json::Value> = nr
                            .schema
                            .fields()
                            .iter()
                            .map(|f| {
                                serde_json::json!({
                                    "name": f.name(),
                                    "type": format!("{}", f.data_type()),
                                    "nullable": f.is_nullable(),
                                })
                            })
                            .collect();
                        serde_json::json!({
                            "node_id": nid.0,
                            "columns": columns,
                            "row_count": nr.row_count,
                            "duration_ms": nr.duration.as_millis() as u64,
                            "rows": rows,
                            "status": nr.status,
                        })
                    })
                })
                .collect();

            let out = serde_json::json!({
                "pipeline": preview.pipeline_name,
                "nodes": nodes,
                "duration_ms": preview.duration.as_millis() as u64,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

/// Convert Arrow record batches to JSON row objects.
fn batches_to_json_rows(batches: &[arrow::array::RecordBatch]) -> Vec<serde_json::Value> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        for batch in batches {
            if let Err(e) = writer.write(batch) {
                tracing::warn!("failed to serialize Arrow batch to JSON: {e}");
                break;
            }
        }
        let _ = writer.finish();
    }
    let text = String::from_utf8(buf).unwrap_or_default();
    text.lines()
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max - 1])
    }
}

fn system_time_to_ms(t: std::time::SystemTime) -> u64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_var_valid() {
        assert_eq!(
            parse_var("key=value").unwrap(),
            ("key".into(), "value".into())
        );
    }

    #[test]
    fn parse_var_with_equals_in_value() {
        assert_eq!(
            parse_var("query=a=b").unwrap(),
            ("query".into(), "a=b".into())
        );
    }

    #[test]
    fn parse_var_empty_key_rejected() {
        assert!(parse_var("=value").is_err());
    }

    #[test]
    fn parse_var_no_equals_rejected() {
        assert!(parse_var("noequals").is_err());
    }

    #[test]
    fn vars_to_map_parses_numbers() {
        let vars = vec![
            ("count".into(), "42".into()),
            ("rate".into(), "3.14".into()),
            ("flag".into(), "true".into()),
            ("name".into(), "hello".into()),
        ];
        let map = vars_to_map(vars);
        assert_eq!(map["count"], serde_json::json!(42));
        assert_eq!(map["rate"], serde_json::json!(3.14));
        assert_eq!(map["flag"], serde_json::json!(true));
        assert_eq!(map["name"], serde_json::json!("hello"));
    }

    #[test]
    fn vars_to_map_rejects_objects_arrays() {
        let vars = vec![
            ("obj".into(), r#"{"a":1}"#.into()),
            ("arr".into(), "[1,2]".into()),
        ];
        let map = vars_to_map(vars);
        // Objects and arrays should be kept as strings
        assert_eq!(map["obj"], serde_json::json!(r#"{"a":1}"#));
        assert_eq!(map["arr"], serde_json::json!("[1,2]"));
    }

    #[test]
    fn format_duration_ms_formats_correctly() {
        assert_eq!(format_duration_ms(50), "50ms");
        assert_eq!(format_duration_ms(1500), "1.5s");
        assert_eq!(format_duration_ms(90_000), "1m 30s");
    }

    #[test]
    fn truncate_short_string() {
        assert_eq!(truncate("hi", 10), "hi");
    }

    #[test]
    fn truncate_long_string() {
        let result = truncate("a very long pipeline name", 10);
        assert_eq!(result.chars().count(), 10);
        assert!(result.ends_with('…'));
    }
}
