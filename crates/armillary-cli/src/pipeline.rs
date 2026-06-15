// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI commands for pipeline operations: run, list, show, history, preview.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::OutputFormat;
use armillary_datafusion::SecretResolver;

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
pub(crate) fn vars_to_map_pub(vars: Vec<(String, String)>) -> HashMap<String, serde_json::Value> {
    vars_to_map(vars)
}

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

pub(crate) struct Stores {
    pub(crate) pipeline_store: Arc<dyn armillary_engine::PipelineStorage>,
    pub(crate) run_store: Arc<dyn armillary_datafusion::RunStorage>,
    pub(crate) incremental_state_store: Arc<dyn armillary_datafusion::IncrementalStateStorage>,
    pub(crate) lineage_store: Arc<dyn armillary_datafusion::LineageStorage>,
    pub(crate) connector_registry: armillary_connectors::ConnectorRegistry,
    pub(crate) output_cache: armillary_datafusion::OutputCache,
    pub(crate) column_lineage_store: Option<Arc<dyn armillary_datafusion::ColumnLineageStorage>>,
    pub(crate) openlineage_client:
        Option<Arc<armillary_observability::openlineage::OpenLineageClient>>,
}

pub(crate) fn open_stores(metadata_url: Option<&str>) -> Result<Stores> {
    let data_dir = crate::config::data_dir()?;
    let backend = crate::config::MetadataBackend::resolve(metadata_url, &data_dir)?;
    let meta = crate::config::open_stores(&backend, &data_dir)?;

    // Discover installed plugins and register their sink types alongside
    // the built-in connectors. Without this, `armillary run` would reject any
    // pipeline that uses a plugin sink with "connector ... not registered"
    // — only `armillary plugin list/check` would see plugins, not `armillary run`.
    let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    let plugin_registry = std::sync::Arc::new(armillary_plugin_host::discover_plugins(&cwd));
    let connector_registry = armillary_connectors::default_registry_with_plugins(plugin_registry);
    let output_cache = armillary_datafusion::OutputCache::new(&data_dir);

    let openlineage_client = crate::config::resolve_openlineage_config(&data_dir)
        .and_then(|cfg| armillary_observability::openlineage::OpenLineageClient::new(&cfg));

    Ok(Stores {
        pipeline_store: meta.pipeline_store,
        run_store: meta.run_store,
        incremental_state_store: meta.incremental_state_store,
        lineage_store: meta.lineage_store,
        connector_registry,
        output_cache,
        column_lineage_store: meta.column_lineage_store,
        openlineage_client,
    })
}

pub(crate) fn resolve_pipeline(
    store: &dyn armillary_engine::PipelineStorage,
    name_or_id: &str,
) -> Result<armillary_engine::PipelineRecord> {
    let record = if let Ok(id) = name_or_id.parse::<armillary_engine::PipelineId>() {
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
// `armillary run`
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn run(
    pipeline_name: &str,
    env: Option<&str>,
    vars: Vec<(String, String)>,
    dry_run: bool,
    full_refresh: bool,
    bootstrap_incremental: bool,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let stores = open_stores(metadata_url)?;
    let record = resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
    let variable_overrides = vars_to_map(vars);

    // Validate variable overrides against declared types.
    let override_errors =
        armillary_engine::variables::validate_overrides(&record.pipeline, &variable_overrides);
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
    if let Err(errors) = armillary_engine::dag::validate(&record.pipeline) {
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
        full_refresh,
        bootstrap_incremental,
        format,
    ));
    // Force-shutdown the runtime — background tasks (tokio-postgres connections,
    // etc.) can keep the process alive indefinitely otherwise.
    rt.shutdown_background();
    result
}

/// [`SecretResolver`] for CLI usage that prompts the user for the store
/// password the first time a secret reference is encountered.
struct CliSecretResolver {
    store: std::sync::Mutex<Option<armillary_secrets::SecretStore>>,
    store_path: std::path::PathBuf,
}

impl CliSecretResolver {
    fn new() -> Option<Self> {
        let path = armillary_secrets::SecretStore::default_path()?;
        if !armillary_secrets::SecretStore::is_initialized(&path) {
            return None;
        }
        Some(Self {
            store: std::sync::Mutex::new(None),
            store_path: path,
        })
    }
}

impl SecretResolver for CliSecretResolver {
    fn resolve_json(
        &self,
        value: &serde_json::Value,
        environment: Option<&str>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        // Quick check — if there are no secret refs, skip the unlock prompt.
        let json_str = serde_json::to_string(value).unwrap_or_default();
        if !armillary_secrets::has_secret_refs(&json_str) {
            return Ok(value.clone());
        }

        let mut guard = self
            .store
            .lock()
            .map_err(|e| format!("mutex poisoned: {e}"))?;
        if guard.is_none() {
            let password = rpassword::prompt_password("Secret store password: ")
                .map_err(|e| format!("failed to read password: {e}"))?;
            let store = armillary_secrets::SecretStore::open(&self.store_path, password.as_bytes())
                .map_err(|e| format!("failed to open secret store: {e}"))?;
            *guard = Some(store);
        }
        let store = guard.as_ref().unwrap();
        armillary_secrets::resolve_json_secrets(value, store, environment)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Construct the standard CLI secret resolver, mirroring the one used by
/// `armillary run`. Returns `None` if the secret store hasn't been initialized,
/// in which case callers should proceed without secret expansion (and any
/// `{{ secret:... }}` references in connector configs will surface as
/// connector errors when the connection is opened).
pub(crate) fn build_secret_resolver() -> Option<Arc<dyn SecretResolver>> {
    CliSecretResolver::new().map(|r| Arc::new(r) as Arc<dyn SecretResolver>)
}

async fn execute_pipeline(
    record: &armillary_engine::PipelineRecord,
    stores: &Stores,
    environment: String,
    variable_overrides: HashMap<String, serde_json::Value>,
    full_refresh: bool,
    bootstrap_incremental: bool,
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

    let secret_resolver: Option<Arc<dyn SecretResolver>> =
        CliSecretResolver::new().map(|r| Arc::new(r) as Arc<dyn SecretResolver>);

    let options = armillary_datafusion::ExecutionOptions {
        environment: environment.clone(),
        run_store: Some(Arc::clone(&stores.run_store)),
        cancel: Arc::new(AtomicBool::new(false)),
        environment_resolver: None,
        progress: Some(progress_tx),
        variable_overrides,
        secret_resolver,
        session_factory: Some(Arc::new(armillary_datafusion::SessionFactory::default())),
        incremental_state_store: Some(Arc::clone(&stores.incremental_state_store)),
        full_refresh,
        bootstrap_incremental,
        dry_run_no_sinks: false,
        lineage_store: Some(Arc::clone(&stores.lineage_store)),
        fingerprint_fn: Some(armillary_connectors::fingerprint::fingerprint),
        pipeline_id: Some(record.id.0.to_string()),
        column_lineage_store: stores.column_lineage_store.clone(),
        on_column_lineage_updated: None,
        triggered_by: Some("cli".into()),
        openlineage_client: stores.openlineage_client.clone(),
    };

    let result = armillary_datafusion::PipelineExecutor::execute(
        &record.pipeline,
        &provider_registry,
        &options,
    )
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

            if run.status == armillary_datafusion::RunStatus::Failed {
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

fn print_progress_event(event: &armillary_datafusion::ExecutionEvent) {
    use crate::color;
    use armillary_datafusion::ExecutionEvent;
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
        ExecutionEvent::TestNodePassed {
            node_id,
            assertions_count,
            ..
        } => {
            eprintln!(
                "  {} {node_id} — {assertions_count} assertion(s) passed",
                color::green("✓")
            );
        }
        ExecutionEvent::TestNodeFailed {
            node_id,
            severity,
            failures,
            ..
        } => {
            let marker = if *severity == armillary_engine::node::TestSeverity::Warn {
                color::yellow("⚠")
            } else {
                color::red("✗")
            };
            for f in failures {
                eprintln!("  {marker} {node_id} — {f}");
            }
        }
        ExecutionEvent::RunCompleted {
            status,
            duration_ms,
            ..
        } => {
            let status_str = status.as_str();
            let colored_status = if *status == armillary_datafusion::RunStatus::Failed {
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
        ExecutionEvent::TriggerChanged { .. } | ExecutionEvent::Backfill(_) => {
            // Not relevant to CLI pipeline run output.
        }
    }
}

// ---------------------------------------------------------------------------
// `armillary list`
// ---------------------------------------------------------------------------

pub fn list(format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let stores = open_stores(metadata_url)?;
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
// `armillary show`
// ---------------------------------------------------------------------------

pub fn show(pipeline_name: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let stores = open_stores(metadata_url)?;
    let record = resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
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
                    armillary_engine::NodeKind::Source(cfg) => {
                        format!("source ({})", cfg.connector)
                    }
                    armillary_engine::NodeKind::Transform(cfg) => {
                        format!("transform ({:?})", cfg.mode)
                    }
                    armillary_engine::NodeKind::Sink(cfg) => {
                        format!("sink ({})", cfg.connector)
                    }
                    armillary_engine::NodeKind::Test(cfg) => {
                        format!(
                            "test ({} assertions, {:?})",
                            cfg.assertions.len(),
                            cfg.severity
                        )
                    }
                    armillary_engine::NodeKind::Snippet(call) => {
                        format!("snippet ({})", call.snippet)
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
// `armillary history`
// ---------------------------------------------------------------------------

pub fn history(
    pipeline_name: &str,
    limit: u32,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let stores = open_stores(metadata_url)?;
    // Resolve the pipeline to get its canonical name.
    let record = resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;

    let runs = stores
        .run_store
        .list_runs(Some(&record.pipeline.name), limit, 0)
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
                let colored_status = if run.status == armillary_datafusion::RunStatus::Failed {
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
// `armillary preview`
// ---------------------------------------------------------------------------

pub fn preview(
    pipeline_name: &str,
    vars: Vec<(String, String)>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let stores = open_stores(metadata_url)?;
    let record = resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
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
    record: &armillary_engine::PipelineRecord,
    stores: &Stores,
    variable_overrides: HashMap<String, serde_json::Value>,
    format: OutputFormat,
) -> Result<()> {
    let provider_registry = stores.connector_registry.to_provider_registry();

    let sample = record.pipeline.sample_config.clone().unwrap_or_default();

    let options = armillary_datafusion::PreviewOptions {
        sample,
        cancel: Arc::new(AtomicBool::new(false)),
        progress: None,
        variable_overrides,
        re_execute_node: None,
    };

    let preview = armillary_datafusion::PipelineExecutor::preview(
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
                        armillary_datafusion::PreviewStatus::Skipped => {
                            eprintln!("--- {} (skipped) ---", node_name);
                            continue;
                        }
                        armillary_datafusion::PreviewStatus::NoCache => {
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
// `armillary test`
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn test(
    pipeline_name: Option<&str>,
    env: Option<&str>,
    vars: Vec<(String, String)>,
    node_filter: Option<&str>,
    all: bool,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let stores = open_stores(metadata_url)?;

    let records: Vec<armillary_engine::PipelineRecord> = if all {
        stores
            .pipeline_store
            .list(10_000, 0)
            .context("failed to list pipelines")?
    } else {
        let name = pipeline_name
            .ok_or_else(|| anyhow::anyhow!("pipeline name or UUID required (or use --all)"))?;
        vec![resolve_pipeline(&*stores.pipeline_store, name)?]
    };

    if records.is_empty() {
        anyhow::bail!("no pipelines found");
    }

    let variable_overrides = vars_to_map(vars);

    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    let result = rt.block_on(execute_tests(
        &records,
        &stores,
        env,
        variable_overrides,
        node_filter,
        format,
    ));
    rt.shutdown_background();
    result
}

/// Aggregate test outcome across one or more pipelines.
struct TestSummary {
    total: usize,
    passed: usize,
    failed_error: usize,
    failed_warn: usize,
}

impl TestSummary {
    fn new() -> Self {
        Self {
            total: 0,
            passed: 0,
            failed_error: 0,
            failed_warn: 0,
        }
    }

    fn record(&mut self, result: &armillary_datafusion::TestNodeResult) {
        self.total += 1;
        if result.passed {
            self.passed += 1;
        } else if result.severity == armillary_engine::node::TestSeverity::Error {
            self.failed_error += 1;
        } else {
            self.failed_warn += 1;
        }
    }

    fn exit_code(&self) -> i32 {
        if self.failed_error > 0 {
            crate::EXIT_TEST_FAIL
        } else if self.failed_warn > 0 {
            crate::EXIT_TEST_WARN
        } else {
            crate::EXIT_TEST_PASS
        }
    }
}

async fn execute_tests(
    records: &[armillary_engine::PipelineRecord],
    stores: &Stores,
    env: Option<&str>,
    variable_overrides: HashMap<String, serde_json::Value>,
    node_filter: Option<&str>,
    format: OutputFormat,
) -> Result<()> {
    let provider_registry = stores.connector_registry.to_provider_registry();
    let secret_resolver = build_secret_resolver();

    let mut summary = TestSummary::new();
    let mut all_results: Vec<(String, Vec<armillary_datafusion::TestNodeResult>)> = Vec::new();

    for record in records {
        let pipeline = &record.pipeline;

        // Check that the pipeline has test nodes.
        let has_tests = pipeline.nodes.iter().any(|n| n.kind.is_test());
        if !has_tests {
            if records.len() == 1 {
                eprintln!(
                    "{} pipeline `{}` has no test nodes",
                    crate::color::yellow("Warning:"),
                    pipeline.name
                );
                std::process::exit(crate::EXIT_TEST_CONFIG_ERROR);
            }
            // When running --all, silently skip pipelines without tests.
            continue;
        }

        // If --node is specified, verify it exists and is a test node.
        if let Some(filter) = node_filter {
            let found = pipeline
                .nodes
                .iter()
                .any(|n| n.kind.is_test() && (n.id.0 == filter || n.name == filter));
            if !found {
                eprintln!(
                    "{} test node `{filter}` not found in pipeline `{}`",
                    crate::color::red("Error:"),
                    pipeline.name
                );
                std::process::exit(crate::EXIT_TEST_CONFIG_ERROR);
            }
        }

        // Validate variable overrides.
        let override_errors =
            armillary_engine::variables::validate_overrides(pipeline, &variable_overrides);
        if !override_errors.is_empty() {
            anyhow::bail!("{}", override_errors.join("\n"));
        }

        // Validate connectors.
        if let Err(errors) = stores.connector_registry.validate_pipeline(pipeline) {
            anyhow::bail!("connector validation failed:\n{}", errors.join("\n"));
        }

        // Validate the DAG.
        if let Err(errors) = armillary_engine::dag::validate(pipeline) {
            let msgs: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
            anyhow::bail!("DAG validation failed:\n{}", msgs.join("\n"));
        }

        let environment = env
            .map(String::from)
            .unwrap_or_else(|| pipeline.default_environment.clone());

        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let is_human = matches!(format, OutputFormat::Human);
        let progress_handle = tokio::spawn(async move {
            while let Some(event) = progress_rx.recv().await {
                if is_human {
                    print_progress_event(&event);
                }
            }
        });

        let options = armillary_datafusion::ExecutionOptions {
            environment,
            run_store: None, // Don't record test runs in history.
            cancel: Arc::new(AtomicBool::new(false)),
            environment_resolver: None,
            progress: Some(progress_tx),
            variable_overrides: variable_overrides.clone(),
            secret_resolver: secret_resolver.clone(),
            session_factory: Some(Arc::new(armillary_datafusion::SessionFactory::default())),
            incremental_state_store: None,
            full_refresh: false,
            bootstrap_incremental: false,
            dry_run_no_sinks: true,
            lineage_store: None,
            fingerprint_fn: None,
            pipeline_id: None,
            column_lineage_store: None,
            on_column_lineage_updated: None,
            triggered_by: Some("cli".into()),
            openlineage_client: None, // Test runs don't emit lineage events.
        };

        let exec_result =
            armillary_datafusion::PipelineExecutor::execute(pipeline, &provider_registry, &options)
                .await;

        drop(options);
        let _ = progress_handle.await;

        match exec_result {
            Ok((pipeline_result, _run)) => {
                let mut test_results = pipeline_result.test_results;

                // Filter to a specific node if requested.
                if let Some(filter) = node_filter {
                    test_results.retain(|r| {
                        r.node_id.0 == filter || {
                            pipeline
                                .node(&r.node_id)
                                .map(|n| n.name == filter)
                                .unwrap_or(false)
                        }
                    });
                }

                for r in &test_results {
                    summary.record(r);
                }
                all_results.push((pipeline.name.clone(), test_results));
            }
            Err(e) => {
                // Execution failed — try to extract test results from a
                // TestAssertionFailed error.
                let error_str = format!("{e}");
                if let armillary_datafusion::error::ExecutorError::Node {
                    kind:
                        armillary_datafusion::error::NodeErrorKind::TestAssertionFailed {
                            result, ..
                        },
                    ..
                } = &e
                {
                    summary.record(result);
                    all_results.push((pipeline.name.clone(), vec![result.clone()]));
                    continue;
                }
                anyhow::bail!("execution of `{}` failed: {error_str}", pipeline.name);
            }
        }
    }

    if summary.total == 0 {
        eprintln!(
            "{} no test nodes found across {} pipeline(s)",
            crate::color::yellow("Warning:"),
            records.len()
        );
        std::process::exit(crate::EXIT_TEST_CONFIG_ERROR);
    }

    // Format output.
    match format {
        OutputFormat::Human => print_test_results_human(&all_results, &summary),
        OutputFormat::Json => print_test_results_json(&all_results, &summary)?,
    }

    let code = summary.exit_code();
    if code != 0 {
        std::process::exit(code);
    }
    Ok(())
}

fn print_test_results_human(
    results: &[(String, Vec<armillary_datafusion::TestNodeResult>)],
    summary: &TestSummary,
) {
    use crate::color;

    eprintln!();
    for (pipeline_name, test_results) in results {
        if results.len() > 1 {
            eprintln!("Pipeline: {}", color::bold(pipeline_name));
        }
        for tr in test_results {
            let status = if tr.passed {
                color::green("PASS").to_string()
            } else if tr.severity == armillary_engine::node::TestSeverity::Error {
                color::red("FAIL").to_string()
            } else {
                color::yellow("WARN").to_string()
            };
            eprintln!("  {} {}", status, tr.node_id);

            for a in &tr.assertions {
                let icon = if a.passed {
                    color::green("✓").to_string()
                } else {
                    color::red("✗").to_string()
                };
                if a.passed {
                    eprintln!("    {} {}", icon, a.name);
                } else {
                    eprintln!(
                        "    {} {} — {} violation(s)",
                        icon, a.name, a.violation_count
                    );
                    // Show violating rows inline.
                    if !a.violating_rows.is_empty() {
                        if let Ok(table) =
                            arrow::util::pretty::pretty_format_batches(&a.violating_rows)
                        {
                            for line in table.to_string().lines() {
                                eprintln!("      {line}");
                            }
                        }
                    }
                }
            }
        }
    }

    eprintln!();
    eprintln!(
        "{} {} passed, {} failed, {} warnings",
        color::bold("Tests:"),
        summary.passed,
        summary.failed_error,
        summary.failed_warn,
    );
}

fn print_test_results_json(
    results: &[(String, Vec<armillary_datafusion::TestNodeResult>)],
    summary: &TestSummary,
) -> Result<()> {
    let pipelines: Vec<serde_json::Value> = results
        .iter()
        .map(|(name, test_results)| {
            let tests: Vec<serde_json::Value> = test_results
                .iter()
                .map(|tr| {
                    let assertions: Vec<serde_json::Value> = tr
                        .assertions
                        .iter()
                        .map(|a| {
                            serde_json::json!({
                                "name": a.name,
                                "passed": a.passed,
                                "violation_count": a.violation_count,
                                "message": a.message,
                            })
                        })
                        .collect();
                    serde_json::json!({
                        "node_id": tr.node_id.0,
                        "passed": tr.passed,
                        "severity": format!("{:?}", tr.severity).to_lowercase(),
                        "assertions": assertions,
                    })
                })
                .collect();
            serde_json::json!({
                "pipeline": name,
                "tests": tests,
            })
        })
        .collect();

    let out = serde_json::json!({
        "pipelines": pipelines,
        "summary": {
            "total": summary.total,
            "passed": summary.passed,
            "failed": summary.failed_error,
            "warnings": summary.failed_warn,
        },
        "exit_code": summary.exit_code(),
    });
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
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
    #[allow(clippy::approx_constant)]
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
