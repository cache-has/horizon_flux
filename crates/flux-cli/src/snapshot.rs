// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `flux snapshot` subcommands — `diff` and `history` for SCD2 snapshot
//! sinks (planning doc 28).
//!
//! `diff` runs the upstream pipeline as a true dry-run (no sink writes,
//! no run/state persistence — see `ExecutionOptions::dry_run_no_sinks`),
//! then compares the would-be-staged batch against the snapshot target's
//! current versions, classifying every business key as
//! `Unchanged | Changed | New | Gone`.
//!
//! `history` queries every historical version of a single business key
//! from the target, ordered most-recent-first.
//!
//! v1 supports the **postgresql** sink only. Parquet and plugin (DuckDB)
//! support is tracked in doc 28's "Deferred" section: each requires its
//! own sink-side reader path with non-trivial Arrow row plumbing, and is
//! best done as a follow-up.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use clap::Subcommand;
use flux_connectors::config::PostgreSqlConfig;
use flux_datafusion::SecretResolver;
use flux_engine::PipelineRecord;
use flux_engine::materialization::{ChangeDetection, SnapshotPolicy, WriteStrategy};
use flux_engine::node::{NodeKind, SinkConfig};
use flux_engine::snapshot::{FLUX_IS_CURRENT, FLUX_SCD_ID, FLUX_VALID_FROM, FLUX_VALID_TO};
use tokio_postgres::NoTls;

use crate::OutputFormat;
use crate::pipeline::{Stores, build_secret_resolver, open_stores, resolve_pipeline};

const NULL_SENTINEL: &str = "\0NULL";

#[derive(Subcommand)]
pub enum SnapshotAction {
    /// Show what `flux run` *would* change on a snapshot sink without
    /// actually writing. Executes the upstream DAG with sink writes
    /// disabled, then diffs the would-be-staged batch against the
    /// snapshot target's current versions.
    Diff {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Snapshot sink node id.
        node_id: String,
        /// Environment scope.
        #[arg(long, short)]
        env: Option<String>,
        /// Variable overrides in key=value format (repeatable).
        #[arg(long, short = 'V', value_parser = crate::pipeline::parse_var)]
        var: Vec<(String, String)>,
    },
    /// Query SCD2 version history for a single business key on a
    /// snapshot sink.
    History {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Snapshot sink node id.
        node_id: String,
        /// Business key value(s) as `column=value`. Repeat for composite
        /// keys; values must cover every column in the sink's
        /// `unique_keys`. Example: `--key tenant_id=42 --key customer_id=7`.
        #[arg(long = "key", required = true, value_parser = parse_key_pair)]
        key: Vec<(String, String)>,
        /// Environment scope.
        #[arg(long, short)]
        env: Option<String>,
    },
}

pub fn handle(
    action: SnapshotAction,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    match action {
        SnapshotAction::Diff {
            pipeline,
            node_id,
            env,
            var,
        } => diff_cmd(
            &pipeline,
            &node_id,
            env.as_deref(),
            var,
            format,
            metadata_url,
        ),
        SnapshotAction::History {
            pipeline,
            node_id,
            key,
            env,
        } => history_cmd(
            &pipeline,
            &node_id,
            key,
            env.as_deref(),
            format,
            metadata_url,
        ),
    }
}

/// Parse a `column=value` argument for `--key`. Mirrors the existing
/// `parse_var` helper but is exported as a value_parser so clap can use
/// it directly.
pub fn parse_key_pair(s: &str) -> Result<(String, String), String> {
    let (col, val) = s
        .split_once('=')
        .ok_or_else(|| format!("expected COLUMN=VALUE, got `{s}`"))?;
    if col.is_empty() {
        return Err("key column name cannot be empty".into());
    }
    Ok((col.to_string(), val.to_string()))
}

// ---------------------------------------------------------------------------
// Common: resolve sink + policy + (interpolated, secret-expanded) pg config
// ---------------------------------------------------------------------------

struct ResolvedSnapshotSink {
    node_id: String,
    unique_keys: Vec<String>,
    snapshot: SnapshotPolicy,
    /// Comparison columns used for change detection (`check_columns` or the
    /// single `updated_at_column`). Used by `diff` to project staged rows
    /// and by `history` purely as informational output.
    comparison_columns: Vec<String>,
    /// Postgres connection + table after secret expansion and variable
    /// interpolation.
    pg_config: PostgreSqlConfig,
    table: String,
}

fn resolve_snapshot_sink(
    record: &PipelineRecord,
    node_id: &str,
    environment: &str,
    variable_overrides: &HashMap<String, serde_json::Value>,
    secret_resolver: Option<&Arc<dyn SecretResolver>>,
) -> Result<ResolvedSnapshotSink> {
    let node = record
        .pipeline
        .node(&flux_engine::NodeId::new(node_id))
        .ok_or_else(|| anyhow::anyhow!("node `{node_id}` not found in pipeline"))?;

    let sink_cfg: &SinkConfig = match &node.kind {
        NodeKind::Sink(s) => s,
        _ => anyhow::bail!("node `{node_id}` is not a sink"),
    };

    let policy = sink_cfg
        .materialization
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("node `{node_id}` has no materialization block"))?;
    if !matches!(policy.write_strategy, WriteStrategy::Snapshot) {
        anyhow::bail!(
            "node `{node_id}` is not a snapshot sink (write_strategy = `{:?}`)",
            policy.write_strategy
        );
    }
    let snapshot = policy
        .snapshot
        .as_ref()
        .ok_or_else(|| {
            anyhow::anyhow!("snapshot sink `{node_id}` is missing the `snapshot:` sub-block")
        })?
        .clone();
    let unique_keys = policy
        .unique_keys
        .clone()
        .ok_or_else(|| anyhow::anyhow!("snapshot sink `{node_id}` is missing `unique_keys`"))?;
    if unique_keys.is_empty() {
        anyhow::bail!("snapshot sink `{node_id}` has empty `unique_keys`");
    }

    if sink_cfg.connector != "postgresql" {
        anyhow::bail!(
            "`flux snapshot` v1 supports the `postgresql` sink only — node `{node_id}` uses \
             connector `{}`. Parquet and plugin (DuckDB) support is tracked in the \
             deferred-tasks section of `planning/28-snapshots-scd2.md`.",
            sink_cfg.connector
        );
    }

    let comparison_columns = match snapshot.change_detection {
        ChangeDetection::Check => snapshot.check_columns.clone().ok_or_else(|| {
            anyhow::anyhow!("snapshot.check_columns is required for change_detection: check")
        })?,
        ChangeDetection::Timestamp => {
            vec![snapshot.updated_at_column.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "snapshot.updated_at_column is required for change_detection: timestamp"
                )
            })?]
        }
    };

    // Apply environment override + variable interpolation + secret resolution
    // to the connector config exactly the way the executor would, so the
    // connection string we end up with matches what `flux run` would use.
    let mut config_json = sink_cfg.config.clone();
    if let Some(overrides) = record
        .pipeline
        .environment_overrides
        .get(environment)
        .and_then(|env| env.get(node_id))
    {
        merge_override(&mut config_json, overrides);
    }
    let resolved_vars = flux_engine::variables::ResolvedVariables::resolve(
        &record.pipeline,
        variable_overrides,
        &flux_engine::variables::BuiltinContext {
            run_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
            run_id: format!("snapshot-diff-{}", chrono::Utc::now().timestamp_micros()),
            pipeline_name: record.pipeline.name.clone(),
            environment: environment.to_string(),
        },
    );
    config_json = resolved_vars.interpolate_json(&config_json);
    if let Some(resolver) = secret_resolver {
        config_json = resolver
            .resolve_json(&config_json, Some(environment))
            .map_err(|e| anyhow::anyhow!("failed to resolve secrets for sink `{node_id}`: {e}"))?;
    }
    let pg_config: PostgreSqlConfig = serde_json::from_value(config_json)
        .with_context(|| format!("invalid postgresql sink config on node `{node_id}`"))?;
    let table = pg_config
        .table
        .clone()
        .ok_or_else(|| anyhow::anyhow!("snapshot sink `{node_id}` is missing the `table` field"))?;

    Ok(ResolvedSnapshotSink {
        node_id: node_id.to_string(),
        unique_keys,
        snapshot,
        comparison_columns,
        pg_config,
        table,
    })
}

/// Mirror of `flux_datafusion::executor::merge_override` (not pub there).
fn merge_override(target: &mut serde_json::Value, overrides: &serde_json::Value) {
    use serde_json::Value;
    match (target, overrides) {
        (Value::Object(t), Value::Object(o)) => {
            for (k, v) in o {
                merge_override(t.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (slot, other) => *slot = other.clone(),
    }
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// `flux snapshot diff`
// ---------------------------------------------------------------------------

fn diff_cmd(
    pipeline_name: &str,
    node_id: &str,
    env: Option<&str>,
    vars: Vec<(String, String)>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let stores = open_stores(metadata_url)?;
    let record = resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
    let environment = env
        .map(String::from)
        .unwrap_or_else(|| record.pipeline.default_environment.clone());
    let variable_overrides = crate::pipeline::vars_to_map_pub(vars);
    let secret_resolver = build_secret_resolver();

    let resolved = resolve_snapshot_sink(
        &record,
        node_id,
        &environment,
        &variable_overrides,
        secret_resolver.as_ref(),
    )?;

    // 1. Run upstream pipeline as dry-run.
    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    let result = rt.block_on(async {
        run_upstream_dry_run(
            &record,
            &stores,
            environment.clone(),
            variable_overrides.clone(),
            secret_resolver.clone(),
        )
        .await
    });
    let pipeline_result = match result {
        Ok(r) => r,
        Err(e) => {
            rt.shutdown_background();
            return Err(e);
        }
    };

    // 2. Collect upstream-of-sink batches as the staged input.
    let sink_node_id = flux_engine::NodeId::new(node_id);
    let upstream_ids = record.pipeline.upstream_of(&sink_node_id);
    let mut staged_batches: Vec<RecordBatch> = Vec::new();
    for uid in upstream_ids {
        if let Some(batches) = pipeline_result.node_outputs.get(uid) {
            staged_batches.extend(batches.iter().cloned());
        }
    }

    // 3. Build (key, signal) tuples from staged batches.
    let staged: Vec<(Vec<String>, String)> = stringify_rows(
        &staged_batches,
        &resolved.unique_keys,
        &resolved.comparison_columns,
    )?;

    // 4. Read current target snapshot from postgres.
    let current = rt.block_on(read_current_signals_pg(&resolved));
    rt.shutdown_background();
    let current = current?;

    // 5. Classify.
    let summary = classify(staged, current);

    // 6. Render.
    render_diff(&resolved, &summary, format)
}

async fn run_upstream_dry_run(
    record: &PipelineRecord,
    stores: &Stores,
    environment: String,
    variable_overrides: HashMap<String, serde_json::Value>,
    secret_resolver: Option<Arc<dyn SecretResolver>>,
) -> Result<flux_datafusion::PipelineResult> {
    let provider_registry = stores.connector_registry.to_provider_registry();
    let options = flux_datafusion::ExecutionOptions {
        environment,
        run_store: None,
        cancel: Arc::new(AtomicBool::new(false)),
        environment_resolver: None,
        progress: None,
        variable_overrides,
        secret_resolver,
        session_factory: Some(Arc::new(flux_datafusion::SessionFactory::default())),
        incremental_state_store: None,
        full_refresh: false,
        bootstrap_incremental: false,
        dry_run_no_sinks: true,
        lineage_store: None,
        fingerprint_fn: None,
        pipeline_id: None,
        column_lineage_store: None,
        on_column_lineage_updated: None,
    };
    let (pipeline_result, _run) =
        flux_datafusion::PipelineExecutor::execute(&record.pipeline, &provider_registry, &options)
            .await
            .map_err(|e| anyhow::anyhow!("dry-run execution failed: {e}"))?;
    Ok(pipeline_result)
}

/// Stringify the unique-key columns and the comparison columns of every
/// row in `batches`. Comparison columns are joined with a `\0FS` separator
/// before hashing — but for the diff CLI we keep them as a single
/// concatenated string used purely for equality comparison.
fn stringify_rows(
    batches: &[RecordBatch],
    unique_keys: &[String],
    comparison_columns: &[String],
) -> Result<Vec<(Vec<String>, String)>> {
    let mut out: Vec<(Vec<String>, String)> = Vec::new();
    let opts = FormatOptions::default().with_null(NULL_SENTINEL);
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let key_fmts: Vec<ArrayFormatter> = unique_keys
            .iter()
            .map(|k| {
                let idx = batch
                    .schema()
                    .index_of(k)
                    .map_err(|_| anyhow::anyhow!("staged batch missing unique_key column `{k}`"))?;
                ArrayFormatter::try_new(batch.column(idx).as_ref(), &opts)
                    .map_err(|e| anyhow::anyhow!("cannot format key column `{k}`: {e}"))
            })
            .collect::<Result<_>>()?;
        let sig_fmts: Vec<ArrayFormatter> = comparison_columns
            .iter()
            .map(|c| {
                let idx = batch
                    .schema()
                    .index_of(c)
                    .map_err(|_| anyhow::anyhow!("staged batch missing comparison column `{c}`"))?;
                ArrayFormatter::try_new(batch.column(idx).as_ref(), &opts)
                    .map_err(|e| anyhow::anyhow!("cannot format comparison column `{c}`: {e}"))
            })
            .collect::<Result<_>>()?;
        for i in 0..batch.num_rows() {
            let key: Vec<String> = key_fmts.iter().map(|f| f.value(i).to_string()).collect();
            let signal = sig_fmts
                .iter()
                .map(|f| f.value(i).to_string())
                .collect::<Vec<_>>()
                .join("\u{1F}");
            out.push((key, signal));
        }
    }
    Ok(out)
}

/// Read every `(unique_keys..., comparison_signal)` tuple from the snapshot
/// target where `flux_is_current = true`. The signal is computed inside
/// Postgres as the same `\u{1F}`-joined `::text`-cast comparison columns
/// produced by [`stringify_rows`] for the staged side, so equality is a
/// straightforward `Vec<String>` HashMap lookup.
async fn read_current_signals_pg(
    resolved: &ResolvedSnapshotSink,
) -> Result<HashMap<Vec<String>, String>> {
    let (client, connection) =
        tokio_postgres::connect(&resolved.pg_config.connection_string, NoTls)
            .await
            .with_context(|| {
                format!(
                    "failed to connect to postgresql for sink `{}`",
                    resolved.node_id
                )
            })?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("postgresql connection error: {e}");
        }
    });

    let key_select = resolved
        .unique_keys
        .iter()
        .map(|k| format!("COALESCE({}::text, '{}')", quote_ident(k), NULL_SENTINEL))
        .collect::<Vec<_>>()
        .join(", ");
    let sig_select = resolved
        .comparison_columns
        .iter()
        .map(|c| format!("COALESCE({}::text, '{}')", quote_ident(c), NULL_SENTINEL))
        .collect::<Vec<_>>()
        .join(" || E'\\x1f' || ");

    let sql = format!(
        "SELECT {key_select}, {sig_select} AS __flux_signal FROM {table} WHERE {is_current}",
        table = quote_ident(&resolved.table),
        is_current = quote_ident(FLUX_IS_CURRENT),
    );

    let rows = client
        .query(&sql, &[])
        .await
        .with_context(|| format!("failed to read current snapshot from `{}`", resolved.table))?;

    let n_keys = resolved.unique_keys.len();
    let mut out: HashMap<Vec<String>, String> = HashMap::with_capacity(rows.len());
    for row in rows {
        let mut key: Vec<String> = Vec::with_capacity(n_keys);
        for i in 0..n_keys {
            key.push(row.get::<_, String>(i));
        }
        let signal: String = row.get(n_keys);
        out.insert(key, signal);
    }
    Ok(out)
}

#[derive(Debug, Default)]
struct DiffSummary {
    unchanged: u64,
    changed: u64,
    new_versions: u64,
    gone: u64,
    /// Sample of (classification, key) for human output.
    sample: Vec<(&'static str, Vec<String>)>,
}

const SAMPLE_LIMIT: usize = 25;

fn classify(
    staged: Vec<(Vec<String>, String)>,
    mut current: HashMap<Vec<String>, String>,
) -> DiffSummary {
    let mut s = DiffSummary::default();
    for (key, signal) in staged {
        match current.remove(&key) {
            None => {
                s.new_versions += 1;
                push_sample(&mut s, "new", key);
            }
            Some(target_signal) if target_signal == signal => {
                s.unchanged += 1;
            }
            Some(_) => {
                s.changed += 1;
                push_sample(&mut s, "changed", key);
            }
        }
    }
    for (key, _) in current {
        s.gone += 1;
        push_sample(&mut s, "gone", key);
    }
    s
}

fn push_sample(s: &mut DiffSummary, kind: &'static str, key: Vec<String>) {
    if s.sample.len() < SAMPLE_LIMIT {
        s.sample.push((kind, key));
    }
}

fn render_diff(
    resolved: &ResolvedSnapshotSink,
    s: &DiffSummary,
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Human => {
            println!(
                "Snapshot diff for sink `{}` (table `{}`):",
                resolved.node_id, resolved.table
            );
            println!(
                "  change_detection : {}",
                serde_json::to_value(resolved.snapshot.change_detection)
                    .ok()
                    .and_then(|v| v.as_str().map(String::from))
                    .unwrap_or_default()
            );
            println!("  unique_keys      : {}", resolved.unique_keys.join(", "));
            println!(
                "  comparison_cols  : {}",
                resolved.comparison_columns.join(", ")
            );
            println!();
            println!("  unchanged    : {}", s.unchanged);
            println!("  changed      : {}", s.changed);
            println!("  new          : {}", s.new_versions);
            println!("  gone         : {}", s.gone);
            if !s.sample.is_empty() {
                println!();
                println!("  sample (first {} non-unchanged keys):", SAMPLE_LIMIT);
                for (kind, key) in &s.sample {
                    println!("    [{kind:>7}] ({})", key.join(", "));
                }
                let remaining =
                    (s.changed + s.new_versions + s.gone).saturating_sub(s.sample.len() as u64);
                if remaining > 0 {
                    println!("    … {remaining} more");
                }
            }
            println!();
            println!("(no rows were written — this is a dry run)");
        }
        OutputFormat::Json => {
            let sample_json: Vec<serde_json::Value> = s
                .sample
                .iter()
                .map(|(k, key)| {
                    serde_json::json!({
                        "classification": k,
                        "unique_key": key,
                    })
                })
                .collect();
            let out = serde_json::json!({
                "node_id": resolved.node_id,
                "table": resolved.table,
                "unique_keys": resolved.unique_keys,
                "comparison_columns": resolved.comparison_columns,
                "stats": {
                    "unchanged": s.unchanged,
                    "changed": s.changed,
                    "new": s.new_versions,
                    "gone": s.gone,
                },
                "sample": sample_json,
                "dry_run": true,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `flux snapshot history`
// ---------------------------------------------------------------------------

fn history_cmd(
    pipeline_name: &str,
    node_id: &str,
    key_pairs: Vec<(String, String)>,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let stores = open_stores(metadata_url)?;
    let record = resolve_pipeline(&*stores.pipeline_store, pipeline_name)?;
    let environment = env
        .map(String::from)
        .unwrap_or_else(|| record.pipeline.default_environment.clone());
    let secret_resolver = build_secret_resolver();
    let resolved = resolve_snapshot_sink(
        &record,
        node_id,
        &environment,
        &HashMap::new(),
        secret_resolver.as_ref(),
    )?;

    // Validate the user supplied exactly one value per unique_key.
    let supplied: HashMap<String, String> = key_pairs.into_iter().collect();
    if supplied.len() != resolved.unique_keys.len()
        || !resolved
            .unique_keys
            .iter()
            .all(|k| supplied.contains_key(k))
    {
        let want = resolved.unique_keys.join(", ");
        anyhow::bail!(
            "`--key` must supply exactly one value per unique_key column ({want}); \
             got: {got}",
            got = supplied.keys().cloned().collect::<Vec<_>>().join(", ")
        );
    }
    let key_values: Vec<String> = resolved
        .unique_keys
        .iter()
        .map(|k| supplied.get(k).cloned().unwrap_or_default())
        .collect();

    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    let rows = rt.block_on(read_history_pg(&resolved, &key_values));
    rt.shutdown_background();
    let rows = rows?;

    render_history(&resolved, &key_values, &rows, format)
}

#[derive(Debug)]
struct HistoryRow {
    scd_id: String,
    valid_from: String,
    valid_to: Option<String>,
    is_current: bool,
    /// Comparison column values (the snapshot's tracked columns), as text.
    /// Keeps the output focused on what actually changed without dumping
    /// every business column.
    comparison_values: Vec<String>,
}

async fn read_history_pg(
    resolved: &ResolvedSnapshotSink,
    key_values: &[String],
) -> Result<Vec<HistoryRow>> {
    let (client, connection) =
        tokio_postgres::connect(&resolved.pg_config.connection_string, NoTls)
            .await
            .with_context(|| {
                format!(
                    "failed to connect to postgresql for sink `{}`",
                    resolved.node_id
                )
            })?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("postgresql connection error: {e}");
        }
    });

    // Build a parameterized WHERE that compares each unique key column to
    // a `$N::text` cast — same approach as `postgres_snapshot::build_hard_delete_sql`
    // so we don't care about the column's physical type.
    let where_clause = resolved
        .unique_keys
        .iter()
        .enumerate()
        .map(|(i, k)| format!("{}::text = ${}", quote_ident(k), i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    let comparison_select = if resolved.comparison_columns.is_empty() {
        "''".to_string()
    } else {
        resolved
            .comparison_columns
            .iter()
            .map(|c| format!("COALESCE({}::text, '{}')", quote_ident(c), NULL_SENTINEL))
            .collect::<Vec<_>>()
            .join(" || E'\\x1f' || ")
    };
    let sql = format!(
        "SELECT {scd_id}, \
                {valid_from}::text, \
                {valid_to}::text, \
                {is_current}, \
                {comparison_select} AS __flux_comparison \
         FROM {table} WHERE {where_clause} \
         ORDER BY {valid_from} DESC",
        scd_id = quote_ident(FLUX_SCD_ID),
        valid_from = quote_ident(FLUX_VALID_FROM),
        valid_to = quote_ident(FLUX_VALID_TO),
        is_current = quote_ident(FLUX_IS_CURRENT),
        table = quote_ident(&resolved.table),
    );

    // Bind each key value as &(dyn ToSql + Sync). The values themselves
    // are owned `String`s; tokio_postgres requires `&dyn ToSql` references.
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = key_values
        .iter()
        .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = client
        .query(&sql, &params)
        .await
        .with_context(|| format!("failed to read history from `{}`", resolved.table))?;

    let mut out: Vec<HistoryRow> = Vec::with_capacity(rows.len());
    for row in rows {
        let combined: String = row.get(4);
        let comparison_values: Vec<String> =
            combined.split('\u{1F}').map(|s| s.to_string()).collect();
        out.push(HistoryRow {
            scd_id: row.get(0),
            valid_from: row.get(1),
            valid_to: row.get::<_, Option<String>>(2),
            is_current: row.get(3),
            comparison_values,
        });
    }
    Ok(out)
}

fn render_history(
    resolved: &ResolvedSnapshotSink,
    key_values: &[String],
    rows: &[HistoryRow],
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Human => {
            let key_label = resolved
                .unique_keys
                .iter()
                .zip(key_values.iter())
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", ");
            println!(
                "Snapshot history for `{}` ({}) — {} version(s):",
                resolved.table,
                key_label,
                rows.len()
            );
            if rows.is_empty() {
                println!("(no rows match)");
                return Ok(());
            }
            println!();
            println!(
                "{:<18} {:<28} {:<28} {:<8} COMPARISON",
                "SCD_ID", "VALID_FROM", "VALID_TO", "CURRENT"
            );
            println!("{}", "-".repeat(120));
            for r in rows {
                println!(
                    "{:<18} {:<28} {:<28} {:<8} {}",
                    truncate(&r.scd_id, 18),
                    truncate(&r.valid_from, 28),
                    truncate(r.valid_to.as_deref().unwrap_or("(current)"), 28),
                    if r.is_current { "yes" } else { "no" },
                    r.comparison_values.join(" | "),
                );
            }
        }
        OutputFormat::Json => {
            let json_rows: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| {
                    let mut comparison_obj = serde_json::Map::new();
                    for (col, val) in resolved
                        .comparison_columns
                        .iter()
                        .zip(r.comparison_values.iter())
                    {
                        comparison_obj.insert(col.clone(), serde_json::Value::String(val.clone()));
                    }
                    serde_json::json!({
                        "flux_scd_id": r.scd_id,
                        "flux_valid_from": r.valid_from,
                        "flux_valid_to": r.valid_to,
                        "flux_is_current": r.is_current,
                        "comparison": comparison_obj,
                    })
                })
                .collect();
            let key_obj: serde_json::Map<String, serde_json::Value> = resolved
                .unique_keys
                .iter()
                .zip(key_values.iter())
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect();
            let out = serde_json::json!({
                "node_id": resolved.node_id,
                "table": resolved.table,
                "key": key_obj,
                "version_count": rows.len(),
                "versions": json_rows,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_key_pair_valid() {
        assert_eq!(
            parse_key_pair("customer_id=42").unwrap(),
            ("customer_id".into(), "42".into())
        );
    }

    #[test]
    fn parse_key_pair_with_equals_in_value() {
        assert_eq!(
            parse_key_pair("filter=a=b").unwrap(),
            ("filter".into(), "a=b".into())
        );
    }

    #[test]
    fn parse_key_pair_empty_column_rejected() {
        assert!(parse_key_pair("=42").is_err());
    }

    #[test]
    fn parse_key_pair_no_equals_rejected() {
        assert!(parse_key_pair("customer_id").is_err());
    }

    #[test]
    fn classify_covers_all_four_outcomes() {
        let staged = vec![
            (vec!["1".into()], "alice".into()), // unchanged
            (vec!["2".into()], "BOB".into()),   // changed (signal differs)
            (vec!["3".into()], "carol".into()), // new
        ];
        let mut current = HashMap::new();
        current.insert(vec!["1".to_string()], "alice".to_string());
        current.insert(vec!["2".to_string()], "bob".to_string());
        current.insert(vec!["4".to_string()], "dave".to_string()); // gone
        let s = classify(staged, current);
        assert_eq!(s.unchanged, 1);
        assert_eq!(s.changed, 1);
        assert_eq!(s.new_versions, 1);
        assert_eq!(s.gone, 1);
        let kinds: Vec<_> = s.sample.iter().map(|(k, _)| *k).collect();
        // unchanged is not sampled; the other three are.
        assert!(kinds.contains(&"changed"));
        assert!(kinds.contains(&"new"));
        assert!(kinds.contains(&"gone"));
        assert!(!kinds.contains(&"unchanged"));
    }
}
