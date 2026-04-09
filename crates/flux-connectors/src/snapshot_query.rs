// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Reusable read-only queries against SCD2 snapshot sink targets.
//!
//! Lives in `flux-connectors` rather than `flux-cli` so both the CLI
//! (`flux snapshot history`) and the HTTP server (snapshot history viewer
//! in the sink editor — planning doc 28) can call into the same code path
//! without flux-server having to depend on flux-cli (which would create a
//! cycle: flux-cli already depends on flux-server).
//!
//! v1 supports the **postgresql** sink only. Parquet and plugin (DuckDB)
//! support is tracked under "Deferred" in `planning/28-snapshots-scd2.md`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use flux_datafusion::SecretResolver;
use flux_engine::PipelineRecord;
use flux_engine::materialization::{ChangeDetection, SnapshotPolicy, WriteStrategy};
use flux_engine::node::{NodeKind, SinkConfig};
use flux_engine::snapshot::{FLUX_IS_CURRENT, FLUX_SCD_ID, FLUX_VALID_FROM, FLUX_VALID_TO};
use thiserror::Error;
use tokio_postgres::NoTls;

use crate::config::PostgreSqlConfig;

const NULL_SENTINEL: &str = "\0NULL";

#[derive(Debug, Error)]
pub enum SnapshotQueryError {
    #[error("node `{0}` not found in pipeline")]
    NodeNotFound(String),
    #[error("node `{0}` is not a sink")]
    NotASink(String),
    #[error("node `{0}` has no materialization block")]
    NoMaterialization(String),
    #[error("node `{node_id}` is not a snapshot sink (write_strategy = `{strategy}`)")]
    NotASnapshot { node_id: String, strategy: String },
    #[error("snapshot sink `{0}` is missing the `snapshot:` sub-block")]
    MissingSnapshotBlock(String),
    #[error("snapshot sink `{0}` is missing or has empty `unique_keys`")]
    MissingUniqueKeys(String),
    #[error(
        "`flux snapshot` v1 supports the `postgresql` sink only — node `{node_id}` uses connector \
         `{connector}`. Parquet and plugin (DuckDB) support is tracked in the deferred-tasks \
         section of `planning/28-snapshots-scd2.md`."
    )]
    UnsupportedConnector { node_id: String, connector: String },
    #[error("snapshot.check_columns is required for change_detection: check")]
    MissingCheckColumns,
    #[error("snapshot.updated_at_column is required for change_detection: timestamp")]
    MissingUpdatedAt,
    #[error("snapshot sink `{0}` is missing the `table` field")]
    MissingTable(String),
    #[error("invalid postgresql sink config on node `{node_id}`: {source}")]
    InvalidConfig {
        node_id: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to resolve secrets for sink `{node_id}`: {message}")]
    SecretResolution { node_id: String, message: String },
    #[error("`--key` must supply exactly one value per unique_key column ({want}); got: {got}")]
    KeyMismatch { want: String, got: String },
    #[error("postgresql connection failed for sink `{node_id}`: {source}")]
    Connect {
        node_id: String,
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("postgresql query against `{table}` failed: {source}")]
    Query {
        table: String,
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("staged batch missing column `{0}`")]
    StagedColumnMissing(String),
    #[error("cannot format staged column `{column}`: {message}")]
    StagedFormat { column: String, message: String },
}

/// Resolved snapshot sink ready for direct postgres queries.
#[derive(Debug)]
pub struct ResolvedSnapshotSink {
    pub node_id: String,
    pub unique_keys: Vec<String>,
    pub snapshot: SnapshotPolicy,
    /// Tracked columns used for change detection (`check_columns` for the
    /// `check` strategy, or the single `updated_at_column` for the
    /// `timestamp` strategy). The history viewer surfaces these per-version
    /// so users can see what actually changed without dumping every column.
    pub comparison_columns: Vec<String>,
    pub pg_config: PostgreSqlConfig,
    pub table: String,
}

/// Resolve a snapshot sink node from a pipeline record, applying the
/// environment override + variable interpolation + secret resolution chain
/// the executor would use, so the resulting connection string matches what
/// `flux run` would dial.
pub fn resolve_snapshot_sink(
    record: &PipelineRecord,
    node_id: &str,
    environment: &str,
    variable_overrides: &HashMap<String, serde_json::Value>,
    secret_resolver: Option<&Arc<dyn SecretResolver>>,
) -> Result<ResolvedSnapshotSink, SnapshotQueryError> {
    let node = record
        .pipeline
        .node(&flux_engine::NodeId::new(node_id))
        .ok_or_else(|| SnapshotQueryError::NodeNotFound(node_id.to_string()))?;

    let sink_cfg: &SinkConfig = match &node.kind {
        NodeKind::Sink(s) => s,
        _ => return Err(SnapshotQueryError::NotASink(node_id.to_string())),
    };

    let policy = sink_cfg
        .materialization
        .as_ref()
        .ok_or_else(|| SnapshotQueryError::NoMaterialization(node_id.to_string()))?;
    if !matches!(policy.write_strategy, WriteStrategy::Snapshot) {
        return Err(SnapshotQueryError::NotASnapshot {
            node_id: node_id.to_string(),
            strategy: format!("{:?}", policy.write_strategy),
        });
    }
    let snapshot = policy
        .snapshot
        .as_ref()
        .ok_or_else(|| SnapshotQueryError::MissingSnapshotBlock(node_id.to_string()))?
        .clone();
    let unique_keys = policy
        .unique_keys
        .clone()
        .ok_or_else(|| SnapshotQueryError::MissingUniqueKeys(node_id.to_string()))?;
    if unique_keys.is_empty() {
        return Err(SnapshotQueryError::MissingUniqueKeys(node_id.to_string()));
    }

    if sink_cfg.connector != "postgresql" {
        return Err(SnapshotQueryError::UnsupportedConnector {
            node_id: node_id.to_string(),
            connector: sink_cfg.connector.clone(),
        });
    }

    let comparison_columns = match snapshot.change_detection {
        ChangeDetection::Check => snapshot
            .check_columns
            .clone()
            .ok_or(SnapshotQueryError::MissingCheckColumns)?,
        ChangeDetection::Timestamp => vec![
            snapshot
                .updated_at_column
                .clone()
                .ok_or(SnapshotQueryError::MissingUpdatedAt)?,
        ],
    };

    // Mirror flux-cli/src/snapshot.rs: env override → var interpolation →
    // secret resolution. Same chain the executor uses.
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
            run_id: format!("snapshot-query-{}", chrono::Utc::now().timestamp_micros()),
            pipeline_name: record.pipeline.name.clone(),
            environment: environment.to_string(),
        },
    );
    config_json = resolved_vars.interpolate_json(&config_json);
    if let Some(resolver) = secret_resolver {
        config_json = resolver
            .resolve_json(&config_json, Some(environment))
            .map_err(|e| SnapshotQueryError::SecretResolution {
                node_id: node_id.to_string(),
                message: e.to_string(),
            })?;
    }
    let pg_config: PostgreSqlConfig =
        serde_json::from_value(config_json).map_err(|e| SnapshotQueryError::InvalidConfig {
            node_id: node_id.to_string(),
            source: e,
        })?;
    let table = pg_config
        .table
        .clone()
        .ok_or_else(|| SnapshotQueryError::MissingTable(node_id.to_string()))?;

    Ok(ResolvedSnapshotSink {
        node_id: node_id.to_string(),
        unique_keys,
        snapshot,
        comparison_columns,
        pg_config,
        table,
    })
}

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

/// One historical version of a single business key in an SCD2 target.
#[derive(Debug, Clone)]
pub struct HistoryRow {
    pub scd_id: String,
    pub valid_from: String,
    pub valid_to: Option<String>,
    pub is_current: bool,
    /// Tracked comparison column values, in the same order as
    /// [`ResolvedSnapshotSink::comparison_columns`].
    pub comparison_values: Vec<String>,
}

/// Validate the user-supplied `column → value` map covers exactly the
/// resolved sink's `unique_keys` and return the values in the canonical
/// order. Used by both the CLI and the server endpoint.
pub fn align_key_values(
    resolved: &ResolvedSnapshotSink,
    supplied: &HashMap<String, String>,
) -> Result<Vec<String>, SnapshotQueryError> {
    if supplied.len() != resolved.unique_keys.len()
        || !resolved
            .unique_keys
            .iter()
            .all(|k| supplied.contains_key(k))
    {
        return Err(SnapshotQueryError::KeyMismatch {
            want: resolved.unique_keys.join(", "),
            got: supplied.keys().cloned().collect::<Vec<_>>().join(", "),
        });
    }
    Ok(resolved
        .unique_keys
        .iter()
        .map(|k| supplied.get(k).cloned().unwrap_or_default())
        .collect())
}

/// Read every historical version of a single business key, ordered most
/// recent first. v1 is postgresql-only.
pub async fn read_history_pg(
    resolved: &ResolvedSnapshotSink,
    key_values: &[String],
) -> Result<Vec<HistoryRow>, SnapshotQueryError> {
    let (client, connection) =
        tokio_postgres::connect(&resolved.pg_config.connection_string, NoTls)
            .await
            .map_err(|e| SnapshotQueryError::Connect {
                node_id: resolved.node_id.clone(),
                source: e,
            })?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("postgresql connection error: {e}");
        }
    });

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

    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = key_values
        .iter()
        .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = client
        .query(&sql, &params)
        .await
        .map_err(|e| SnapshotQueryError::Query {
            table: resolved.table.clone(),
            source: e,
        })?;

    let mut out: Vec<HistoryRow> = Vec::with_capacity(rows.len());
    for row in rows {
        let combined: String = row.get(4);
        let comparison_values: Vec<String> = if resolved.comparison_columns.is_empty() {
            Vec::new()
        } else {
            combined.split('\u{1F}').map(|s| s.to_string()).collect()
        };
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

// ---------------------------------------------------------------------------
// Snapshot diff (planning doc 28 — diff preview)
//
// Shared between `flux snapshot diff` (CLI) and the `snapshot/diff` HTTP
// endpoint backing the in-canvas Diff Preview panel. The CLI currently
// keeps its own copy of `stringify_rows` / `classify` for historical
// reasons; the server uses the functions below directly.
// ---------------------------------------------------------------------------

/// Classification of a single business key when staged rows are diffed
/// against a snapshot target's `flux_is_current = true` slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffClassification {
    /// Key exists in both staged and target with the same comparison signal.
    Unchanged,
    /// Key exists in both, but the comparison signal differs — would close
    /// the current target version and open a new one.
    Changed,
    /// Key only in staged input — would insert as a brand-new version.
    New,
    /// Key only in target — staged input has dropped it. Treatment at write
    /// time depends on `hard_deletes` (ignore / invalidate / delete).
    Gone,
}

/// Aggregate counts plus a bounded sample of non-`Unchanged` keys.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct DiffSummary {
    pub unchanged: u64,
    pub changed: u64,
    pub new_versions: u64,
    pub gone: u64,
    /// Up to `sample_limit` (classification, unique_key_values) tuples for
    /// the UI to render. `Unchanged` keys are intentionally excluded — the
    /// sample is meant to surface what would actually move.
    pub sample: Vec<DiffSample>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DiffSample {
    pub classification: DiffClassification,
    pub unique_key: Vec<String>,
}

/// Hard cap on rows materialized from the staged side before classification.
/// A 100M-row source must not be allowed to OOM the server. The CLI does
/// not enforce this cap (it streams to stdout from a local terminal); the
/// server endpoint does.
pub const DEFAULT_DIFF_STAGED_ROW_CAP: usize = 10_000;

/// Default number of non-unchanged keys retained in [`DiffSummary::sample`].
pub const DEFAULT_DIFF_SAMPLE_LIMIT: usize = 50;

/// Sentinel substituted for SQL `NULL` so the staged-side and target-side
/// comparison signals collide on a real `=` rather than skipping NULLs.
const DIFF_NULL_SENTINEL: &str = "\0NULL";

/// Stringify the unique-key columns and the comparison columns of every
/// row in `batches` into `(key, signal)` tuples. The signal is the
/// `\u{1F}`-joined display form of each comparison column — chosen to
/// match what `read_current_signals_pg` produces from the target side
/// (postgres `::text` cast joined with the same separator), so equality
/// is a direct `String` comparison.
///
/// Caps the result at `row_cap` rows; the second tuple element is `true`
/// if the cap was hit and more staged rows exist than were materialized.
/// `(unique_key_values, joined_comparison_signal)` tuple — one per staged
/// row. Kept as a type alias to keep the [`stringify_rows`] return type
/// readable (and clippy quiet).
pub type StagedRow = (Vec<String>, String);

#[allow(clippy::type_complexity)]
pub fn stringify_rows(
    batches: &[RecordBatch],
    unique_keys: &[String],
    comparison_columns: &[String],
    row_cap: usize,
) -> Result<(Vec<StagedRow>, bool), SnapshotQueryError> {
    let mut out: Vec<(Vec<String>, String)> = Vec::new();
    let mut truncated = false;
    let opts = FormatOptions::default().with_null(DIFF_NULL_SENTINEL);
    'outer: for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let key_fmts: Vec<ArrayFormatter> = unique_keys
            .iter()
            .map(|k| {
                let idx = batch
                    .schema()
                    .index_of(k)
                    .map_err(|_| SnapshotQueryError::StagedColumnMissing(k.clone()))?;
                ArrayFormatter::try_new(batch.column(idx).as_ref(), &opts).map_err(|e| {
                    SnapshotQueryError::StagedFormat {
                        column: k.clone(),
                        message: e.to_string(),
                    }
                })
            })
            .collect::<Result<_, _>>()?;
        let sig_fmts: Vec<ArrayFormatter> = comparison_columns
            .iter()
            .map(|c| {
                let idx = batch
                    .schema()
                    .index_of(c)
                    .map_err(|_| SnapshotQueryError::StagedColumnMissing(c.clone()))?;
                ArrayFormatter::try_new(batch.column(idx).as_ref(), &opts).map_err(|e| {
                    SnapshotQueryError::StagedFormat {
                        column: c.clone(),
                        message: e.to_string(),
                    }
                })
            })
            .collect::<Result<_, _>>()?;
        for i in 0..batch.num_rows() {
            if out.len() >= row_cap {
                truncated = true;
                break 'outer;
            }
            let key: Vec<String> = key_fmts.iter().map(|f| f.value(i).to_string()).collect();
            let signal = sig_fmts
                .iter()
                .map(|f| f.value(i).to_string())
                .collect::<Vec<_>>()
                .join("\u{1F}");
            out.push((key, signal));
        }
    }
    Ok((out, truncated))
}

/// Read every `(unique_keys..., signal)` tuple from the snapshot target
/// where `flux_is_current = true`. The signal is computed inside Postgres
/// as the same `\u{1F}`-joined `::text`-cast comparison columns produced
/// by [`stringify_rows`] for the staged side.
pub async fn read_current_signals_pg(
    resolved: &ResolvedSnapshotSink,
) -> Result<HashMap<Vec<String>, String>, SnapshotQueryError> {
    let (client, connection) =
        tokio_postgres::connect(&resolved.pg_config.connection_string, NoTls)
            .await
            .map_err(|e| SnapshotQueryError::Connect {
                node_id: resolved.node_id.clone(),
                source: e,
            })?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("postgresql connection error: {e}");
        }
    });

    let key_select = resolved
        .unique_keys
        .iter()
        .map(|k| {
            format!(
                "COALESCE({}::text, '{}')",
                quote_ident(k),
                DIFF_NULL_SENTINEL
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let sig_select = resolved
        .comparison_columns
        .iter()
        .map(|c| {
            format!(
                "COALESCE({}::text, '{}')",
                quote_ident(c),
                DIFF_NULL_SENTINEL
            )
        })
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
        .map_err(|e| SnapshotQueryError::Query {
            table: resolved.table.clone(),
            source: e,
        })?;

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

/// Classify staged rows against a target's current-version map.
///
/// `current` is consumed: keys that match a staged row are removed as we
/// go, so whatever remains at the end is the `Gone` set. This avoids a
/// second hash-table pass.
pub fn classify_diff(
    staged: Vec<StagedRow>,
    mut current: HashMap<Vec<String>, String>,
    sample_limit: usize,
) -> DiffSummary {
    let mut s = DiffSummary::default();
    for (key, signal) in staged {
        match current.remove(&key) {
            None => {
                s.new_versions += 1;
                push_diff_sample(&mut s, DiffClassification::New, key, sample_limit);
            }
            Some(target_signal) if target_signal == signal => {
                s.unchanged += 1;
            }
            Some(_) => {
                s.changed += 1;
                push_diff_sample(&mut s, DiffClassification::Changed, key, sample_limit);
            }
        }
    }
    for (key, _) in current {
        s.gone += 1;
        push_diff_sample(&mut s, DiffClassification::Gone, key, sample_limit);
    }
    s
}

fn push_diff_sample(
    s: &mut DiffSummary,
    classification: DiffClassification,
    unique_key: Vec<String>,
    sample_limit: usize,
) {
    if s.sample.len() < sample_limit {
        s.sample.push(DiffSample {
            classification,
            unique_key,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_resolved(unique_keys: Vec<&str>) -> ResolvedSnapshotSink {
        ResolvedSnapshotSink {
            node_id: "n".into(),
            unique_keys: unique_keys.into_iter().map(String::from).collect(),
            snapshot: SnapshotPolicy {
                change_detection: ChangeDetection::Check,
                check_columns: Some(vec!["email".into()]),
                updated_at_column: None,
                hard_deletes: Default::default(),
            },
            comparison_columns: vec!["email".into()],
            pg_config: PostgreSqlConfig {
                connection_string: "postgres://x".into(),
                table: Some("t".into()),
                query: None,
                batch_size: None,
                indexes: vec![],
            },
            table: "t".into(),
        }
    }

    #[test]
    fn align_key_values_orders_by_unique_keys() {
        let r = dummy_resolved(vec!["tenant_id", "customer_id"]);
        let mut supplied = HashMap::new();
        supplied.insert("customer_id".to_string(), "7".to_string());
        supplied.insert("tenant_id".to_string(), "42".to_string());
        let aligned = align_key_values(&r, &supplied).unwrap();
        assert_eq!(aligned, vec!["42".to_string(), "7".to_string()]);
    }

    #[test]
    fn align_key_values_rejects_missing_key() {
        let r = dummy_resolved(vec!["tenant_id", "customer_id"]);
        let mut supplied = HashMap::new();
        supplied.insert("tenant_id".to_string(), "42".to_string());
        let err = align_key_values(&r, &supplied).unwrap_err();
        assert!(matches!(err, SnapshotQueryError::KeyMismatch { .. }));
    }

    #[test]
    fn align_key_values_rejects_extra_key() {
        let r = dummy_resolved(vec!["tenant_id"]);
        let mut supplied = HashMap::new();
        supplied.insert("tenant_id".to_string(), "42".to_string());
        supplied.insert("oops".to_string(), "x".to_string());
        assert!(align_key_values(&r, &supplied).is_err());
    }

    #[test]
    fn classify_diff_covers_all_four_outcomes() {
        let staged = vec![
            (vec!["1".into()], "alice".into()),
            (vec!["2".into()], "BOB".into()),
            (vec!["3".into()], "carol".into()),
        ];
        let mut current = HashMap::new();
        current.insert(vec!["1".to_string()], "alice".to_string());
        current.insert(vec!["2".to_string()], "bob".to_string());
        current.insert(vec!["4".to_string()], "dave".to_string());
        let s = classify_diff(staged, current, 10);
        assert_eq!(s.unchanged, 1);
        assert_eq!(s.changed, 1);
        assert_eq!(s.new_versions, 1);
        assert_eq!(s.gone, 1);
        let kinds: Vec<_> = s.sample.iter().map(|r| r.classification).collect();
        assert!(kinds.contains(&DiffClassification::Changed));
        assert!(kinds.contains(&DiffClassification::New));
        assert!(kinds.contains(&DiffClassification::Gone));
        assert!(!kinds.contains(&DiffClassification::Unchanged));
    }

    #[test]
    fn classify_diff_respects_sample_limit() {
        let staged: Vec<_> = (0..50)
            .map(|i| (vec![i.to_string()], format!("v{i}")))
            .collect();
        let s = classify_diff(staged, HashMap::new(), 5);
        assert_eq!(s.new_versions, 50);
        assert_eq!(s.sample.len(), 5);
    }

    #[test]
    fn stringify_rows_caps_at_row_cap_and_flags_truncated() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
                StdArc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .unwrap();
        let (rows, truncated) =
            stringify_rows(&[batch], &["id".into()], &["email".into()], 3).unwrap();
        assert_eq!(rows.len(), 3);
        assert!(truncated);
        assert_eq!(rows[0].0, vec!["1".to_string()]);
        assert_eq!(rows[0].1, "a");
    }

    #[test]
    fn stringify_rows_rejects_missing_unique_key_column() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;
        let schema = StdArc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![StdArc::new(Int64Array::from(vec![1i64]))]).unwrap();
        let err = stringify_rows(&[batch], &["id".into()], &[], 100).unwrap_err();
        assert!(matches!(err, SnapshotQueryError::StagedColumnMissing(c) if c == "id"));
    }
}
