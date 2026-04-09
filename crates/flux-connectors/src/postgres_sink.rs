// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PostgreSQL sink connector.
//!
//! Writes Arrow record batches to a PostgreSQL table with support for
//! append, merge (ON CONFLICT DO UPDATE), and truncate-then-insert write
//! strategies, driven by the sink's `materialization` block (doc 27).
//! Writes are wrapped in a transaction for all-or-nothing semantics.
//!
//! The pre-doc-27 `PostgresWriteMode` enum has been removed; pipelines that
//! still carry the legacy shape are auto-migrated on import by
//! `flux_engine::validate::migrate_legacy_sinks`.

use std::time::Instant;

use arrow::array::{
    Array, AsArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use flux_datafusion::provider::{
    MaterializationContext, MaterializationReceipt, PipelineSink, ProviderError, WriteOptions,
    WriteStats,
};
use flux_engine::materialization::{
    HardDeletes, MaterializationPolicy, SnapshotPolicy, WriteStrategy,
};
use flux_engine::node::SinkConfig;
use tokio_postgres::NoTls;
use tokio_postgres::types::ToSql;
use tracing::debug;

use crate::config::PostgreSqlConfig;
use crate::postgres_snapshot::{
    build_close_versions_sql, build_create_snapshot_table_sql, build_create_stage_table_sql,
    build_diff_query, build_hard_delete_sql, build_insert_new_versions_sql,
    build_snapshot_index_sql, comparison_columns, stage_table_name, stats_from_diff,
    validate_snapshot_runtime,
};

const DEFAULT_BATCH_SIZE: usize = 1000;

/// Internal representation of the (small) subset of write strategies the
/// Postgres sink supports today. Derived from the sink's
/// [`MaterializationPolicy`] (or defaulted to `Append` when absent).
#[derive(Debug, Clone)]
enum PgWrite {
    /// Plain `INSERT`. Used for `WriteStrategy::Append` (and as the default
    /// when no materialization block is configured).
    Append,
    /// `INSERT ... ON CONFLICT (keys) DO UPDATE`. Used for
    /// `WriteStrategy::Merge`. Vec is the unique key columns.
    Upsert(Vec<String>),
    /// `TRUNCATE` then `INSERT`. Used for `WriteStrategy::TruncateInsert`.
    TruncateInsert,
    /// Per-row `DELETE` on the unique key columns followed by plain `INSERT`,
    /// all inside a single transaction. Used for `WriteStrategy::DeleteInsert`.
    /// Vec is the unique key columns.
    DeleteInsert(Vec<String>),
    /// SCD2 stage-diff-merge against a snapshot target. See doc 28 and
    /// `postgres_snapshot.rs` for the SQL builders.
    Snapshot {
        unique_keys: Vec<String>,
        policy: SnapshotPolicy,
    },
}

/// Translate a `MaterializationPolicy` (if any) into the internal Postgres
/// write mode. Returns a clear error for strategies the Postgres sink cannot
/// implement yet (`DeleteInsert`, `InsertOverwrite`).
fn pg_write_from_policy(policy: Option<&MaterializationPolicy>) -> Result<PgWrite, ProviderError> {
    let Some(p) = policy else {
        return Ok(PgWrite::Append);
    };
    match p.write_strategy {
        WriteStrategy::Append => Ok(PgWrite::Append),
        WriteStrategy::Merge => {
            let keys = p.unique_keys.clone().unwrap_or_default();
            if keys.is_empty() {
                // validate_policy in flux-engine should already reject this,
                // but we double-check at runtime to fail loud rather than
                // silently degrade to plain insert.
                return Err("postgresql merge strategy requires non-empty `unique_keys`".into());
            }
            Ok(PgWrite::Upsert(keys))
        }
        WriteStrategy::TruncateInsert => Ok(PgWrite::TruncateInsert),
        WriteStrategy::DeleteInsert => {
            let keys = p.unique_keys.clone().unwrap_or_default();
            if keys.is_empty() {
                return Err(
                    "postgresql delete_insert strategy requires non-empty `unique_keys`".into(),
                );
            }
            Ok(PgWrite::DeleteInsert(keys))
        }
        WriteStrategy::InsertOverwrite => Err(
            "postgresql sink does not support `insert_overwrite`; declarative partitioning is required and not yet wired up"
                .into(),
        ),
        WriteStrategy::Snapshot => {
            let keys = p.unique_keys.clone().unwrap_or_default();
            if keys.is_empty() {
                return Err(
                    "postgresql snapshot strategy requires non-empty `unique_keys`".into(),
                );
            }
            let policy = p
                .snapshot
                .clone()
                .ok_or("postgresql snapshot strategy requires a `snapshot:` block")?;
            Ok(PgWrite::Snapshot {
                unique_keys: keys,
                policy,
            })
        }
    }
}

/// Sink connector for PostgreSQL databases.
///
/// Supports:
/// - Write modes: insert, upsert (ON CONFLICT DO UPDATE), truncate-and-insert, append
/// - Batch inserts for performance (configurable batch size)
/// - Auto-create table from Arrow schema if it doesn't exist
/// - Transaction wrapping for all-or-nothing writes
pub struct PostgresSink;

impl PostgresSink {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PostgresSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PipelineSink for PostgresSink {
    async fn write(
        &self,
        config: &SinkConfig,
        data: Vec<RecordBatch>,
        _options: &WriteOptions,
        ctx: &MaterializationContext,
    ) -> Result<MaterializationReceipt, ProviderError> {
        let start = Instant::now();

        let pg_config: PostgreSqlConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid postgresql sink config: {e}"))?;

        if data.is_empty() {
            let stats = WriteStats {
                rows_written: 0,
                bytes_written: 0,
                duration: start.elapsed(),
            };
            return Ok(MaterializationReceipt::from_write_stats(&stats, ctx));
        }

        let table = pg_config
            .table
            .as_deref()
            .ok_or("postgresql sink requires a 'table' name")?;

        let write_mode = pg_write_from_policy(config.materialization.as_ref())?;
        let batch_size = pg_config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let schema = data[0].schema();

        // Connect.
        let (mut client, connection) = tokio_postgres::connect(&pg_config.connection_string, NoTls)
            .await
            .map_err(|e| format!("failed to connect to postgresql: {e}"))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("postgresql connection error: {e}");
            }
        });

        // Validate schema compatibility (does the row-binding loop know
        // how to handle every Arrow type in the incoming schema?).
        validate_schema_compatibility(&schema)?;

        // Auto-create the target table if it doesn't exist. Snapshot
        // targets need the four SCD2 metadata columns appended; other
        // strategies use the plain business-column-only DDL.
        let business_col_ddls = build_business_column_ddls(&schema)?;
        let create_sql = match &write_mode {
            PgWrite::Snapshot { unique_keys, .. } => {
                build_create_snapshot_table_sql(table, &business_col_ddls, unique_keys)
            }
            _ => format!(
                "CREATE TABLE IF NOT EXISTS {} ({})",
                quote_ident(table),
                business_col_ddls.join(", ")
            ),
        };
        client
            .execute(&create_sql, &[])
            .await
            .map_err(|e| format!("failed to auto-create table '{table}': {e}"))?;

        // Snapshot path also creates a helper index on (unique_keys, flux_is_current)
        // outside the transaction so the diff query is fast on subsequent runs.
        if let PgWrite::Snapshot { unique_keys, .. } = &write_mode {
            let idx_sql = build_snapshot_index_sql(table, unique_keys);
            client
                .execute(&idx_sql, &[])
                .await
                .map_err(|e| format!("failed to create snapshot helper index: {e}"))?;
        }

        // Begin transaction.
        let transaction = client
            .transaction()
            .await
            .map_err(|e| format!("failed to begin transaction: {e}"))?;

        // Apply target-side schema evolution if the executor signaled it
        // (`on_schema_change: append_new_columns` or `sync_all_columns`
        // produced a `SchemaAction::ProceedWithAlter` for this run). Done
        // inside the same transaction as the data write so a failed ALTER
        // rolls everything back. We introspect the *target* (rather than
        // trusting the executor's previous-run schema diff) so manual
        // out-of-band table edits can't desync us — the source of truth is
        // information_schema. Implements doc 27's Postgres
        // `append_new_columns` follow-up: missing columns are added with
        // their Arrow-mapped pg type, nullable. Removed columns and type
        // changes are intentionally not applied in v1 (they require
        // destructive DDL and a per-strategy migration plan we have not
        // designed yet).
        if ctx.apply_schema_changes {
            let existing = fetch_existing_columns(&transaction, table)
                .await
                .map_err(|e| format!("failed to introspect '{table}' columns: {e}"))?;
            let alter_stmts = build_add_column_stmts(table, &schema, &existing)?;
            for sql in &alter_stmts {
                debug!(sql = %sql, "altering table to add column");
                transaction
                    .execute(sql, &[])
                    .await
                    .map_err(|e| format!("failed to ALTER TABLE '{table}': {e}"))?;
            }
        }

        // Snapshot path: stage→diff→merge inside the same transaction.
        // Returns early — none of the other write-strategy branches apply.
        if let PgWrite::Snapshot {
            unique_keys,
            policy,
        } = &write_mode
        {
            validate_snapshot_runtime(&schema, unique_keys, policy)?;
            let comparison = comparison_columns(&schema, unique_keys, policy);
            let business_cols: Vec<String> =
                schema.fields().iter().map(|f| f.name().clone()).collect();

            // 1. Stage. Create the temp table mirroring the incoming
            //    business schema and populate it via the existing
            //    row-binding INSERT loop.
            let stage_ddl = build_create_stage_table_sql(&business_col_ddls);
            transaction
                .execute(&stage_ddl, &[])
                .await
                .map_err(|e| format!("failed to create snapshot stage table: {e}"))?;

            let stage_insert_sql = build_insert_sql(stage_table_name(), &schema, &PgWrite::Append)?;
            let mut total_stage_rows: u64 = 0;
            let mut total_bytes: u64 = 0;
            for batch in &data {
                let n = batch.num_rows();
                for chunk_start in (0..n).step_by(batch_size) {
                    let chunk_end = (chunk_start + batch_size).min(n);
                    for row_idx in chunk_start..chunk_end {
                        let params = extract_row_params(batch, row_idx)?;
                        let param_refs: Vec<&(dyn ToSql + Sync)> =
                            params.iter().map(|p| p.as_ref()).collect();
                        transaction
                            .execute(&stage_insert_sql, &param_refs)
                            .await
                            .map_err(|e| format!("failed to stage snapshot row {row_idx}: {e}"))?;
                        total_stage_rows += 1;
                        total_bytes += params.iter().map(|p| p.byte_size() as u64).sum::<u64>();
                    }
                }
            }

            // 2. Diff. One round-trip captures (changed_ids, gone_ids, new_count).
            let diff_sql = build_diff_query(table, unique_keys, &comparison);
            debug!(sql = %diff_sql, "snapshot diff query");
            let row = transaction
                .query_one(&diff_sql, &[])
                .await
                .map_err(|e| format!("snapshot diff query failed: {e}"))?;
            let changed_ids: Vec<String> = row.get("changed_ids");
            let gone_ids: Vec<String> = row.get("gone_ids");
            let new_count: i64 = row.get("new_count");
            let new_count = new_count.max(0) as u64;
            let changed_count = changed_ids.len() as u64;
            let gone_count = gone_ids.len() as u64;

            // 3. Merge. Close changed (and gone-when-invalidate) versions, then
            //    insert new versions for changed ∪ new rows. Both run inside
            //    the same transaction the staging insert used, so any failure
            //    rolls everything back.
            let now = chrono::Utc::now();

            let mut to_close: Vec<String> = changed_ids.clone();
            if matches!(policy.hard_deletes, HardDeletes::Invalidate) {
                to_close.extend(gone_ids.iter().cloned());
            }
            if !to_close.is_empty() {
                let close_sql = build_close_versions_sql(table);
                debug!(rows = to_close.len(), "closing snapshot versions");
                transaction
                    .execute(
                        &close_sql,
                        &[
                            &now as &(dyn ToSql + Sync),
                            &to_close as &(dyn ToSql + Sync),
                        ],
                    )
                    .await
                    .map_err(|e| format!("failed to close snapshot versions: {e}"))?;
            }

            if changed_count + new_count > 0 {
                let insert_sql =
                    build_insert_new_versions_sql(table, &business_cols, unique_keys, &comparison);
                debug!(sql = %insert_sql, "inserting new snapshot versions");
                transaction
                    .execute(&insert_sql, &[&now as &(dyn ToSql + Sync)])
                    .await
                    .map_err(|e| {
                        let detail = e
                            .as_db_error()
                            .map(|d| format!(": {}", d.message()))
                            .unwrap_or_default();
                        format!("failed to insert snapshot new versions: {e}{detail}")
                    })?;
            }

            // Hard-delete: remove every historical version of the gone keys.
            // Done last so the close + insert path above has already observed
            // any current versions of those keys.
            if matches!(policy.hard_deletes, HardDeletes::Delete) && !gone_ids.is_empty() {
                // Re-read the gone keys' unique-key tuples (we only captured
                // their flux_scd_id above; for DELETE we need the business
                // keys to remove all historical versions).
                let key_cols_select = unique_keys
                    .iter()
                    .map(|k| quote_ident(k))
                    .collect::<Vec<_>>()
                    .join(", ");
                let select_sql = format!(
                    "SELECT DISTINCT {} FROM {} WHERE \"flux_scd_id\" = ANY($1)",
                    key_cols_select,
                    quote_ident(table)
                );
                let key_rows = transaction
                    .query(&select_sql, &[&gone_ids as &(dyn ToSql + Sync)])
                    .await
                    .map_err(|e| format!("failed to load gone keys for hard delete: {e}"))?;
                if !key_rows.is_empty() {
                    let delete_sql = build_hard_delete_sql(table, unique_keys, key_rows.len());
                    // Bind the unique-key tuple values from each row,
                    // typed as TEXT — Postgres coerces to the column type
                    // via the IN comparison.
                    let mut param_strings: Vec<String> = Vec::new();
                    for kr in &key_rows {
                        for i in 0..unique_keys.len() {
                            let v: String = kr
                                .try_get::<_, String>(i)
                                .or_else(|_| kr.try_get::<_, i64>(i).map(|n| n.to_string()))
                                .or_else(|_| kr.try_get::<_, i32>(i).map(|n| n.to_string()))
                                .map_err(|e| {
                                    format!(
                                        "unsupported snapshot unique-key type for hard delete: {e}"
                                    )
                                })?;
                            param_strings.push(v);
                        }
                    }
                    let param_refs: Vec<&(dyn ToSql + Sync)> = param_strings
                        .iter()
                        .map(|s| s as &(dyn ToSql + Sync))
                        .collect();
                    transaction
                        .execute(&delete_sql, &param_refs)
                        .await
                        .map_err(|e| format!("failed to hard-delete snapshot rows: {e}"))?;
                }
            }

            transaction
                .commit()
                .await
                .map_err(|e| format!("failed to commit snapshot transaction: {e}"))?;

            let stats = stats_from_diff(
                total_stage_rows,
                changed_count,
                new_count,
                gone_count,
                policy.hard_deletes,
            );
            let write_stats = WriteStats {
                rows_written: total_stage_rows,
                bytes_written: total_bytes,
                duration: start.elapsed(),
            };
            let mut receipt = MaterializationReceipt::from_write_stats(&write_stats, ctx);
            receipt.rows_inserted = stats.receipt_rows_inserted();
            receipt.rows_updated = stats.receipt_rows_updated();
            receipt.rows_deleted = stats.receipt_rows_deleted();
            return Ok(receipt);
        }

        // Handle truncate mode. Capture row count *before* truncating so the
        // receipt can report `rows_deleted` accurately.
        let mut total_truncated: u64 = 0;
        if matches!(write_mode, PgWrite::TruncateInsert) {
            let count_sql = format!("SELECT COUNT(*) FROM {}", quote_ident(table));
            let row = transaction
                .query_one(&count_sql, &[])
                .await
                .map_err(|e| format!("failed to count rows in '{table}' before truncate: {e}"))?;
            let count: i64 = row.get(0);
            total_truncated = count.max(0) as u64;

            let truncate_sql = format!("TRUNCATE {}", quote_ident(table));
            debug!(sql = %truncate_sql, "truncating table");
            transaction
                .execute(&truncate_sql, &[])
                .await
                .map_err(|e| format!("failed to truncate table '{table}': {e}"))?;
        }

        // For delete_insert: pre-pass to delete existing rows matching each
        // incoming row's unique-key tuple. Done inside the same transaction
        // so a failure rolls everything back.
        let mut total_deleted: u64 = 0;
        if let PgWrite::DeleteInsert(keys) = &write_mode {
            let key_indices = resolve_key_indices(&schema, keys)?;
            let delete_sql = build_delete_sql(table, keys);
            debug!(sql = %delete_sql, "deleting matching rows");
            for batch in &data {
                for row_idx in 0..batch.num_rows() {
                    let row_params = extract_row_params(batch, row_idx)?;
                    let key_params: Vec<&(dyn ToSql + Sync)> = key_indices
                        .iter()
                        .map(|i| row_params[*i].as_ref())
                        .collect();
                    let n = transaction
                        .execute(&delete_sql, &key_params)
                        .await
                        .map_err(|e| format!("failed to delete row {row_idx}: {e}"))?;
                    total_deleted += n;
                }
            }
        }

        // Build the INSERT statement.
        let insert_sql = build_insert_sql(table, &schema, &write_mode)?;
        debug!(sql = %insert_sql, batch_size = batch_size, "inserting rows");

        let mut total_rows: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut upsert_inserted: u64 = 0;
        let mut upsert_updated: u64 = 0;
        let is_upsert = matches!(write_mode, PgWrite::Upsert(_));

        for batch in &data {
            let num_rows = batch.num_rows();
            for chunk_start in (0..num_rows).step_by(batch_size) {
                let chunk_end = (chunk_start + batch_size).min(num_rows);

                for row_idx in chunk_start..chunk_end {
                    let params = extract_row_params(batch, row_idx)?;
                    let param_refs: Vec<&(dyn ToSql + Sync)> =
                        params.iter().map(|p| p.as_ref()).collect();

                    let exec_result: Result<(), tokio_postgres::Error> = if is_upsert {
                        match transaction.query(&insert_sql, &param_refs).await {
                            Ok(rows) => {
                                if let Some(row) = rows.first() {
                                    let inserted: bool = row.get("inserted");
                                    if inserted {
                                        upsert_inserted += 1;
                                    } else {
                                        upsert_updated += 1;
                                    }
                                }
                                // No row → ON CONFLICT DO NOTHING skipped it; leave both counters.
                                Ok(())
                            }
                            Err(e) => Err(e),
                        }
                    } else {
                        transaction
                            .execute(&insert_sql, &param_refs)
                            .await
                            .map(|_| ())
                    };

                    if let Err(e) = exec_result {
                        let schema = batch.schema();
                        let col_info: Vec<String> = schema
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(i, f)| {
                                let is_null = batch.column(i).is_null(row_idx);
                                format!(
                                    "  ${}: {} ({}) null={}",
                                    i + 1,
                                    f.name(),
                                    f.data_type(),
                                    is_null
                                )
                            })
                            .collect();
                        return Err(format!(
                            "failed to insert row {row_idx}: {e}\n{}",
                            col_info.join("\n")
                        )
                        .into());
                    }

                    total_rows += 1;
                    // Estimate bytes as a rough sum of parameter sizes.
                    total_bytes += params.iter().map(|p| p.byte_size() as u64).sum::<u64>();
                }
            }
        }

        // Commit transaction.
        transaction
            .commit()
            .await
            .map_err(|e| format!("failed to commit transaction: {e}"))?;

        // Create indexes after data is written.
        if !pg_config.indexes.is_empty() {
            for (i, columns) in pg_config.indexes.iter().enumerate() {
                if columns.is_empty() {
                    continue;
                }
                let idx_name = format!("idx_{}_{}", table.replace('"', ""), columns.join("_"));
                let col_list: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
                let sql = format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {} ({})",
                    quote_ident(&idx_name),
                    quote_ident(table),
                    col_list.join(", ")
                );
                debug!(index = i, sql = %sql, "creating index");
                client
                    .execute(&sql, &[])
                    .await
                    .map_err(|e| format!("failed to create index '{idx_name}': {e}"))?;
            }
        }

        debug!(rows = total_rows, "postgresql sink write complete");

        let stats = WriteStats {
            rows_written: total_rows,
            bytes_written: total_bytes,
            duration: start.elapsed(),
        };
        // Populate the rows_inserted/updated/deleted breakdown per strategy:
        //   - Append:         every executed INSERT succeeded → all inserted.
        //   - Upsert:         counts come from the RETURNING (xmax = 0) flag.
        //   - TruncateInsert: rows_deleted = pre-truncate count, rest inserted.
        //   - DeleteInsert:   rows_deleted = pre-pass count, rest inserted.
        let mut receipt = MaterializationReceipt::from_write_stats(&stats, ctx);
        match &write_mode {
            PgWrite::Append => {
                receipt.rows_inserted = total_rows;
            }
            PgWrite::Upsert(_) => {
                receipt.rows_inserted = upsert_inserted;
                receipt.rows_updated = upsert_updated;
            }
            PgWrite::TruncateInsert => {
                receipt.rows_inserted = total_rows;
                receipt.rows_deleted = total_truncated;
            }
            PgWrite::DeleteInsert(_) => {
                receipt.rows_inserted = total_rows;
                receipt.rows_deleted = total_deleted;
            }
            PgWrite::Snapshot { .. } => unreachable!("snapshot path returns early above"),
        }
        Ok(receipt)
    }

    fn validate_config(&self, config: &SinkConfig) -> Result<(), ProviderError> {
        let pg_config: PostgreSqlConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid postgresql sink config: {e}"))?;

        if pg_config.table.is_none() {
            return Err("postgresql sink requires a 'table' name".into());
        }

        if pg_config.connection_string.is_empty() {
            return Err("postgresql sink requires a 'connection_string'".into());
        }

        // Validate the materialization policy maps to a supported PG write mode.
        // (engine-side validation already enforces unique_keys-presence rules,
        // but we re-check here to surface a clear connector error instead of a
        // generic engine error.)
        let _ = pg_write_from_policy(config.materialization.as_ref())?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SQL generation
// ---------------------------------------------------------------------------

/// Build per-column DDL fragments (`"name" TYPE`) for every field in an Arrow
/// schema. Shared between the plain `CREATE TABLE` path and the snapshot
/// path (which appends SCD2 metadata columns to this list).
pub(crate) fn build_business_column_ddls(schema: &Schema) -> Result<Vec<String>, ProviderError> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let pg_type = arrow_type_to_pg(field.data_type())?;
            // Always allow NULLs in auto-created tables — Arrow schemas from
            // query results don't always reflect actual nullability accurately
            // (e.g. LEFT JOINs may produce nulls in fields marked non-nullable).
            Ok(format!("{} {pg_type}", quote_ident(field.name())))
        })
        .collect()
}

/// Build a CREATE TABLE IF NOT EXISTS statement from an Arrow schema.
/// Used by tests; the live write path inlines the same shape so it can
/// dispatch on snapshot vs. non-snapshot.
#[cfg(test)]
fn build_create_table_sql(table: &str, schema: &Schema) -> Result<String, ProviderError> {
    let columns = build_business_column_ddls(schema)?;
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_ident(table),
        columns.join(", ")
    ))
}

/// Query `information_schema.columns` for the column names that already exist
/// on the target table. Used by the `append_new_columns` schema-evolution path
/// to compute "what's in the incoming Arrow schema that the target doesn't
/// have yet?" by introspecting the *target*, not by trusting cached state.
async fn fetch_existing_columns(
    tx: &tokio_postgres::Transaction<'_>,
    table: &str,
) -> Result<std::collections::HashSet<String>, tokio_postgres::Error> {
    // Allow `schema.table` syntax in the user-facing table name. Strip
    // any quotes the user supplied so the query matches what postgres
    // stores in information_schema (lowercase, unquoted).
    let unquoted = table.replace('"', "");
    let (schema_name, table_name) = match unquoted.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => ("public".to_string(), unquoted),
    };
    let rows = tx
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2",
            &[&schema_name, &table_name],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Build `ALTER TABLE ... ADD COLUMN` statements for every Arrow field whose
/// name is missing from `existing`. Columns are always added nullable —
/// existing rows have no value, so requiring `NOT NULL` would fail without a
/// default and v1 keeps the surface small.
fn build_add_column_stmts(
    table: &str,
    schema: &Schema,
    existing: &std::collections::HashSet<String>,
) -> Result<Vec<String>, ProviderError> {
    let mut out = Vec::new();
    for field in schema.fields() {
        if existing.contains(field.name()) {
            continue;
        }
        let pg_type = arrow_type_to_pg(field.data_type())?;
        out.push(format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {pg_type}",
            quote_ident(table),
            quote_ident(field.name())
        ));
    }
    Ok(out)
}

/// Build the INSERT SQL statement, handling write strategy variations.
fn build_insert_sql(
    table: &str,
    schema: &Schema,
    write_mode: &PgWrite,
) -> Result<String, ProviderError> {
    let col_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect();

    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("${i}")).collect();

    let mut sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_ident(table),
        col_names.join(", "),
        placeholders.join(", ")
    );

    if let PgWrite::Upsert(unique_keys) = write_mode {
        if unique_keys.is_empty() {
            return Err("merge strategy requires unique_keys".into());
        }

        let conflict_cols: Vec<String> = unique_keys.iter().map(|k| quote_ident(k)).collect();

        // Update all non-key columns.
        let update_cols: Vec<String> = schema
            .fields()
            .iter()
            .filter(|f| !unique_keys.contains(f.name()))
            .map(|f| {
                let quoted = quote_ident(f.name());
                format!("{quoted} = EXCLUDED.{quoted}")
            })
            .collect();

        if update_cols.is_empty() {
            sql.push_str(&format!(
                " ON CONFLICT ({}) DO NOTHING",
                conflict_cols.join(", ")
            ));
        } else {
            sql.push_str(&format!(
                " ON CONFLICT ({}) DO UPDATE SET {}",
                conflict_cols.join(", "),
                update_cols.join(", ")
            ));
        }
        // RETURNING (xmax = 0) lets us distinguish freshly-inserted rows
        // (xmax = 0) from rows that triggered the DO UPDATE branch (xmax != 0)
        // for the MaterializationReceipt breakdown. DO NOTHING conflicts
        // simply emit no row, which is correct.
        sql.push_str(" RETURNING (xmax = 0) AS inserted");
    }

    Ok(sql)
}

/// Build a `DELETE FROM t WHERE k1 = $1 AND k2 = $2 ...` statement for the
/// given unique-key columns. Used by the `delete_insert` write strategy.
fn build_delete_sql(table: &str, keys: &[String]) -> String {
    let preds: Vec<String> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| format!("{} = ${}", quote_ident(k), i + 1))
        .collect();
    format!(
        "DELETE FROM {} WHERE {}",
        quote_ident(table),
        preds.join(" AND ")
    )
}

/// Resolve unique-key column names to their positional indices in the
/// incoming Arrow schema, returning a clear error if any are missing.
fn resolve_key_indices(schema: &Schema, keys: &[String]) -> Result<Vec<usize>, ProviderError> {
    keys.iter()
        .map(|k| {
            schema.index_of(k).map_err(|_| {
                format!("delete_insert unique key '{k}' is not present in incoming schema").into()
            })
        })
        .collect()
}

/// Map an Arrow data type to a PostgreSQL column type string.
fn arrow_type_to_pg(data_type: &DataType) -> Result<&'static str, ProviderError> {
    match data_type {
        DataType::Boolean => Ok("BOOLEAN"),
        DataType::Int8 | DataType::UInt8 | DataType::Int16 => Ok("SMALLINT"),
        DataType::UInt16 | DataType::Int32 => Ok("INTEGER"),
        DataType::UInt32 | DataType::Int64 => Ok("BIGINT"),
        DataType::UInt64 => Ok("BIGINT"),
        DataType::Float16 | DataType::Float32 => Ok("REAL"),
        DataType::Float64 => Ok("DOUBLE PRECISION"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("TEXT"),
        DataType::Binary | DataType::LargeBinary => Ok("BYTEA"),
        DataType::Date32 | DataType::Date64 => Ok("DATE"),
        DataType::Timestamp(_, Some(_)) => Ok("TIMESTAMPTZ"),
        DataType::Timestamp(_, None) => Ok("TIMESTAMP"),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Ok("NUMERIC"),
        _ => Err(format!("unsupported Arrow type for postgresql sink: {data_type}").into()),
    }
}

/// Validate that all fields in the schema can be mapped to PostgreSQL types.
fn validate_schema_compatibility(schema: &Schema) -> Result<(), ProviderError> {
    for field in schema.fields() {
        arrow_type_to_pg(field.data_type())?;
    }
    Ok(())
}

/// Quote a SQL identifier.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// Row parameter extraction
// ---------------------------------------------------------------------------

/// A boxed SQL parameter with an estimated byte size for stats tracking.
struct SqlParam {
    value: Box<dyn ToSql + Sync + Send>,
    size: usize,
}

impl SqlParam {
    fn new<T: ToSql + Sync + Send + 'static>(val: T, size: usize) -> Self {
        Self {
            value: Box::new(val),
            size,
        }
    }

    fn null() -> Self {
        let v: Option<String> = None;
        Self {
            value: Box::new(v),
            size: 0,
        }
    }

    fn as_ref(&self) -> &(dyn ToSql + Sync) {
        &*self.value
    }

    fn byte_size(&self) -> usize {
        self.size
    }
}

/// Extract a single row from a RecordBatch as a vector of SQL parameters.
fn extract_row_params(batch: &RecordBatch, row_idx: usize) -> Result<Vec<SqlParam>, ProviderError> {
    let mut params = Vec::with_capacity(batch.num_columns());

    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx);
        let schema = batch.schema();
        let field = schema.field(col_idx);

        if col.is_null(row_idx) {
            // Use a typed null so tokio-postgres can match the parameter type.
            let param = match field.data_type() {
                DataType::Boolean => SqlParam::new(None::<bool>, 0),
                DataType::Int8 | DataType::UInt8 | DataType::Int16 => SqlParam::new(None::<i16>, 0),
                DataType::UInt16 | DataType::Int32 => SqlParam::new(None::<i32>, 0),
                DataType::UInt32 | DataType::Int64 | DataType::UInt64 => {
                    SqlParam::new(None::<i64>, 0)
                }
                DataType::Float16 | DataType::Float32 => SqlParam::new(None::<f32>, 0),
                DataType::Float64 | DataType::Decimal128(_, _) => SqlParam::new(None::<f64>, 0),
                DataType::Date32 | DataType::Date64 => SqlParam::new(None::<chrono::NaiveDate>, 0),
                DataType::Timestamp(_, Some(_)) => {
                    SqlParam::new(None::<chrono::DateTime<chrono::Utc>>, 0)
                }
                DataType::Timestamp(_, None) => SqlParam::new(None::<chrono::NaiveDateTime>, 0),
                _ => SqlParam::new(None::<String>, 0), // TEXT fallback
            };
            params.push(param);
            continue;
        }

        let param = extract_typed_param(col, field.data_type(), row_idx)?;
        params.push(param);
    }

    Ok(params)
}

/// Extract a single typed value from an Arrow array at the given row index.
fn extract_typed_param(
    col: &dyn Array,
    data_type: &DataType,
    row_idx: usize,
) -> Result<SqlParam, ProviderError> {
    match data_type {
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            let val = arr.value(row_idx);
            Ok(SqlParam::new(val, 1))
        }
        DataType::Int8 | DataType::UInt8 => {
            let val = if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int8Array>() {
                arr.value(row_idx) as i16
            } else {
                col.as_any()
                    .downcast_ref::<arrow::array::UInt8Array>()
                    .unwrap()
                    .value(row_idx) as i16
            };
            Ok(SqlParam::new(val, 2))
        }
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            let val = arr.value(row_idx);
            Ok(SqlParam::new(val, 2))
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            let val = arr.value(row_idx);
            Ok(SqlParam::new(val, 4))
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            let val = arr.value(row_idx);
            Ok(SqlParam::new(val, 8))
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            let val = arr.value(row_idx);
            Ok(SqlParam::new(val, 4))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let val = arr.value(row_idx);
            Ok(SqlParam::new(val, 8))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            // Double-check nullability — some query engines produce nulls in
            // non-nullable schema fields (e.g. LEFT JOIN results).
            if col.is_null(row_idx) {
                return Ok(SqlParam::null());
            }
            let val = if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                arr.value(row_idx).to_string()
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringViewArray>() {
                arr.value(row_idx).to_string()
            } else {
                col.as_string::<i32>().value(row_idx).to_string()
            };
            let size = val.len();
            Ok(SqlParam::new(val, size))
        }
        DataType::Binary | DataType::LargeBinary => {
            let arr = col.as_any().downcast_ref::<BinaryArray>().unwrap();
            let val = arr.value(row_idx).to_vec();
            let size = val.len();
            Ok(SqlParam::new(val, size))
        }
        DataType::Date32 => {
            let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row_idx);
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = epoch + chrono::Duration::days(days as i64);
            Ok(SqlParam::new(date, 4))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let micros = arr.value(row_idx);
            let dt = chrono::DateTime::from_timestamp_micros(micros)
                .map(|dt| dt.naive_utc())
                .ok_or_else(|| format!("invalid timestamp microseconds: {micros}"))?;
            Ok(SqlParam::new(dt, 8))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let micros = arr.value(row_idx);
            let dt = chrono::DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| format!("invalid timestamp microseconds: {micros}"))?;
            Ok(SqlParam::new(dt, 8))
        }
        DataType::Decimal128(_, scale) => {
            let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw = arr.value(row_idx);
            // Convert to f64 for PostgreSQL NUMERIC. The scale gives decimal places.
            let val = raw as f64 / 10f64.powi(*scale as i32);
            Ok(SqlParam::new(val, 8))
        }
        _ => Err(format!(
            "unsupported Arrow type for postgresql parameter extraction: {data_type}"
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn postgres_sink_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PostgresSink>();
    }

    #[test]
    fn arrow_type_mappings() {
        assert_eq!(arrow_type_to_pg(&DataType::Boolean).unwrap(), "BOOLEAN");
        assert_eq!(arrow_type_to_pg(&DataType::Int16).unwrap(), "SMALLINT");
        assert_eq!(arrow_type_to_pg(&DataType::Int32).unwrap(), "INTEGER");
        assert_eq!(arrow_type_to_pg(&DataType::Int64).unwrap(), "BIGINT");
        assert_eq!(arrow_type_to_pg(&DataType::Float32).unwrap(), "REAL");
        assert_eq!(
            arrow_type_to_pg(&DataType::Float64).unwrap(),
            "DOUBLE PRECISION"
        );
        assert_eq!(arrow_type_to_pg(&DataType::Utf8).unwrap(), "TEXT");
        assert_eq!(arrow_type_to_pg(&DataType::Binary).unwrap(), "BYTEA");
        assert_eq!(arrow_type_to_pg(&DataType::Date32).unwrap(), "DATE");
        assert_eq!(
            arrow_type_to_pg(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            "TIMESTAMP"
        );
        assert_eq!(
            arrow_type_to_pg(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
            .unwrap(),
            "TIMESTAMPTZ"
        );
    }

    #[test]
    fn unsupported_arrow_type_fails() {
        // FixedSizeBinary is not mapped to any PG type.
        assert!(arrow_type_to_pg(&DataType::FixedSizeBinary(16)).is_err());
    }

    #[test]
    fn decimal_type_maps_to_numeric() {
        assert_eq!(
            arrow_type_to_pg(&DataType::Decimal128(10, 2)).unwrap(),
            "NUMERIC"
        );
    }

    #[test]
    fn create_table_sql_basic() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]);

        // All columns allow NULLs in auto-created tables (no NOT NULL constraint).
        let sql = build_create_table_sql("users", &schema).unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS \"users\" (\"id\" BIGINT, \"name\" TEXT, \"score\" DOUBLE PRECISION)"
        );
    }

    #[test]
    fn insert_sql_basic() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let sql = build_insert_sql("users", &schema, &PgWrite::Append).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"id\", \"name\") VALUES ($1, $2)"
        );
    }

    #[test]
    fn insert_sql_upsert() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
        ]);

        let sql =
            build_insert_sql("users", &schema, &PgWrite::Upsert(vec!["id".to_string()])).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"id\", \"name\", \"email\") VALUES ($1, $2, $3) ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"email\" = EXCLUDED.\"email\" RETURNING (xmax = 0) AS inserted"
        );
    }

    #[test]
    fn insert_sql_upsert_all_conflict_keys() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let sql = build_insert_sql(
            "users",
            &schema,
            &PgWrite::Upsert(vec!["id".to_string(), "name".to_string()]),
        )
        .unwrap();
        assert!(sql.contains("DO NOTHING"));
        // DO NOTHING upserts still get RETURNING so the row stream is uniform;
        // no rows come back when nothing was inserted, which is the desired
        // signal for the receipt counters.
        assert!(sql.contains("RETURNING (xmax = 0) AS inserted"));
    }

    #[test]
    fn delete_sql_basic() {
        let sql = build_delete_sql("users", &["id".to_string()]);
        assert_eq!(sql, "DELETE FROM \"users\" WHERE \"id\" = $1");
    }

    #[test]
    fn delete_sql_composite_key() {
        let sql = build_delete_sql("users", &["tenant".to_string(), "id".to_string()]);
        assert_eq!(
            sql,
            "DELETE FROM \"users\" WHERE \"tenant\" = $1 AND \"id\" = $2"
        );
    }

    #[test]
    fn resolve_key_indices_ok_and_err() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        assert_eq!(
            resolve_key_indices(&schema, &["id".to_string()]).unwrap(),
            vec![0]
        );
        assert!(resolve_key_indices(&schema, &["missing".to_string()]).is_err());
    }

    #[test]
    fn delete_insert_policy_translates() {
        use flux_engine::materialization::{MaterializationPolicy, WriteStrategy};
        let policy = MaterializationPolicy {
            write_strategy: WriteStrategy::DeleteInsert,
            unique_keys: Some(vec!["id".into()]),
            ..MaterializationPolicy::default()
        };
        let pg = pg_write_from_policy(Some(&policy)).unwrap();
        assert!(matches!(pg, PgWrite::DeleteInsert(ref k) if k == &vec!["id".to_string()]));
    }

    #[test]
    fn delete_insert_policy_requires_keys() {
        use flux_engine::materialization::{MaterializationPolicy, WriteStrategy};
        let policy = MaterializationPolicy {
            write_strategy: WriteStrategy::DeleteInsert,
            unique_keys: None,
            ..MaterializationPolicy::default()
        };
        assert!(pg_write_from_policy(Some(&policy)).is_err());
    }

    #[test]
    fn delete_insert_uses_plain_insert_sql() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let sql = build_insert_sql(
            "users",
            &schema,
            &PgWrite::DeleteInsert(vec!["id".to_string()]),
        )
        .unwrap();
        // DeleteInsert builds a plain INSERT — the DELETE pass runs separately.
        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"id\", \"name\") VALUES ($1, $2)"
        );
    }

    #[test]
    fn upsert_without_unique_keys_fails() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let result = build_insert_sql("t", &schema, &PgWrite::Upsert(vec![]));
        assert!(result.is_err());
    }

    #[test]
    fn validate_rejects_missing_table() {
        let sink = PostgresSink::new();
        let config = SinkConfig {
            connector: "postgresql".to_string(),
            materialization: None,
            config: serde_json::json!({
                "connection_string": "host=localhost dbname=test"
            }),
        };
        assert!(sink.validate_config(&config).is_err());
    }

    #[test]
    fn validate_rejects_empty_connection_string() {
        let sink = PostgresSink::new();
        let config = SinkConfig {
            connector: "postgresql".to_string(),
            materialization: None,
            config: serde_json::json!({
                "connection_string": "",
                "table": "users"
            }),
        };
        assert!(sink.validate_config(&config).is_err());
    }

    #[test]
    fn validate_rejects_merge_without_unique_keys() {
        use flux_engine::materialization::{MaterializationPolicy, WriteStrategy};
        let sink = PostgresSink::new();
        let config = SinkConfig {
            connector: "postgresql".to_string(),
            materialization: Some(MaterializationPolicy {
                write_strategy: WriteStrategy::Merge,
                unique_keys: None,
                ..MaterializationPolicy::default()
            }),
            config: serde_json::json!({
                "connection_string": "host=localhost",
                "table": "users"
            }),
        };
        assert!(sink.validate_config(&config).is_err());
    }

    #[test]
    fn validate_accepts_valid_config() {
        let sink = PostgresSink::new();
        let config = SinkConfig {
            connector: "postgresql".to_string(),
            materialization: None,
            config: serde_json::json!({
                "connection_string": "host=localhost",
                "table": "users"
            }),
        };
        assert!(sink.validate_config(&config).is_ok());
    }

    #[test]
    fn validate_accepts_merge_with_unique_keys() {
        use flux_engine::materialization::{MaterializationPolicy, WriteStrategy};
        let sink = PostgresSink::new();
        let config = SinkConfig {
            connector: "postgresql".to_string(),
            materialization: Some(MaterializationPolicy {
                write_strategy: WriteStrategy::Merge,
                unique_keys: Some(vec!["id".into()]),
                ..MaterializationPolicy::default()
            }),
            config: serde_json::json!({
                "connection_string": "host=localhost",
                "table": "users"
            }),
        };
        assert!(sink.validate_config(&config).is_ok());
    }

    #[test]
    fn schema_compatibility_rejects_unsupported_types() {
        let schema = Schema::new(vec![Field::new("val", DataType::FixedSizeBinary(16), true)]);
        assert!(validate_schema_compatibility(&schema).is_err());
    }

    #[test]
    fn add_column_stmts_adds_only_missing_fields() {
        use std::collections::HashSet;
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let mut existing = HashSet::new();
        existing.insert("id".to_string());
        let stmts = build_add_column_stmts("users", &schema, &existing).unwrap();
        assert_eq!(stmts.len(), 2);
        assert_eq!(
            stmts[0],
            "ALTER TABLE \"users\" ADD COLUMN IF NOT EXISTS \"name\" TEXT"
        );
        assert_eq!(
            stmts[1],
            "ALTER TABLE \"users\" ADD COLUMN IF NOT EXISTS \"created_at\" TIMESTAMPTZ"
        );
    }

    #[test]
    fn add_column_stmts_noop_when_target_has_all_fields() {
        use std::collections::HashSet;
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let existing: HashSet<String> = ["id".to_string()].into_iter().collect();
        let stmts = build_add_column_stmts("t", &schema, &existing).unwrap();
        assert!(stmts.is_empty());
    }

    #[test]
    fn schema_compatibility_accepts_supported_types() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("score", DataType::Float64, true),
        ]);
        assert!(validate_schema_compatibility(&schema).is_ok());
    }
}
