// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PostgreSQL sink connector.
//!
//! Writes Arrow record batches to a PostgreSQL table with support for
//! insert, upsert (ON CONFLICT DO UPDATE), truncate-and-insert, and append
//! write modes. Writes are wrapped in a transaction for all-or-nothing semantics.

use std::time::Instant;

use arrow::array::{
    Array, AsArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use flux_datafusion::provider::{PipelineSink, ProviderError, WriteOptions, WriteStats};
use flux_engine::node::SinkConfig;
use tokio_postgres::NoTls;
use tokio_postgres::types::ToSql;
use tracing::debug;

use crate::config::{PostgreSqlConfig, PostgresWriteMode};

const DEFAULT_BATCH_SIZE: usize = 1000;

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
    ) -> Result<WriteStats, ProviderError> {
        let start = Instant::now();

        let pg_config: PostgreSqlConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid postgresql sink config: {e}"))?;

        if data.is_empty() {
            return Ok(WriteStats {
                rows_written: 0,
                bytes_written: 0,
                duration: start.elapsed(),
            });
        }

        let table = pg_config
            .table
            .as_deref()
            .ok_or("postgresql sink requires a 'table' name")?;

        let write_mode = pg_config
            .write_mode
            .clone()
            .unwrap_or(PostgresWriteMode::Insert);
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

        // Auto-create table if it doesn't exist.
        let create_sql = build_create_table_sql(table, &schema)?;
        client
            .execute(&create_sql, &[])
            .await
            .map_err(|e| format!("failed to auto-create table '{table}': {e}"))?;

        // Validate schema compatibility.
        validate_schema_compatibility(&schema)?;

        // Begin transaction.
        let transaction = client
            .transaction()
            .await
            .map_err(|e| format!("failed to begin transaction: {e}"))?;

        // Handle truncate mode.
        if matches!(write_mode, PostgresWriteMode::TruncateInsert) {
            let truncate_sql = format!("TRUNCATE {}", quote_ident(table));
            debug!(sql = %truncate_sql, "truncating table");
            transaction
                .execute(&truncate_sql, &[])
                .await
                .map_err(|e| format!("failed to truncate table '{table}': {e}"))?;
        }

        // Build the INSERT statement.
        let insert_sql = build_insert_sql(table, &schema, &write_mode, &pg_config.conflict_keys)?;
        debug!(sql = %insert_sql, batch_size = batch_size, "inserting rows");

        let mut total_rows: u64 = 0;
        let mut total_bytes: u64 = 0;

        for batch in &data {
            let num_rows = batch.num_rows();
            for chunk_start in (0..num_rows).step_by(batch_size) {
                let chunk_end = (chunk_start + batch_size).min(num_rows);

                for row_idx in chunk_start..chunk_end {
                    let params = extract_row_params(batch, row_idx)?;
                    let param_refs: Vec<&(dyn ToSql + Sync)> =
                        params.iter().map(|p| p.as_ref()).collect();

                    if let Err(e) = transaction
                        .execute(&insert_sql, &param_refs)
                        .await
                    {
                        let schema = batch.schema();
                        let col_info: Vec<String> = schema
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(i, f)| {
                                let is_null = batch.column(i).is_null(row_idx);
                                format!("  ${}: {} ({}) null={}", i + 1, f.name(), f.data_type(), is_null)
                            })
                            .collect();
                        return Err(format!(
                            "failed to insert row {row_idx}: {e}\n{}", col_info.join("\n")
                        ).into());
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
                let idx_name = format!(
                    "idx_{}_{}",
                    table.replace('"', ""),
                    columns.join("_")
                );
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

        Ok(WriteStats {
            rows_written: total_rows,
            bytes_written: total_bytes,
            duration: start.elapsed(),
        })
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

        // Upsert mode requires conflict keys.
        if matches!(pg_config.write_mode, Some(PostgresWriteMode::Upsert))
            && pg_config.conflict_keys.is_empty()
        {
            return Err("postgresql upsert mode requires 'conflict_keys' to be specified".into());
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SQL generation
// ---------------------------------------------------------------------------

/// Build a CREATE TABLE IF NOT EXISTS statement from an Arrow schema.
fn build_create_table_sql(table: &str, schema: &Schema) -> Result<String, ProviderError> {
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| {
            let pg_type = arrow_type_to_pg(field.data_type())?;
            // Always allow NULLs in auto-created tables — Arrow schemas from
            // query results don't always reflect actual nullability accurately
            // (e.g. LEFT JOINs may produce nulls in fields marked non-nullable).
            Ok(format!("{} {pg_type}", quote_ident(field.name())))
        })
        .collect::<Result<Vec<_>, ProviderError>>()?;

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_ident(table),
        columns.join(", ")
    ))
}

/// Build the INSERT SQL statement, handling write mode variations.
fn build_insert_sql(
    table: &str,
    schema: &Schema,
    write_mode: &PostgresWriteMode,
    conflict_keys: &[String],
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

    if matches!(write_mode, PostgresWriteMode::Upsert) {
        if conflict_keys.is_empty() {
            return Err("upsert mode requires conflict_keys".into());
        }

        let conflict_cols: Vec<String> = conflict_keys.iter().map(|k| quote_ident(k)).collect();

        // Update all non-conflict columns.
        let update_cols: Vec<String> = schema
            .fields()
            .iter()
            .filter(|f| !conflict_keys.contains(f.name()))
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
    }

    Ok(sql)
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
                DataType::UInt32 | DataType::Int64 | DataType::UInt64 => SqlParam::new(None::<i64>, 0),
                DataType::Float16 | DataType::Float32 => SqlParam::new(None::<f32>, 0),
                DataType::Float64 | DataType::Decimal128(_, _) => SqlParam::new(None::<f64>, 0),
                DataType::Date32 | DataType::Date64 => SqlParam::new(None::<chrono::NaiveDate>, 0),
                DataType::Timestamp(_, Some(_)) => SqlParam::new(None::<chrono::DateTime<chrono::Utc>>, 0),
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
                col.as_any().downcast_ref::<arrow::array::UInt8Array>().unwrap().value(row_idx) as i16
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
            } else if let Some(arr) =
                col.as_any().downcast_ref::<arrow::array::StringViewArray>()
            {
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

        let sql = build_insert_sql("users", &schema, &PostgresWriteMode::Insert, &[]).unwrap();
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

        let sql = build_insert_sql(
            "users",
            &schema,
            &PostgresWriteMode::Upsert,
            &["id".to_string()],
        )
        .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"id\", \"name\", \"email\") VALUES ($1, $2, $3) ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"email\" = EXCLUDED.\"email\""
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
            &PostgresWriteMode::Upsert,
            &["id".to_string(), "name".to_string()],
        )
        .unwrap();
        assert!(sql.contains("DO NOTHING"));
    }

    #[test]
    fn upsert_without_conflict_keys_fails() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let result = build_insert_sql("t", &schema, &PostgresWriteMode::Upsert, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn validate_rejects_missing_table() {
        let sink = PostgresSink::new();
        let config = SinkConfig {
            connector: "postgresql".to_string(),
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
            config: serde_json::json!({
                "connection_string": "",
                "table": "users"
            }),
        };
        assert!(sink.validate_config(&config).is_err());
    }

    #[test]
    fn validate_rejects_upsert_without_conflict_keys() {
        let sink = PostgresSink::new();
        let config = SinkConfig {
            connector: "postgresql".to_string(),
            config: serde_json::json!({
                "connection_string": "host=localhost",
                "table": "users",
                "write_mode": "upsert"
            }),
        };
        assert!(sink.validate_config(&config).is_err());
    }

    #[test]
    fn validate_accepts_valid_config() {
        let sink = PostgresSink::new();
        let config = SinkConfig {
            connector: "postgresql".to_string(),
            config: serde_json::json!({
                "connection_string": "host=localhost",
                "table": "users"
            }),
        };
        assert!(sink.validate_config(&config).is_ok());
    }

    #[test]
    fn validate_accepts_upsert_with_conflict_keys() {
        let sink = PostgresSink::new();
        let config = SinkConfig {
            connector: "postgresql".to_string(),
            config: serde_json::json!({
                "connection_string": "host=localhost",
                "table": "users",
                "write_mode": "upsert",
                "conflict_keys": ["id"]
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
