// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PostgreSQL source connector.
//!
//! Implements a DataFusion [`TableProvider`] that reads from PostgreSQL with
//! filter and projection pushdown. Filters are translated to SQL WHERE clauses
//! and pushed to the database; projections cause only needed columns to be
//! SELECTed.

use std::any::Any;
use std::sync::Arc;

use armillary_datafusion::provider::{ProviderError, SourceConnector};
use armillary_engine::node::SourceConfig;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use tokio_postgres::types::Type;
use tokio_postgres::{NoTls, Row};
use tracing::debug;

use crate::config::PostgreSqlConfig;

// ---------------------------------------------------------------------------
// SourceConnector implementation
// ---------------------------------------------------------------------------

/// Source connector for PostgreSQL databases.
///
/// Creates a [`PostgresTableProvider`] that supports filter and projection
/// pushdown. When the user specifies a `table`, filters are translated to
/// SQL WHERE clauses. When a raw `query` is provided, only projection
/// pushdown is applied (filters are handled by DataFusion post-scan).
pub struct PostgresSource;

impl PostgresSource {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PostgresSource {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceConnector for PostgresSource {
    fn create_table_provider(
        &self,
        config: &SourceConfig,
    ) -> Result<Arc<dyn TableProvider>, ProviderError> {
        let pg_config: PostgreSqlConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid postgresql source config: {e}"))?;

        if pg_config.table.is_none() && pg_config.query.is_none() {
            return Err("postgresql source requires either 'table' or 'query'".into());
        }
        if pg_config.table.is_some() && pg_config.query.is_some() {
            return Err("postgresql source cannot specify both 'table' and 'query'".into());
        }

        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| "postgresql source requires a tokio runtime")?;

        let (schema, pg_types) =
            tokio::task::block_in_place(|| rt.block_on(discover_schema(&pg_config)))?;

        debug!(
            columns = schema.fields().len(),
            table = ?pg_config.table,
            query = ?pg_config.query,
            "discovered postgresql schema"
        );

        Ok(Arc::new(PostgresTableProvider {
            config: pg_config,
            schema: Arc::new(schema),
            pg_types,
        }))
    }
}

// ---------------------------------------------------------------------------
// TableProvider implementation
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct PostgresTableProvider {
    config: PostgreSqlConfig,
    schema: SchemaRef,
    /// Original PostgreSQL types for each column (used for SQL casting).
    pg_types: Vec<Type>,
}

#[async_trait]
impl TableProvider for PostgresTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // Only push filters when reading from a table (not a raw query).
        if self.config.table.is_none() {
            return Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect());
        }

        Ok(filters
            .iter()
            .map(|f| {
                if expr_to_sql(f).is_some() {
                    // Inexact: we apply it in PG, but DataFusion should verify.
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let query = build_query(
            &self.config,
            &self.schema,
            &self.pg_types,
            projection,
            filters,
            limit,
        );

        debug!(query = %query, "executing postgresql query");

        let projected_schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => self.schema.clone(),
        };

        let batches =
            fetch_batches(&self.config.connection_string, &query, &projected_schema).await?;

        let mem_table = MemTable::try_new(projected_schema.clone(), vec![batches])?;
        mem_table.scan(state, None, &[], limit).await
    }
}

// ---------------------------------------------------------------------------
// Schema discovery
// ---------------------------------------------------------------------------

/// Connect to PostgreSQL and discover the schema of the target table or query.
///
/// Uses `PREPARE` on a `LIMIT 0` query to get column metadata without
/// fetching any rows.
async fn discover_schema(config: &PostgreSqlConfig) -> Result<(Schema, Vec<Type>), ProviderError> {
    let (client, connection) = tokio_postgres::connect(&config.connection_string, NoTls)
        .await
        .map_err(|e| format!("failed to connect to postgresql: {e}"))?;

    // Drive the connection in the background.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("postgresql connection error: {e}");
        }
    });

    let discovery_sql = match (&config.table, &config.query) {
        (Some(table), None) => format!("SELECT * FROM {} LIMIT 0", quote_ident(table)),
        (None, Some(query)) => format!("SELECT * FROM ({query}) AS _hf_probe LIMIT 0"),
        _ => unreachable!("validated in create_table_provider"),
    };

    let stmt = client
        .prepare(&discovery_sql)
        .await
        .map_err(|e| format!("schema discovery failed: {e}"))?;

    let columns = stmt.columns();
    let mut fields = Vec::with_capacity(columns.len());
    let mut pg_types = Vec::with_capacity(columns.len());

    for col in columns {
        let arrow_type = pg_type_to_arrow(col.type_());
        // PostgreSQL columns are nullable by default.
        fields.push(Field::new(col.name(), arrow_type, true));
        pg_types.push(col.type_().clone());
    }

    Ok((Schema::new(fields), pg_types))
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Map a PostgreSQL type to the closest Arrow data type.
fn pg_type_to_arrow(pg_type: &Type) -> DataType {
    match *pg_type {
        Type::BOOL => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::NUMERIC => DataType::Float64, // cast to float8 in SQL
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => DataType::Utf8,
        Type::BYTEA => DataType::Binary,
        Type::DATE => DataType::Date32,
        Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        Type::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        // Types that are cast to text in the SQL query.
        Type::JSON | Type::JSONB | Type::UUID => DataType::Utf8,
        _ => DataType::Utf8, // fallback: cast to text
    }
}

/// Returns a SQL cast suffix for PG types that need conversion before
/// extraction (e.g. NUMERIC → `::float8`). Returns `None` if the type
/// can be read natively.
fn pg_type_sql_cast(pg_type: &Type) -> Option<&'static str> {
    match *pg_type {
        Type::NUMERIC => Some("::float8"),
        Type::JSON | Type::JSONB | Type::UUID => Some("::text"),
        _ => {
            // Unknown types get cast to text for safe extraction.
            if !is_natively_supported(pg_type) {
                Some("::text")
            } else {
                None
            }
        }
    }
}

/// Whether a PG type can be extracted without a SQL cast.
fn is_natively_supported(pg_type: &Type) -> bool {
    matches!(
        *pg_type,
        Type::BOOL
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::TEXT
            | Type::VARCHAR
            | Type::BPCHAR
            | Type::NAME
            | Type::BYTEA
            | Type::DATE
            | Type::TIMESTAMP
            | Type::TIMESTAMPTZ
    )
}

// ---------------------------------------------------------------------------
// Query building
// ---------------------------------------------------------------------------

/// Build the SQL query to execute against PostgreSQL, applying projection,
/// filter, and limit pushdown.
fn build_query(
    config: &PostgreSqlConfig,
    schema: &Schema,
    pg_types: &[Type],
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> String {
    let indices: Vec<usize> = match projection {
        Some(proj) => proj.clone(),
        None => (0..schema.fields().len()).collect(),
    };

    // Build SELECT clause with casts for types that need conversion.
    let select_cols: Vec<String> = indices
        .iter()
        .map(|&i| {
            let col_name = quote_ident(schema.field(i).name());
            match pg_type_sql_cast(&pg_types[i]) {
                Some(cast) => format!("{col_name}{cast}"),
                None => col_name,
            }
        })
        .collect();
    let select_clause = select_cols.join(", ");

    let from_clause = match (&config.table, &config.query) {
        (Some(table), None) => quote_ident(table),
        (None, Some(query)) => format!("({query}) AS _hf_subquery"),
        _ => unreachable!(),
    };

    // Translate pushable filters to WHERE clause (table mode only).
    let where_clause = if config.table.is_some() {
        let sql_filters: Vec<String> = filters.iter().filter_map(expr_to_sql).collect();
        if sql_filters.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", sql_filters.join(" AND "))
        }
    } else {
        String::new()
    };

    let limit_clause = match limit {
        Some(n) => format!(" LIMIT {n}"),
        None => String::new(),
    };

    format!("SELECT {select_clause} FROM {from_clause}{where_clause}{limit_clause}")
}

/// Quote a SQL identifier to prevent injection.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// Filter translation (DataFusion Expr → SQL)
// ---------------------------------------------------------------------------

/// Attempt to translate a DataFusion expression to a SQL string.
/// Returns `None` for expressions that cannot be safely pushed down.
fn expr_to_sql(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left_sql = expr_to_sql(left)?;
            let right_sql = expr_to_sql(right)?;
            let op_str = match op {
                Operator::Eq => "=",
                Operator::NotEq => "!=",
                Operator::Lt => "<",
                Operator::LtEq => "<=",
                Operator::Gt => ">",
                Operator::GtEq => ">=",
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::IsDistinctFrom => "IS DISTINCT FROM",
                Operator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
                Operator::LikeMatch => "LIKE",
                Operator::NotLikeMatch => "NOT LIKE",
                _ => return None,
            };
            Some(format!("({left_sql} {op_str} {right_sql})"))
        }
        Expr::Column(col) => Some(quote_ident(&col.name)),
        Expr::Literal(scalar, _) => scalar_to_sql(scalar),
        Expr::Not(inner) => {
            let inner_sql = expr_to_sql(inner)?;
            Some(format!("(NOT {inner_sql})"))
        }
        Expr::IsNull(inner) => {
            let inner_sql = expr_to_sql(inner)?;
            Some(format!("({inner_sql} IS NULL)"))
        }
        Expr::IsNotNull(inner) => {
            let inner_sql = expr_to_sql(inner)?;
            Some(format!("({inner_sql} IS NOT NULL)"))
        }
        Expr::IsTrue(inner) => {
            let inner_sql = expr_to_sql(inner)?;
            Some(format!("({inner_sql} IS TRUE)"))
        }
        Expr::IsFalse(inner) => {
            let inner_sql = expr_to_sql(inner)?;
            Some(format!("({inner_sql} IS FALSE)"))
        }
        Expr::Between(between) => {
            let expr_sql = expr_to_sql(&between.expr)?;
            let low_sql = expr_to_sql(&between.low)?;
            let high_sql = expr_to_sql(&between.high)?;
            let not = if between.negated { "NOT " } else { "" };
            Some(format!(
                "({expr_sql} {not}BETWEEN {low_sql} AND {high_sql})"
            ))
        }
        _ => None,
    }
}

/// Convert a DataFusion scalar value to a SQL literal string.
fn scalar_to_sql(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Null => Some("NULL".to_string()),
        ScalarValue::Boolean(Some(b)) => Some(if *b { "TRUE" } else { "FALSE" }.to_string()),
        ScalarValue::Int8(Some(n)) => Some(n.to_string()),
        ScalarValue::Int16(Some(n)) => Some(n.to_string()),
        ScalarValue::Int32(Some(n)) => Some(n.to_string()),
        ScalarValue::Int64(Some(n)) => Some(n.to_string()),
        ScalarValue::Float32(Some(n)) => Some(n.to_string()),
        ScalarValue::Float64(Some(n)) => Some(n.to_string()),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            // Escape single quotes for SQL safety.
            Some(format!("'{}'", s.replace('\'', "''")))
        }
        // Date32: days since unix epoch → 'YYYY-MM-DD'::date
        ScalarValue::Date32(Some(days)) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
            let date = epoch.checked_add_signed(chrono::Duration::days(*days as i64))?;
            Some(format!("'{}'::date", date.format("%Y-%m-%d")))
        }
        // Date64: milliseconds since unix epoch → 'YYYY-MM-DD'::date
        ScalarValue::Date64(Some(ms)) => {
            let dt = DateTime::<Utc>::from_timestamp_millis(*ms)?;
            Some(format!("'{}'::date", dt.format("%Y-%m-%d")))
        }
        // Timestamp variants. With a timezone we emit ::timestamptz; without
        // we emit ::timestamp. Naive (no tz) values are interpreted in UTC for
        // the purpose of formatting, matching the executor's coercion rules.
        ScalarValue::TimestampSecond(Some(v), tz) => {
            timestamp_literal(DateTime::<Utc>::from_timestamp(*v, 0)?, tz.as_deref())
        }
        ScalarValue::TimestampMillisecond(Some(v), tz) => {
            timestamp_literal(DateTime::<Utc>::from_timestamp_millis(*v)?, tz.as_deref())
        }
        ScalarValue::TimestampMicrosecond(Some(v), tz) => {
            timestamp_literal(DateTime::<Utc>::from_timestamp_micros(*v)?, tz.as_deref())
        }
        ScalarValue::TimestampNanosecond(Some(v), tz) => {
            timestamp_literal(DateTime::<Utc>::from_timestamp_nanos(*v), tz.as_deref())
        }
        _ => None,
    }
}

/// Format a UTC instant as a Postgres SQL literal. When the source ScalarValue
/// carries a timezone we tag the literal as `timestamptz`; otherwise `timestamp`.
fn timestamp_literal(dt: DateTime<Utc>, tz: Option<&str>) -> Option<String> {
    let formatted = dt.format("%Y-%m-%dT%H:%M:%S%.9fZ");
    let suffix = if tz.is_some() {
        "::timestamptz"
    } else {
        "::timestamp"
    };
    Some(format!("'{formatted}'{suffix}"))
}

// ---------------------------------------------------------------------------
// Data fetching and conversion
// ---------------------------------------------------------------------------

/// Connect to PostgreSQL, execute the query, and convert results to Arrow
/// record batches.
async fn fetch_batches(
    connection_string: &str,
    query: &str,
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, ProviderError> {
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
        .await
        .map_err(|e| format!("failed to connect to postgresql: {e}"))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("postgresql connection error: {e}");
        }
    });

    let rows = client
        .query(query, &[])
        .await
        .map_err(|e| format!("postgresql query failed: {e}"))?;

    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema.clone())]);
    }

    rows_to_batches(&rows, schema)
}

/// Convert PostgreSQL rows to Arrow record batches.
///
/// Processes all rows into a single batch. The schema's field data types
/// determine how values are extracted from each row.
fn rows_to_batches(rows: &[Row], schema: &SchemaRef) -> Result<Vec<RecordBatch>, ProviderError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array = build_column(rows, col_idx, field.data_type())?;
        columns.push(array);
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| format!("failed to create record batch: {e}"))?;

    Ok(vec![batch])
}

/// Build an Arrow array from a column across all rows.
fn build_column(
    rows: &[Row],
    col_idx: usize,
    data_type: &DataType,
) -> Result<ArrayRef, ProviderError> {
    match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                let val: Option<bool> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<i16> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<i32> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<i64> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<f32> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<f64> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                let val: Option<String> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                builder.append_option(val.as_deref());
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
            for row in rows {
                let val: Option<Vec<u8>> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                match val {
                    Some(v) => builder.append_value(&v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let mut builder = Date32Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<NaiveDate> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                builder.append_option(val.map(|d| (d - epoch).num_days() as i32));
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                let val: Option<NaiveDateTime> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                builder.append_option(val.map(|dt| dt.and_utc().timestamp_micros()));
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
            let mut builder =
                TimestampMicrosecondBuilder::with_capacity(rows.len()).with_timezone("UTC");
            for row in rows {
                let val: Option<DateTime<Utc>> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                builder.append_option(val.map(|dt| dt.timestamp_micros()));
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => {
            // Fallback: read as String (column should have been cast to text).
            let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                let val: Option<String> = row.try_get(col_idx).map_err(col_err(col_idx))?;
                builder.append_option(val.as_deref());
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Create a human-readable error message for column extraction failures.
fn col_err(col_idx: usize) -> impl Fn(tokio_postgres::Error) -> ProviderError {
    move |e| format!("failed to read column {col_idx}: {e}").into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_source_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PostgresSource>();
    }

    // -- Type mapping tests --

    #[test]
    fn pg_type_bool_maps_to_boolean() {
        assert_eq!(pg_type_to_arrow(&Type::BOOL), DataType::Boolean);
    }

    #[test]
    fn pg_type_integers_map_correctly() {
        assert_eq!(pg_type_to_arrow(&Type::INT2), DataType::Int16);
        assert_eq!(pg_type_to_arrow(&Type::INT4), DataType::Int32);
        assert_eq!(pg_type_to_arrow(&Type::INT8), DataType::Int64);
    }

    #[test]
    fn pg_type_floats_map_correctly() {
        assert_eq!(pg_type_to_arrow(&Type::FLOAT4), DataType::Float32);
        assert_eq!(pg_type_to_arrow(&Type::FLOAT8), DataType::Float64);
        assert_eq!(pg_type_to_arrow(&Type::NUMERIC), DataType::Float64);
    }

    #[test]
    fn pg_type_text_maps_to_utf8() {
        assert_eq!(pg_type_to_arrow(&Type::TEXT), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(&Type::VARCHAR), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(&Type::BPCHAR), DataType::Utf8);
    }

    #[test]
    fn pg_type_timestamps_map_correctly() {
        assert_eq!(
            pg_type_to_arrow(&Type::TIMESTAMP),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            pg_type_to_arrow(&Type::TIMESTAMPTZ),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn pg_type_date_maps_to_date32() {
        assert_eq!(pg_type_to_arrow(&Type::DATE), DataType::Date32);
    }

    #[test]
    fn pg_type_json_maps_to_utf8() {
        assert_eq!(pg_type_to_arrow(&Type::JSON), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(&Type::JSONB), DataType::Utf8);
    }

    // -- SQL cast tests --

    #[test]
    fn numeric_needs_float8_cast() {
        assert_eq!(pg_type_sql_cast(&Type::NUMERIC), Some("::float8"));
    }

    #[test]
    fn json_needs_text_cast() {
        assert_eq!(pg_type_sql_cast(&Type::JSON), Some("::text"));
        assert_eq!(pg_type_sql_cast(&Type::JSONB), Some("::text"));
    }

    #[test]
    fn native_types_need_no_cast() {
        assert_eq!(pg_type_sql_cast(&Type::INT4), None);
        assert_eq!(pg_type_sql_cast(&Type::TEXT), None);
        assert_eq!(pg_type_sql_cast(&Type::BOOL), None);
    }

    // -- Identifier quoting --

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("users"), "\"users\"");
    }

    #[test]
    fn quote_ident_with_double_quotes() {
        assert_eq!(quote_ident("my\"table"), "\"my\"\"table\"");
    }

    // -- Filter translation tests --

    #[test]
    fn translate_simple_equality() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "id",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(42)), None)),
        });
        let sql = expr_to_sql(&expr).unwrap();
        assert_eq!(sql, "(\"id\" = 42)");
    }

    #[test]
    fn translate_string_literal_escapes_quotes() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "name",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("O'Brien".to_string())),
                None,
            )),
        });
        let sql = expr_to_sql(&expr).unwrap();
        assert_eq!(sql, "(\"name\" = 'O''Brien')");
    }

    #[test]
    fn translate_is_null() {
        let expr = Expr::IsNull(Box::new(Expr::Column(
            datafusion::common::Column::new_unqualified("email"),
        )));
        let sql = expr_to_sql(&expr).unwrap();
        assert_eq!(sql, "(\"email\" IS NULL)");
    }

    #[test]
    fn translate_compound_and() {
        let left = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "age",
            ))),
            op: Operator::GtEq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(18)), None)),
        });
        let right = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "active",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
        });
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::And,
            right: Box::new(right),
        });
        let sql = expr_to_sql(&expr).unwrap();
        assert_eq!(sql, "((\"age\" >= 18) AND (\"active\" = TRUE))");
    }

    #[test]
    fn translate_date32_literal() {
        // 2026-04-08 is 20_551 days since 1970-01-01.
        let days = (NaiveDate::from_ymd_opt(2026, 4, 8).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let sql = scalar_to_sql(&ScalarValue::Date32(Some(days))).unwrap();
        assert_eq!(sql, "'2026-04-08'::date");
    }

    #[test]
    fn translate_timestamp_micro_with_tz() {
        // 2026-04-08T12:34:56Z
        let micros = DateTime::parse_from_rfc3339("2026-04-08T12:34:56Z")
            .unwrap()
            .with_timezone(&Utc)
            .timestamp_micros();
        let sql = scalar_to_sql(&ScalarValue::TimestampMicrosecond(
            Some(micros),
            Some("UTC".into()),
        ))
        .unwrap();
        assert_eq!(sql, "'2026-04-08T12:34:56.000000000Z'::timestamptz");
    }

    #[test]
    fn translate_timestamp_nanos_naive_emits_timestamp() {
        let nanos = DateTime::parse_from_rfc3339("2026-04-08T12:34:56.123456789Z")
            .unwrap()
            .with_timezone(&Utc)
            .timestamp_nanos_opt()
            .unwrap();
        let sql = scalar_to_sql(&ScalarValue::TimestampNanosecond(Some(nanos), None)).unwrap();
        assert_eq!(sql, "'2026-04-08T12:34:56.123456789Z'::timestamp");
    }

    #[test]
    fn pushdown_filter_with_timestamp_literal_emits_sql() {
        // The smoking-gun assertion: an incremental watermark filter using a
        // Timestamp scalar must produce a pushable WHERE clause, not fall back
        // to post-scan filtering. This is the dbt-refugee win on Postgres.
        let micros = DateTime::parse_from_rfc3339("2026-04-08T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            .timestamp_micros();
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "updated_at",
            ))),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(
                ScalarValue::TimestampMicrosecond(Some(micros), Some("UTC".into())),
                None,
            )),
        });
        let sql = expr_to_sql(&expr).expect("timestamp filter must be pushable");
        assert_eq!(
            sql,
            "(\"updated_at\" > '2026-04-08T00:00:00.000000000Z'::timestamptz)"
        );
    }

    // -- Query building tests --

    #[test]
    fn build_query_table_select_all() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let pg_types = vec![Type::INT4, Type::TEXT];
        let config = PostgreSqlConfig {
            connection_string: String::new(),
            table: Some("users".to_string()),
            query: None,
            batch_size: None,
            indexes: Vec::new(),
        };

        let sql = build_query(&config, &schema, &pg_types, None, &[], None);
        assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\"");
    }

    #[test]
    fn build_query_table_with_projection() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]);
        let pg_types = vec![Type::INT4, Type::TEXT, Type::FLOAT8];
        let config = PostgreSqlConfig {
            connection_string: String::new(),
            table: Some("users".to_string()),
            query: None,
            batch_size: None,
            indexes: Vec::new(),
        };

        let projection = vec![0, 2];
        let sql = build_query(&config, &schema, &pg_types, Some(&projection), &[], None);
        assert_eq!(sql, "SELECT \"id\", \"score\" FROM \"users\"");
    }

    #[test]
    fn build_query_table_with_filter_and_limit() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let pg_types = vec![Type::INT4, Type::TEXT];
        let config = PostgreSqlConfig {
            connection_string: String::new(),
            table: Some("users".to_string()),
            query: None,
            batch_size: None,
            indexes: Vec::new(),
        };

        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "id",
            ))),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(10)), None)),
        })];

        let sql = build_query(&config, &schema, &pg_types, None, &filters, Some(100));
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE (\"id\" > 10) LIMIT 100"
        );
    }

    #[test]
    fn build_query_with_numeric_cast() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Float64, true),
        ]);
        let pg_types = vec![Type::INT4, Type::NUMERIC];
        let config = PostgreSqlConfig {
            connection_string: String::new(),
            table: Some("orders".to_string()),
            query: None,
            batch_size: None,
            indexes: Vec::new(),
        };

        let sql = build_query(&config, &schema, &pg_types, None, &[], None);
        assert_eq!(sql, "SELECT \"id\", \"amount\"::float8 FROM \"orders\"");
    }

    #[test]
    fn build_query_raw_query_no_filters() {
        let schema = Schema::new(vec![Field::new("total", DataType::Int64, true)]);
        let pg_types = vec![Type::INT8];
        let config = PostgreSqlConfig {
            connection_string: String::new(),
            table: None,
            query: Some("SELECT count(*) AS total FROM users".to_string()),
            batch_size: None,
            indexes: Vec::new(),
        };

        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "total",
            ))),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(0)), None)),
        })];

        // Filters should be ignored for raw query mode.
        let sql = build_query(&config, &schema, &pg_types, None, &filters, None);
        assert_eq!(
            sql,
            "SELECT \"total\" FROM (SELECT count(*) AS total FROM users) AS _hf_subquery"
        );
    }

    // -- Config validation tests --

    #[test]
    fn rejects_config_without_table_or_query() {
        let source = PostgresSource::new();
        let config = SourceConfig {
            connector: "postgresql".to_string(),
            config: serde_json::json!({
                "connection_string": "host=localhost dbname=test"
            }),
            cache_row_limit: None,
        };
        let result = source.create_table_provider(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires"));
    }

    #[test]
    fn rejects_config_with_both_table_and_query() {
        let source = PostgresSource::new();
        let config = SourceConfig {
            connector: "postgresql".to_string(),
            config: serde_json::json!({
                "connection_string": "host=localhost dbname=test",
                "table": "users",
                "query": "SELECT * FROM users"
            }),
            cache_row_limit: None,
        };
        let result = source.create_table_provider(&config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot specify both")
        );
    }
}
