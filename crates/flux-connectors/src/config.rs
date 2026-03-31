// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Typed, serializable connector configurations.
//!
//! [`ConnectorConfig`] is the typed counterpart to the opaque
//! `serde_json::Value` stored in pipeline JSON. Each variant holds the
//! validated, strongly-typed options for one connector type.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Typed connector configuration, used for validation and documentation.
///
/// Pipeline JSON stores connector config as opaque `serde_json::Value`.
/// This enum provides the typed representation used at execution time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "connector", rename_all = "snake_case")]
pub enum ConnectorConfig {
    /// Local CSV or Parquet file source/sink.
    File(FileConfig),
    /// PostgreSQL database source/sink.
    #[serde(rename = "postgresql")]
    PostgreSql(PostgreSqlConfig),
    /// REST API source.
    RestApi(Box<RestApiConfig>),
    /// Stdout sink (debugging/CLI).
    Stdout(StdoutConfig),
}

// ---------------------------------------------------------------------------
// File connector (CSV / Parquet)
// ---------------------------------------------------------------------------

/// Configuration for file-based connectors (CSV, Parquet).
///
/// The `path` field accepts local paths, glob patterns, and cloud URLs:
/// - `/local/path/data.csv` or `./relative/path` — local filesystem
/// - `s3://bucket/path` — Amazon S3 (and S3-compatible: MinIO, R2)
/// - `gs://bucket/path` — Google Cloud Storage
/// - `az://container/path` — Azure Blob Storage
/// - `https://host/path` — HTTP/HTTPS (read-only source)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileConfig {
    /// File path, glob pattern, or cloud URL (e.g. `s3://bucket/data.csv`).
    pub path: PathBuf,
    /// File format.
    pub format: FileFormat,
    /// Format-specific options.
    #[serde(default)]
    pub options: FileOptions,
    /// Hive-style partition column names.
    ///
    /// When set, DataFusion extracts partition values from directory names
    /// (e.g., `year=2026/month=03/`) and adds them as additional columns.
    /// All partition columns are typed as `Utf8`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_partition_cols: Option<Vec<String>>,
    /// Per-connector cloud storage options (credentials, endpoint overrides).
    ///
    /// Common keys:
    /// - S3: `aws_access_key_id`, `aws_secret_access_key`, `aws_region`,
    ///   `aws_session_token`, `aws_endpoint`, `aws_allow_http`, `aws_skip_signature`
    /// - GCS: `google_service_account_key`
    /// - Azure: `azure_storage_account_name`, `azure_storage_account_key`
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub storage_options: HashMap<String, String>,
}

/// Supported file formats.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    Csv,
    Parquet,
}

/// Format-specific options for file connectors.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileOptions {
    // -- CSV options --
    /// CSV field delimiter (default: `,`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<char>,
    /// Whether the CSV has a header row (default: `true`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_header: Option<bool>,
    /// CSV quote character (default: `"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_char: Option<char>,
    /// Values to treat as null.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub null_values: Vec<String>,

    // -- Parquet options --
    /// Parquet compression codec (snappy, zstd, gzip, none).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,
    /// Parquet row group size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_group_size: Option<usize>,

    // -- Write options --
    /// Write mode: overwrite or append.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_mode: Option<WriteMode>,
}

/// File write mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Overwrite,
    Append,
}

// ---------------------------------------------------------------------------
// PostgreSQL connector
// ---------------------------------------------------------------------------

/// Configuration for PostgreSQL source/sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgreSqlConfig {
    /// Connection string (may contain `{{ secret:... }}` references).
    pub connection_string: String,
    /// Table name or SQL query (source only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// Raw SQL query to execute (source only; mutually exclusive with `table`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Write mode for sink operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_mode: Option<PostgresWriteMode>,
    /// Batch size for insert operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,
    /// Conflict key columns for upsert mode (used in ON CONFLICT).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflict_keys: Vec<String>,
    /// Indexes to create after writing. Each entry is a list of column names
    /// that form a single index. Example: `[["customer_id"], ["region", "tier"]]`
    /// creates two indexes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<Vec<String>>,
}

/// PostgreSQL sink write modes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresWriteMode {
    /// Simple INSERT statements.
    Insert,
    /// INSERT ... ON CONFLICT DO UPDATE (upsert).
    Upsert,
    /// TRUNCATE then INSERT.
    TruncateInsert,
    /// INSERT without truncating (append).
    Append,
}

// ---------------------------------------------------------------------------
// REST API connector
// ---------------------------------------------------------------------------

/// Configuration for REST API source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestApiConfig {
    /// Request URL.
    pub url: String,
    /// HTTP method (default: GET).
    #[serde(default = "default_http_method")]
    pub method: String,
    /// Request headers.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// Authentication configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<RestApiAuth>,
    /// Response format.
    #[serde(default)]
    pub response_format: ResponseFormat,
    /// JSONPath or field name for extracting the data array from JSON responses.
    /// Supports dot-notation (e.g. `data.items`) and JSON Pointer (e.g. `/data/items`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_path: Option<String>,
    /// Pagination configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationConfig>,
    /// User-defined schema. If omitted, schema is inferred from the first response.
    /// Map of field name → Arrow-compatible type string (e.g. `{"id": "int64", "name": "utf8"}`).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub schema: HashMap<String, String>,
    /// Minimum delay between paginated requests in milliseconds (rate limiting).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_ms: Option<u64>,
    /// Maximum number of retry attempts for failed requests (default: 3).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
    /// Maximum number of pages to fetch (safety limit for pagination).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_pages: Option<usize>,
}

fn default_http_method() -> String {
    "GET".to_string()
}

/// Authentication for REST API requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RestApiAuth {
    /// HTTP Basic authentication.
    Basic { username: String, password: String },
    /// Bearer token authentication.
    Bearer { token: String },
    /// API key in a header.
    ApiKey { header: String, value: String },
}

/// REST API response format.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseFormat {
    #[default]
    Json,
    Ndjson,
    Csv,
}

/// Pagination configuration for REST API sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PaginationConfig {
    /// Offset-based pagination.
    Offset {
        offset_param: String,
        limit_param: String,
        limit: usize,
    },
    /// Cursor-based pagination.
    Cursor {
        cursor_param: String,
        cursor_path: String,
    },
    /// Link-header pagination (RFC 8288).
    LinkHeader,
}

// ---------------------------------------------------------------------------
// Stdout connector
// ---------------------------------------------------------------------------

/// Configuration for the stdout sink (debugging/CLI output).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StdoutConfig {
    /// Output format.
    #[serde(default)]
    pub format: StdoutFormat,
    /// Maximum number of rows to display.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<usize>,
}

/// Stdout output format.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StdoutFormat {
    /// Pretty-printed table (default, similar to psql output).
    #[default]
    Table,
    Csv,
    Json,
    Ndjson,
}

// ---------------------------------------------------------------------------
// Deserialization from opaque JSON
// ---------------------------------------------------------------------------

impl ConnectorConfig {
    /// Return the set of valid top-level config keys for a connector type.
    ///
    /// Used to validate environment override keys — overrides may only set
    /// keys that the target connector actually supports.
    pub fn valid_config_keys(connector: &str) -> Option<&'static [&'static str]> {
        match connector {
            "file" | "csv" | "parquet" => Some(&[
                "path",
                "format",
                "options",
                "table_partition_cols",
                "storage_options",
            ]),
            "postgresql" | "postgres" => Some(&[
                "connection_string",
                "table",
                "query",
                "write_mode",
                "batch_size",
                "conflict_keys",
            ]),
            "rest_api" | "rest" | "http" => Some(&[
                "url",
                "method",
                "headers",
                "auth",
                "response_format",
                "data_path",
                "pagination",
                "schema",
                "rate_limit_ms",
                "max_retries",
                "max_pages",
            ]),
            "stdout" => Some(&["format", "max_rows"]),
            _ => None,
        }
    }

    /// Try to deserialize a `ConnectorConfig` from a connector type name and
    /// opaque JSON value (as stored in pipeline definitions).
    pub fn from_json(
        connector: &str,
        value: &serde_json::Value,
    ) -> Result<Self, serde_json::Error> {
        match connector {
            "file" | "csv" | "parquet" => {
                let cfg: FileConfig = serde_json::from_value(value.clone())?;
                Ok(ConnectorConfig::File(cfg))
            }
            "postgresql" | "postgres" => {
                let cfg: PostgreSqlConfig = serde_json::from_value(value.clone())?;
                Ok(ConnectorConfig::PostgreSql(cfg))
            }
            "rest_api" | "rest" | "http" => {
                let cfg: RestApiConfig = serde_json::from_value(value.clone())?;
                Ok(ConnectorConfig::RestApi(Box::new(cfg)))
            }
            "stdout" => {
                let cfg: StdoutConfig = serde_json::from_value(value.clone())?;
                Ok(ConnectorConfig::Stdout(cfg))
            }
            other => Err(serde::de::Error::custom(format!(
                "unknown connector type: {other}"
            ))),
        }
    }
}
