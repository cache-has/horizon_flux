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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -----------------------------------------------------------------------
    // FileConfig
    // -----------------------------------------------------------------------

    #[test]
    fn file_config_csv_roundtrip() {
        let cfg = FileConfig {
            path: PathBuf::from("/data/input.csv"),
            format: FileFormat::Csv,
            options: FileOptions {
                delimiter: Some('|'),
                has_header: Some(false),
                quote_char: Some('\''),
                null_values: vec!["NA".into(), "".into()],
                ..Default::default()
            },
            table_partition_cols: None,
            storage_options: HashMap::new(),
        };
        let json = serde_json::to_value(&cfg).unwrap();
        let cfg2: FileConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg2.path, PathBuf::from("/data/input.csv"));
        assert_eq!(cfg2.options.delimiter, Some('|'));
        assert_eq!(cfg2.options.has_header, Some(false));
        assert_eq!(cfg2.options.null_values, vec!["NA", ""]);
    }

    #[test]
    fn file_config_parquet_with_storage_options() {
        let cfg = FileConfig {
            path: PathBuf::from("s3://bucket/data.parquet"),
            format: FileFormat::Parquet,
            options: FileOptions {
                compression: Some("zstd".into()),
                row_group_size: Some(100_000),
                ..Default::default()
            },
            table_partition_cols: Some(vec!["year".into(), "month".into()]),
            storage_options: HashMap::from([
                ("aws_region".into(), "us-east-1".into()),
                ("aws_access_key_id".into(), "AKIA...".into()),
            ]),
        };
        let json = serde_json::to_value(&cfg).unwrap();
        let cfg2: FileConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg2.options.compression.as_deref(), Some("zstd"));
        assert_eq!(cfg2.table_partition_cols.as_ref().unwrap().len(), 2);
        assert_eq!(cfg2.storage_options.len(), 2);
    }

    #[test]
    fn file_config_write_mode() {
        let json =
            json!({"path": "/out.csv", "format": "csv", "options": {"write_mode": "append"}});
        let cfg: FileConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(cfg.options.write_mode, Some(WriteMode::Append)));
    }

    // -----------------------------------------------------------------------
    // PostgreSqlConfig
    // -----------------------------------------------------------------------

    #[test]
    fn postgres_config_source_roundtrip() {
        let cfg = PostgreSqlConfig {
            connection_string: "host=localhost dbname=test".into(),
            table: Some("users".into()),
            query: None,
            write_mode: None,
            batch_size: None,
            conflict_keys: vec![],
            indexes: vec![],
        };
        let json = serde_json::to_value(&cfg).unwrap();
        let cfg2: PostgreSqlConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg2.table.as_deref(), Some("users"));
        assert!(cfg2.query.is_none());
    }

    #[test]
    fn postgres_config_sink_upsert() {
        let cfg = PostgreSqlConfig {
            connection_string: "{{ secret:pg_conn }}".into(),
            table: Some("output".into()),
            query: None,
            write_mode: Some(PostgresWriteMode::Upsert),
            batch_size: Some(1000),
            conflict_keys: vec!["id".into()],
            indexes: vec![
                vec!["customer_id".into()],
                vec!["region".into(), "tier".into()],
            ],
        };
        let json = serde_json::to_value(&cfg).unwrap();
        let cfg2: PostgreSqlConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(cfg2.write_mode, Some(PostgresWriteMode::Upsert)));
        assert_eq!(cfg2.batch_size, Some(1000));
        assert_eq!(cfg2.conflict_keys, vec!["id"]);
        assert_eq!(cfg2.indexes.len(), 2);
    }

    #[test]
    fn postgres_write_mode_variants() {
        for (mode, expected) in [
            (PostgresWriteMode::Insert, "insert"),
            (PostgresWriteMode::Upsert, "upsert"),
            (PostgresWriteMode::TruncateInsert, "truncate_insert"),
            (PostgresWriteMode::Append, "append"),
        ] {
            let json = serde_json::to_value(&mode).unwrap();
            assert_eq!(json.as_str().unwrap(), expected);
        }
    }

    // -----------------------------------------------------------------------
    // RestApiConfig
    // -----------------------------------------------------------------------

    #[test]
    fn rest_api_config_minimal() {
        let json = json!({"url": "https://api.example.com/data"});
        let cfg: RestApiConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.url, "https://api.example.com/data");
        assert_eq!(cfg.method, "GET"); // default
        assert!(cfg.auth.is_none());
        assert!(matches!(cfg.response_format, ResponseFormat::Json));
    }

    #[test]
    fn rest_api_auth_variants() {
        let basic = json!({"type": "basic", "username": "user", "password": "pass"});
        let auth: RestApiAuth = serde_json::from_value(basic).unwrap();
        assert!(matches!(auth, RestApiAuth::Basic { .. }));

        let bearer = json!({"type": "bearer", "token": "tok123"});
        let auth: RestApiAuth = serde_json::from_value(bearer).unwrap();
        assert!(matches!(auth, RestApiAuth::Bearer { .. }));

        let api_key = json!({"type": "api_key", "header": "X-API-Key", "value": "key123"});
        let auth: RestApiAuth = serde_json::from_value(api_key).unwrap();
        assert!(matches!(auth, RestApiAuth::ApiKey { .. }));
    }

    #[test]
    fn rest_api_pagination_variants() {
        let offset = json!({"type": "offset", "offset_param": "offset", "limit_param": "limit", "limit": 100});
        let pg: PaginationConfig = serde_json::from_value(offset).unwrap();
        assert!(matches!(pg, PaginationConfig::Offset { limit: 100, .. }));

        let cursor =
            json!({"type": "cursor", "cursor_param": "cursor", "cursor_path": "meta.next"});
        let pg: PaginationConfig = serde_json::from_value(cursor).unwrap();
        assert!(matches!(pg, PaginationConfig::Cursor { .. }));

        let link = json!({"type": "link_header"});
        let pg: PaginationConfig = serde_json::from_value(link).unwrap();
        assert!(matches!(pg, PaginationConfig::LinkHeader));
    }

    #[test]
    fn rest_api_full_config_roundtrip() {
        let cfg = RestApiConfig {
            url: "https://api.example.com/v2/data".into(),
            method: "POST".into(),
            headers: HashMap::from([("Accept".into(), "application/json".into())]),
            auth: Some(RestApiAuth::Bearer {
                token: "{{ secret:api_token }}".into(),
            }),
            response_format: ResponseFormat::Ndjson,
            data_path: Some("data.items".into()),
            pagination: Some(PaginationConfig::Offset {
                offset_param: "offset".into(),
                limit_param: "limit".into(),
                limit: 50,
            }),
            schema: HashMap::from([
                ("id".into(), "int64".into()),
                ("name".into(), "utf8".into()),
            ]),
            rate_limit_ms: Some(200),
            max_retries: Some(5),
            max_pages: Some(100),
        };
        let json = serde_json::to_value(&cfg).unwrap();
        let cfg2: RestApiConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg2.method, "POST");
        assert_eq!(cfg2.headers.len(), 1);
        assert!(cfg2.auth.is_some());
        assert_eq!(cfg2.rate_limit_ms, Some(200));
        assert_eq!(cfg2.max_pages, Some(100));
        assert_eq!(cfg2.schema.len(), 2);
    }

    // -----------------------------------------------------------------------
    // StdoutConfig
    // -----------------------------------------------------------------------

    #[test]
    fn stdout_config_defaults() {
        let cfg: StdoutConfig = serde_json::from_value(json!({})).unwrap();
        assert!(matches!(cfg.format, StdoutFormat::Table));
        assert!(cfg.max_rows.is_none());
    }

    #[test]
    fn stdout_format_variants() {
        for (fmt_str, expected) in [
            ("table", StdoutFormat::Table),
            ("csv", StdoutFormat::Csv),
            ("json", StdoutFormat::Json),
            ("ndjson", StdoutFormat::Ndjson),
        ] {
            let val = json!(fmt_str);
            let fmt: StdoutFormat = serde_json::from_value(val).unwrap();
            assert_eq!(
                std::mem::discriminant(&fmt),
                std::mem::discriminant(&expected)
            );
        }
    }

    // -----------------------------------------------------------------------
    // ConnectorConfig::from_json dispatcher
    // -----------------------------------------------------------------------

    #[test]
    fn from_json_file_aliases() {
        let val = json!({"path": "/data.csv", "format": "csv"});
        for alias in &["file", "csv", "parquet"] {
            let cfg = ConnectorConfig::from_json(alias, &val).unwrap();
            assert!(matches!(cfg, ConnectorConfig::File(_)));
        }
    }

    #[test]
    fn from_json_postgres_aliases() {
        let val = json!({"connection_string": "host=localhost", "table": "t"});
        for alias in &["postgresql", "postgres"] {
            let cfg = ConnectorConfig::from_json(alias, &val).unwrap();
            assert!(matches!(cfg, ConnectorConfig::PostgreSql(_)));
        }
    }

    #[test]
    fn from_json_rest_aliases() {
        let val = json!({"url": "https://example.com"});
        for alias in &["rest_api", "rest", "http"] {
            let cfg = ConnectorConfig::from_json(alias, &val).unwrap();
            assert!(matches!(cfg, ConnectorConfig::RestApi(_)));
        }
    }

    #[test]
    fn from_json_stdout() {
        let cfg = ConnectorConfig::from_json("stdout", &json!({})).unwrap();
        assert!(matches!(cfg, ConnectorConfig::Stdout(_)));
    }

    #[test]
    fn from_json_unknown_connector_errors() {
        let err = ConnectorConfig::from_json("redis", &json!({})).unwrap_err();
        assert!(err.to_string().contains("unknown connector type"));
    }

    #[test]
    fn from_json_invalid_value_errors() {
        // Missing required field "path" for file config.
        let err = ConnectorConfig::from_json("file", &json!({"format": "csv"})).unwrap_err();
        assert!(err.to_string().contains("path"));
    }

    // -----------------------------------------------------------------------
    // valid_config_keys
    // -----------------------------------------------------------------------

    #[test]
    fn valid_config_keys_known_connectors() {
        assert!(
            ConnectorConfig::valid_config_keys("file")
                .unwrap()
                .contains(&"path")
        );
        assert!(
            ConnectorConfig::valid_config_keys("csv")
                .unwrap()
                .contains(&"format")
        );
        assert!(
            ConnectorConfig::valid_config_keys("postgresql")
                .unwrap()
                .contains(&"connection_string")
        );
        assert!(
            ConnectorConfig::valid_config_keys("rest_api")
                .unwrap()
                .contains(&"url")
        );
        assert!(
            ConnectorConfig::valid_config_keys("stdout")
                .unwrap()
                .contains(&"format")
        );
    }

    #[test]
    fn valid_config_keys_unknown_returns_none() {
        assert!(ConnectorConfig::valid_config_keys("redis").is_none());
        assert!(ConnectorConfig::valid_config_keys("").is_none());
    }
}
