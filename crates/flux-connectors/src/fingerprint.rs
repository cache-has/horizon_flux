// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Resource fingerprinting for cross-pipeline lineage.
//!
//! Each connector type that participates in lineage implements a fingerprint
//! function that returns a canonical, secret-free identifier for the external
//! resource it reads from or writes to.

use flux_engine::lineage::ResourceFingerprint;
use std::path::PathBuf;

/// Compute a resource fingerprint for a connector node.
///
/// Returns `None` for connectors that don't participate in lineage:
/// - `stdout` — ephemeral debug output, no persistent resource
/// - `rest_api` / `rest` / `http` — no reliable resource identity
///
/// Returns `Some(fingerprint)` for connectors with identifiable resources:
/// - `file` / `csv` / `parquet` — `file://` or cloud URL
/// - `postgresql` / `postgres` — `postgres://host:port/db/schema.table`
pub fn fingerprint(connector: &str, config: &serde_json::Value) -> Option<ResourceFingerprint> {
    match connector {
        "file" | "csv" | "parquet" => fingerprint_file(config),
        "postgresql" | "postgres" => fingerprint_postgres(config),
        "rest_api" | "rest" | "http" => None,
        "stdout" => None,
        // Unknown connectors (including plugins) don't participate in static
        // lineage. Plugins can declare resources via the DeclareResource message.
        _ => None,
    }
}

/// File connector fingerprint.
///
/// Produces fingerprints like:
/// - `file:///absolute/path/to/data.csv`
/// - `s3://bucket/path/to/data/`
/// - `gs://bucket/path/`
/// - `az://container/path/`
fn fingerprint_file(config: &serde_json::Value) -> Option<ResourceFingerprint> {
    let path_str = config.get("path")?.as_str()?;

    // Cloud URLs pass through with lowercased scheme and normalized trailing.
    if path_str.starts_with("s3://")
        || path_str.starts_with("gs://")
        || path_str.starts_with("az://")
        || path_str.starts_with("https://")
        || path_str.starts_with("http://")
    {
        return Some(ResourceFingerprint::new(path_str.to_string()));
    }

    // Local path: canonicalize to absolute.
    let path = PathBuf::from(path_str);
    let canonical = if path.is_absolute() {
        path
    } else {
        // Best-effort: try to canonicalize, fall back to as-is.
        std::fs::canonicalize(&path).unwrap_or(std::env::current_dir().ok()?.join(&path))
    };

    Some(ResourceFingerprint::new(format!(
        "file://{}",
        canonical.display()
    )))
}

/// PostgreSQL connector fingerprint.
///
/// Produces fingerprints like: `postgres://host:port/dbname/schema.table`
///
/// Parses both URI-style (`postgres://user:pass@host:port/db`) and
/// key-value style (`host=localhost port=5432 dbname=mydb`) connection strings.
/// Strips credentials entirely.
fn fingerprint_postgres(config: &serde_json::Value) -> Option<ResourceFingerprint> {
    let conn_str = config.get("connection_string")?.as_str()?;
    let table = config.get("table")?.as_str()?;

    // If the table is empty, we can't fingerprint.
    if table.is_empty() {
        return None;
    }

    let (host, port, dbname) = parse_postgres_connection(conn_str)?;

    // Normalize table name: strip quotes, default schema to "public".
    let (schema, table_name) = parse_table_name(table);

    Some(ResourceFingerprint::new(format!(
        "postgres://{}:{}/{}/{}.{}",
        host.to_lowercase(),
        port,
        dbname,
        schema,
        table_name,
    )))
}

/// Parse host, port, and dbname from a Postgres connection string.
///
/// Supports both formats:
/// - URI: `postgres://user:pass@host:port/dbname?params`
/// - Key-value: `host=localhost port=5432 dbname=mydb user=x password=y`
/// - Secret-templated: `{{ secret:pg_conn }}` — returns None (can't fingerprint)
fn parse_postgres_connection(conn_str: &str) -> Option<(String, u16, String)> {
    // Skip templated secrets — they can't be parsed statically.
    if conn_str.contains("{{") && conn_str.contains("}}") {
        // If the entire connection string is a secret, we can't fingerprint.
        // But if only parts are templated (e.g. password), we might still parse.
        let trimmed = conn_str.trim();
        if trimmed.starts_with("{{") {
            return None;
        }
    }

    if conn_str.starts_with("postgres://") || conn_str.starts_with("postgresql://") {
        parse_postgres_uri(conn_str)
    } else {
        parse_postgres_kv(conn_str)
    }
}

/// Parse a Postgres URI-style connection string.
fn parse_postgres_uri(uri: &str) -> Option<(String, u16, String)> {
    // Strip scheme.
    let after_scheme = uri.split("://").nth(1)?;

    // Split off query parameters.
    let main_part = after_scheme.split('?').next()?;

    // Split userinfo@hostinfo/dbname.
    let (host_part, dbname) = if let Some(at_pos) = main_part.rfind('@') {
        let after_at = &main_part[at_pos + 1..];
        // after_at is "host:port/dbname" or "host/dbname" or "host:port" or "host"
        split_host_db(after_at)
    } else {
        // No credentials in the URI.
        split_host_db(main_part)
    };

    let (host, port) = split_host_port(&host_part);

    Some((host, port, dbname))
}

/// Split "host:port/dbname" into (host:port, dbname).
fn split_host_db(s: &str) -> (String, String) {
    if let Some(slash_pos) = s.find('/') {
        let host_port = s[..slash_pos].to_string();
        let db = s[slash_pos + 1..].to_string();
        (host_port, db)
    } else {
        (s.to_string(), String::new())
    }
}

/// Split "host:port" into (host, port), defaulting port to 5432.
fn split_host_port(s: &str) -> (String, u16) {
    if let Some(colon_pos) = s.rfind(':') {
        let host = s[..colon_pos].to_string();
        let port = s[colon_pos + 1..].parse().unwrap_or(5432);
        (host, port)
    } else {
        (s.to_string(), 5432)
    }
}

/// Parse a key-value style Postgres connection string.
fn parse_postgres_kv(conn_str: &str) -> Option<(String, u16, String)> {
    let mut host = "localhost".to_string();
    let mut port: u16 = 5432;
    let mut dbname = String::new();

    for part in conn_str.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            match key {
                "host" | "hostaddr" => host = value.to_string(),
                "port" => port = value.parse().unwrap_or(5432),
                "dbname" => dbname = value.to_string(),
                // Intentionally skip user, password, and other keys.
                _ => {}
            }
        }
    }

    if dbname.is_empty() {
        return None;
    }

    Some((host, port, dbname))
}

/// Parse a table reference into (schema, table), defaulting schema to "public".
/// Strips double-quotes from identifiers.
fn parse_table_name(table: &str) -> (String, String) {
    let unquoted = table.replace('"', "");
    if let Some((schema, name)) = unquoted.split_once('.') {
        (schema.to_string(), name.to_string())
    } else {
        ("public".to_string(), unquoted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -----------------------------------------------------------------------
    // File fingerprinting
    // -----------------------------------------------------------------------

    #[test]
    fn file_absolute_path() {
        // `/data/orders.csv` is absolute on Unix but drive-relative on Windows
        // (where it resolves against the current drive, e.g. `D:/data/...`), so
        // assert the stable shape rather than a platform-specific string.
        let config = json!({"path": "/data/orders.csv", "format": "csv"});
        let fp = fingerprint("file", &config).unwrap();
        assert!(fp.0.starts_with("file://"), "got {}", fp.0);
        assert!(
            fp.0.replace('\\', "/").ends_with("/data/orders.csv"),
            "got {}",
            fp.0
        );
    }

    #[test]
    fn file_s3_path() {
        let config = json!({"path": "s3://my-bucket/path/to/orders/", "format": "parquet"});
        let fp = fingerprint("csv", &config).unwrap();
        assert_eq!(fp.0, "s3://my-bucket/path/to/orders/");
    }

    #[test]
    fn file_gs_path() {
        let config = json!({"path": "gs://bucket/data.parquet", "format": "parquet"});
        let fp = fingerprint("parquet", &config).unwrap();
        assert_eq!(fp.0, "gs://bucket/data.parquet");
    }

    #[test]
    fn file_az_path() {
        let config = json!({"path": "az://container/path/", "format": "parquet"});
        let fp = fingerprint("file", &config).unwrap();
        assert_eq!(fp.0, "az://container/path/");
    }

    #[test]
    fn file_missing_path_returns_none() {
        let config = json!({"format": "csv"});
        assert!(fingerprint("file", &config).is_none());
    }

    // -----------------------------------------------------------------------
    // PostgreSQL fingerprinting
    // -----------------------------------------------------------------------

    #[test]
    fn postgres_uri_style() {
        let config = json!({
            "connection_string": "postgres://user:password@db.example.com:5432/analytics",
            "table": "public.orders"
        });
        let fp = fingerprint("postgresql", &config).unwrap();
        assert_eq!(
            fp.0,
            "postgres://db.example.com:5432/analytics/public.orders"
        );
    }

    #[test]
    fn postgres_uri_no_port() {
        let config = json!({
            "connection_string": "postgres://user:pass@myhost/mydb",
            "table": "users"
        });
        let fp = fingerprint("postgres", &config).unwrap();
        assert_eq!(fp.0, "postgres://myhost:5432/mydb/public.users");
    }

    #[test]
    fn postgres_kv_style() {
        let config = json!({
            "connection_string": "host=db.example.com port=5433 dbname=analytics user=admin password=secret",
            "table": "staging.events"
        });
        let fp = fingerprint("postgresql", &config).unwrap();
        assert_eq!(
            fp.0,
            "postgres://db.example.com:5433/analytics/staging.events"
        );
    }

    #[test]
    fn postgres_kv_defaults() {
        let config = json!({
            "connection_string": "dbname=test",
            "table": "items"
        });
        let fp = fingerprint("postgres", &config).unwrap();
        assert_eq!(fp.0, "postgres://localhost:5432/test/public.items");
    }

    #[test]
    fn postgres_quoted_table() {
        let config = json!({
            "connection_string": "host=localhost dbname=test",
            "table": "\"my_schema\".\"My Table\""
        });
        let fp = fingerprint("postgresql", &config).unwrap();
        assert_eq!(fp.0, "postgres://localhost:5432/test/my_schema.My Table");
    }

    #[test]
    fn postgres_host_lowercased() {
        let config = json!({
            "connection_string": "postgres://user:pass@DB.EXAMPLE.COM:5432/analytics",
            "table": "orders"
        });
        let fp = fingerprint("postgresql", &config).unwrap();
        assert!(fp.0.starts_with("postgres://db.example.com:"));
    }

    #[test]
    fn postgres_missing_table_returns_none() {
        let config = json!({
            "connection_string": "host=localhost dbname=test"
        });
        assert!(fingerprint("postgresql", &config).is_none());
    }

    #[test]
    fn postgres_secret_template_returns_none() {
        let config = json!({
            "connection_string": "{{ secret:pg_conn }}",
            "table": "orders"
        });
        assert!(fingerprint("postgresql", &config).is_none());
    }

    #[test]
    fn postgres_query_only_no_table_returns_none() {
        let config = json!({
            "connection_string": "host=localhost dbname=test",
            "query": "SELECT * FROM orders"
        });
        assert!(fingerprint("postgresql", &config).is_none());
    }

    #[test]
    fn postgres_empty_table_returns_none() {
        let config = json!({
            "connection_string": "host=localhost dbname=test",
            "table": ""
        });
        assert!(fingerprint("postgresql", &config).is_none());
    }

    // -----------------------------------------------------------------------
    // Non-participating connectors
    // -----------------------------------------------------------------------

    #[test]
    fn stdout_returns_none() {
        assert!(fingerprint("stdout", &json!({})).is_none());
    }

    #[test]
    fn rest_api_returns_none() {
        let config = json!({"url": "https://api.example.com/data"});
        assert!(fingerprint("rest_api", &config).is_none());
        assert!(fingerprint("rest", &config).is_none());
        assert!(fingerprint("http", &config).is_none());
    }

    #[test]
    fn unknown_connector_returns_none() {
        assert!(fingerprint("redis", &json!({})).is_none());
    }

    // -----------------------------------------------------------------------
    // Equivalence: same resource must produce same fingerprint
    // -----------------------------------------------------------------------

    #[test]
    fn same_postgres_table_different_credentials() {
        let config_a = json!({
            "connection_string": "postgres://alice:secret1@db.example.com:5432/analytics",
            "table": "orders"
        });
        let config_b = json!({
            "connection_string": "postgres://bob:secret2@db.example.com:5432/analytics",
            "table": "orders"
        });
        let fp_a = fingerprint("postgresql", &config_a).unwrap();
        let fp_b = fingerprint("postgresql", &config_b).unwrap();
        assert_eq!(fp_a, fp_b);
    }

    #[test]
    fn different_tables_different_fingerprints() {
        let config_a = json!({
            "connection_string": "host=localhost dbname=test",
            "table": "orders"
        });
        let config_b = json!({
            "connection_string": "host=localhost dbname=test",
            "table": "customers"
        });
        let fp_a = fingerprint("postgresql", &config_a).unwrap();
        let fp_b = fingerprint("postgresql", &config_b).unwrap();
        assert_ne!(fp_a, fp_b);
    }

    #[test]
    fn connector_aliases_produce_same_fingerprint() {
        let config = json!({"path": "/data/test.csv", "format": "csv"});
        let fp_file = fingerprint("file", &config).unwrap();
        let fp_csv = fingerprint("csv", &config).unwrap();
        let fp_parquet = fingerprint("parquet", &config).unwrap();
        assert_eq!(fp_file, fp_csv);
        assert_eq!(fp_file, fp_parquet);
    }
}
