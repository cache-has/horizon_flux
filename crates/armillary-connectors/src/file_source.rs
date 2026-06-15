// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! File-based source connector (CSV and Parquet).
//!
//! Uses DataFusion's [`ListingTable`] under the hood, which provides automatic
//! filter pushdown (Parquet row-group pruning) and projection pushdown (only
//! reading needed columns).
//!
//! Supports local paths and cloud URLs (`s3://`, `gs://`, `az://`, `https://`).
//! Cloud object stores are registered with the DataFusion session automatically.

use std::sync::Arc;

use armillary_datafusion::provider::{ProviderError, SourceConnector};
use armillary_engine::node::SourceConfig;
use arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use tracing::debug;
use url::Url;

use crate::cloud_store;
use crate::config::{FileConfig, FileFormat};

/// Source connector for CSV and Parquet files (local and cloud).
///
/// Supports:
/// - CSV with configurable delimiter, header, quote char
/// - Parquet with automatic schema discovery
/// - Glob patterns for reading multiple files (e.g. `data/*.csv`)
/// - Cloud URLs: `s3://`, `gs://`, `az://`, `https://`
/// - Filter pushdown (Parquet row-group pruning)
/// - Projection pushdown (only reads needed columns)
pub struct FileSource;

impl FileSource {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FileSource {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceConnector for FileSource {
    fn create_table_provider(
        &self,
        config: &SourceConfig,
    ) -> Result<Arc<dyn TableProvider>, ProviderError> {
        let file_config: FileConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid file source config: {e}"))?;

        let path_str = resolve_path_str(&file_config)?;

        if cloud_store::is_cloud_url(&path_str) {
            // Cloud path — skip local validation, register object store for
            // schema inference, and let DataFusion handle remote listing.
            debug!(path = %path_str, format = ?file_config.format, "creating cloud file source");

            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "file source requires a tokio runtime")?;

            tokio::task::block_in_place(|| {
                rt.block_on(create_cloud_listing_table(&path_str, &file_config))
            })
        } else {
            // Local path — existing behavior with glob validation.
            validate_local_path(&path_str)?;

            debug!(path = %path_str, format = ?file_config.format, "creating local file source");

            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "file source requires a tokio runtime")?;

            tokio::task::block_in_place(|| {
                rt.block_on(create_local_listing_table(&path_str, &file_config))
            })
        }
    }

    fn configure_session(
        &self,
        config: &SourceConfig,
        ctx: &SessionContext,
    ) -> Result<(), ProviderError> {
        let file_config: FileConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid file source config: {e}"))?;

        let path_str = resolve_path_str(&file_config)?;

        cloud_store::register_cloud_store(ctx, &path_str, &file_config.storage_options)?;
        Ok(())
    }
}

/// Resolve the path string from a `FileConfig`.
///
/// For local paths, converts relative paths to absolute using CWD.
/// For cloud URLs, returns the path as-is.
fn resolve_path_str(file_config: &FileConfig) -> Result<String, ProviderError> {
    let raw = file_config
        .path
        .to_str()
        .ok_or_else(|| format!("path is not valid UTF-8: {}", file_config.path.display()))?;

    if cloud_store::is_cloud_url(raw) {
        return Ok(raw.to_string());
    }

    // Local: resolve relative paths to absolute.
    let path = if file_config.path.is_relative() {
        std::env::current_dir()
            .map_err(|e| format!("failed to get current directory: {e}"))?
            .join(&file_config.path)
    } else {
        file_config.path.clone()
    };

    path.to_str()
        .map(|s| s.to_string())
        .ok_or_else(|| format!("path is not valid UTF-8: {}", path.display()).into())
}

/// Validate a local path exists (or glob matches at least one file).
fn validate_local_path(path_str: &str) -> Result<(), ProviderError> {
    let path = std::path::Path::new(path_str);
    if path_str.contains('*') || path_str.contains('?') || path_str.contains('[') {
        let matches: Vec<_> = glob::glob(path_str)
            .map_err(|e| format!("invalid glob pattern '{}': {}", path_str, e))?
            .filter_map(Result::ok)
            .collect();
        if matches.is_empty() {
            return Err(format!("no files found matching pattern '{}'", path_str).into());
        }
        debug!(
            pattern = path_str,
            count = matches.len(),
            "glob matched files"
        );
    } else if !path.exists() {
        return Err(format!("file not found: '{}'", path.display()).into());
    }
    Ok(())
}

/// Create a `ListingTable` for a local file path.
async fn create_local_listing_table(
    path_str: &str,
    file_config: &FileConfig,
) -> Result<Arc<dyn TableProvider>, ProviderError> {
    let ctx = SessionContext::new();
    let state = ctx.state();

    let table_url = ListingTableUrl::parse(path_str)
        .map_err(|e| format!("invalid file path '{}': {}", path_str, e))?;

    let listing_options = build_listing_options(file_config);

    let schema = listing_options
        .infer_schema(&state, &table_url)
        .await
        .map_err(|e| format!("failed to infer schema from '{}': {}", path_str, e))?;

    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(schema);

    let table = ListingTable::try_new(config)
        .map_err(|e| format!("failed to create listing table: {e}"))?;

    Ok(Arc::new(table))
}

/// Create a `ListingTable` for a cloud URL, registering the object store
/// on a temporary session for schema inference.
///
/// Supports glob patterns in cloud URLs (e.g. `s3://bucket/data/*.parquet`).
/// DataFusion's `ListingTableUrl::parse` only handles globs for local paths,
/// so we split the URL into a base + glob and use `try_new` directly.
async fn create_cloud_listing_table(
    path_str: &str,
    file_config: &FileConfig,
) -> Result<Arc<dyn TableProvider>, ProviderError> {
    let ctx = SessionContext::new();

    // Register the cloud object store so DataFusion can access it.
    cloud_store::register_cloud_store(&ctx, path_str, &file_config.storage_options)?;

    let state = ctx.state();

    let table_url = parse_cloud_listing_url(path_str)?;

    let listing_options = build_listing_options(file_config);

    let schema = listing_options
        .infer_schema(&state, &table_url)
        .await
        .map_err(|e| format!("failed to infer schema from '{}': {}", path_str, e))?;

    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(schema);

    let table = ListingTable::try_new(config)
        .map_err(|e| format!("failed to create listing table: {e}"))?;

    Ok(Arc::new(table))
}

/// Characters that start a glob expression.
const GLOB_CHARS: &[char] = &['*', '?', '['];

/// Parse a cloud URL into a [`ListingTableUrl`], handling glob patterns.
///
/// DataFusion's `ListingTableUrl::parse` only splits globs for local filesystem
/// paths. For cloud URLs (`s3://`, `gs://`, `az://`), we detect glob characters,
/// split the URL path into a prefix and glob pattern, and call `try_new` so that
/// the `ListingTable` correctly filters listed objects.
fn parse_cloud_listing_url(path_str: &str) -> Result<ListingTableUrl, ProviderError> {
    let has_glob = path_str.contains(GLOB_CHARS);

    if !has_glob {
        return ListingTableUrl::parse(path_str)
            .map_err(|e| format!("invalid cloud URL '{}': {}", path_str, e).into());
    }

    // Find the last '/' before the first glob character to split prefix / glob.
    let glob_start = path_str
        .find(GLOB_CHARS)
        .expect("glob char must exist (checked above)");

    let last_sep = path_str[..glob_start].rfind('/').unwrap_or(0);
    // prefix includes the trailing '/' so it's a valid directory URL.
    let prefix = &path_str[..=last_sep];
    // glob is the remainder (e.g. "*.parquet" or "year=*/month=*/*.parquet").
    let glob_expr = &path_str[last_sep + 1..];

    debug!(
        prefix = prefix,
        glob = glob_expr,
        "splitting cloud URL into prefix + glob"
    );

    let url =
        Url::parse(prefix).map_err(|e| format!("invalid cloud URL prefix '{}': {}", prefix, e))?;

    let glob_pattern = glob::Pattern::new(glob_expr)
        .map_err(|e| format!("invalid glob pattern '{}': {}", glob_expr, e))?;

    ListingTableUrl::try_new(url, Some(glob_pattern))
        .map_err(|e| format!("failed to create listing URL for '{}': {}", path_str, e).into())
}

/// Build `ListingOptions` from the file config (shared by local and cloud paths).
fn build_listing_options(file_config: &FileConfig) -> ListingOptions {
    let mut options = match file_config.format {
        FileFormat::Csv => {
            let mut csv_format = CsvFormat::default();

            if let Some(delim) = file_config.options.delimiter {
                csv_format = csv_format.with_delimiter(delim as u8);
            }
            if let Some(has_header) = file_config.options.has_header {
                csv_format = csv_format.with_has_header(has_header);
            }
            if let Some(quote) = file_config.options.quote_char {
                csv_format = csv_format.with_quote(quote as u8);
            }

            ListingOptions::new(Arc::new(csv_format)).with_file_extension("")
        }
        FileFormat::Parquet => {
            let parquet_format = ParquetFormat::default();
            ListingOptions::new(Arc::new(parquet_format)).with_file_extension("")
        }
    };

    // Hive-style partition columns (e.g., year=2026/month=03/ → year, month cols).
    if let Some(ref cols) = file_config.table_partition_cols {
        let partition_cols: Vec<(String, DataType)> =
            cols.iter().map(|c| (c.clone(), DataType::Utf8)).collect();
        options = options.with_table_partition_cols(partition_cols);
    }

    options
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use object_store::ObjectStoreExt;

    #[test]
    fn file_source_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FileSource>();
    }

    #[tokio::test]
    async fn cloud_glob_csv_read() {
        use arrow::array::StringArray;
        use object_store::PutPayload;
        use object_store::memory::InMemory;
        use object_store::path::Path as ObjectPath;

        let store = InMemory::new();

        // Put two CSV files under a common prefix.
        store
            .put(
                &ObjectPath::from("data/a.csv"),
                PutPayload::from_static(b"name,value\nalpha,1\n"),
            )
            .await
            .unwrap();
        store
            .put(
                &ObjectPath::from("data/b.csv"),
                PutPayload::from_static(b"name,value\nbeta,2\n"),
            )
            .await
            .unwrap();
        // Unrelated file that should NOT be matched.
        store
            .put(
                &ObjectPath::from("other/c.csv"),
                PutPayload::from_static(b"name,value\ngamma,3\n"),
            )
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let base_url = url::Url::parse("s3://test-bucket").unwrap();
        ctx.register_object_store(&base_url, Arc::new(store));

        let table_url = parse_cloud_listing_url("s3://test-bucket/data/*.csv").unwrap();
        let csv_format = CsvFormat::default().with_has_header(true);
        let listing_opts = ListingOptions::new(Arc::new(csv_format)).with_file_extension("");

        let schema = listing_opts
            .infer_schema(&ctx.state(), &table_url)
            .await
            .unwrap();
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_opts)
            .with_schema(schema);
        let table = ListingTable::try_new(config).unwrap();

        ctx.register_table("test", Arc::new(table)).unwrap();
        let df = ctx
            .sql("SELECT name FROM test ORDER BY name")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let names: Vec<&str> = batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name("name")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..col.len()).map(move |i| col.value(i))
            })
            .collect();

        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn cloud_partitioned_csv_read() {
        use arrow::array::StringArray;
        use object_store::PutPayload;
        use object_store::memory::InMemory;
        use object_store::path::Path as ObjectPath;

        let store = InMemory::new();

        // Hive-partitioned layout: region=us/data.csv, region=eu/data.csv
        store
            .put(
                &ObjectPath::from("data/region=us/part.csv"),
                PutPayload::from_static(b"city\nnew_york\n"),
            )
            .await
            .unwrap();
        store
            .put(
                &ObjectPath::from("data/region=eu/part.csv"),
                PutPayload::from_static(b"city\nlondon\n"),
            )
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let base_url = url::Url::parse("s3://test-bucket").unwrap();
        ctx.register_object_store(&base_url, Arc::new(store));

        let table_url = ListingTableUrl::parse("s3://test-bucket/data/").unwrap();
        let csv_format = CsvFormat::default().with_has_header(true);
        let listing_opts = ListingOptions::new(Arc::new(csv_format))
            .with_file_extension("")
            .with_table_partition_cols(vec![("region".to_string(), DataType::Utf8)]);

        let schema = listing_opts
            .infer_schema(&ctx.state(), &table_url)
            .await
            .unwrap();
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_opts)
            .with_schema(schema);
        let table = ListingTable::try_new(config).unwrap();

        ctx.register_table("partitioned", Arc::new(table)).unwrap();
        let df = ctx
            .sql("SELECT city, region FROM partitioned ORDER BY region")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let cities: Vec<&str> = batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name("city")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..col.len()).map(move |i| col.value(i))
            })
            .collect();
        let regions: Vec<&str> = batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name("region")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..col.len()).map(move |i| col.value(i))
            })
            .collect();

        assert_eq!(cities, vec!["london", "new_york"]);
        assert_eq!(regions, vec!["eu", "us"]);
    }
}
