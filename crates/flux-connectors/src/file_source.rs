// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! File-based source connector (CSV and Parquet).
//!
//! Uses DataFusion's [`ListingTable`] under the hood, which provides automatic
//! filter pushdown (Parquet row-group pruning) and projection pushdown (only
//! reading needed columns).

use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use flux_datafusion::provider::{ProviderError, SourceConnector};
use flux_engine::node::SourceConfig;
use tracing::debug;

use crate::config::{FileConfig, FileFormat};

/// Source connector for local CSV and Parquet files.
///
/// Supports:
/// - CSV with configurable delimiter, header, quote char
/// - Parquet with automatic schema discovery
/// - Glob patterns for reading multiple files (e.g. `data/*.csv`)
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

        // Resolve the path — convert relative paths to absolute using CWD.
        let path = if file_config.path.is_relative() {
            std::env::current_dir()
                .map_err(|e| format!("failed to get current directory: {e}"))?
                .join(&file_config.path)
        } else {
            file_config.path.clone()
        };

        let path_str = path
            .to_str()
            .ok_or_else(|| format!("path is not valid UTF-8: {}", path.display()))?;

        // Validate the path exists (or glob matches at least one file) before
        // handing off to DataFusion, which silently returns empty results for
        // missing files.
        let path_string = path_str.to_string();
        if path_string.contains('*') || path_string.contains('?') || path_string.contains('[') {
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

        debug!(path = %path.display(), format = ?file_config.format, "creating file source");

        // SourceConnector::create_table_provider is sync, but ListingTable
        // schema inference is async. Use block_in_place to bridge the gap
        // without panicking when called from within a tokio runtime.
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| "file source requires a tokio runtime")?;

        tokio::task::block_in_place(|| rt.block_on(create_listing_table(path_str, &file_config)))
    }
}

async fn create_listing_table(
    path_str: &str,
    file_config: &FileConfig,
) -> Result<Arc<dyn TableProvider>, ProviderError> {
    let ctx = SessionContext::new();
    let state = ctx.state();

    let table_url = ListingTableUrl::parse(path_str)
        .map_err(|e| format!("invalid file path '{}': {}", path_str, e))?;

    let listing_options = match file_config.format {
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

    // Infer schema from the file(s).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_source_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FileSource>();
    }
}
