// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! File-based sink connector (CSV and Parquet).
//!
//! Writes Arrow record batches to local files in CSV or Parquet format.
//! Supports configurable options for each format and overwrite/append modes.

use std::fs;
use std::io::{BufWriter, Write};
use std::time::Instant;

use arrow::csv::WriterBuilder as CsvWriterBuilder;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use flux_datafusion::provider::{PipelineSink, ProviderError, WriteOptions, WriteStats};
use flux_engine::node::SinkConfig;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tracing::debug;

use crate::config::{FileConfig, FileFormat, WriteMode};

/// Sink connector for local CSV and Parquet files.
///
/// Supports:
/// - CSV with configurable delimiter, header, quote char
/// - Parquet with configurable compression (snappy, zstd, gzip, none) and row group size
/// - Overwrite or append mode
/// - Automatic parent directory creation
pub struct FileSink;

impl FileSink {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FileSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PipelineSink for FileSink {
    async fn write(
        &self,
        config: &SinkConfig,
        data: Vec<RecordBatch>,
        _options: &WriteOptions,
    ) -> Result<WriteStats, ProviderError> {
        let start = Instant::now();

        let file_config: FileConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid file sink config: {e}"))?;

        if data.is_empty() {
            return Ok(WriteStats {
                rows_written: 0,
                bytes_written: 0,
                duration: start.elapsed(),
            });
        }

        // Resolve the path — convert relative paths to absolute using CWD.
        let path = if file_config.path.is_relative() {
            std::env::current_dir()
                .map_err(|e| format!("failed to get current directory: {e}"))?
                .join(&file_config.path)
        } else {
            file_config.path.clone()
        };

        // Create parent directories if they don't exist.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create directory '{}': {e}", parent.display()))?;
        }

        debug!(path = %path.display(), format = ?file_config.format, "writing file sink");

        let (rows_written, bytes_written) = match file_config.format {
            FileFormat::Csv => write_csv(&path, &data, &file_config)?,
            FileFormat::Parquet => write_parquet(&path, &data, &file_config)?,
        };

        Ok(WriteStats {
            rows_written,
            bytes_written,
            duration: start.elapsed(),
        })
    }

    fn validate_config(&self, config: &SinkConfig) -> Result<(), ProviderError> {
        let file_config: FileConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid file sink config: {e}"))?;

        // Path must not be empty.
        if file_config.path.as_os_str().is_empty() {
            return Err("file sink path must not be empty".into());
        }

        // Glob patterns are not valid for sink paths.
        let path_str = file_config.path.to_string_lossy();
        if path_str.contains('*') || path_str.contains('?') || path_str.contains('[') {
            return Err("file sink path must not contain glob patterns".into());
        }

        // Validate compression value if specified for Parquet.
        if let FileFormat::Parquet = file_config.format {
            if let Some(ref codec) = file_config.options.compression {
                parse_compression(codec)?;
            }
        }

        Ok(())
    }
}

/// Write record batches as CSV.
fn write_csv(
    path: &std::path::Path,
    data: &[RecordBatch],
    config: &FileConfig,
) -> Result<(u64, u64), ProviderError> {
    let append = matches!(config.options.write_mode, Some(WriteMode::Append)) && path.exists();

    let file = if append {
        fs::OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|e| format!("failed to open '{}' for append: {e}", path.display()))?
    } else {
        fs::File::create(path).map_err(|e| format!("failed to create '{}': {e}", path.display()))?
    };
    let mut buf = BufWriter::new(file);

    let has_header = config.options.has_header.unwrap_or(true);

    let mut rows_written: u64 = 0;
    for (i, batch) in data.iter().enumerate() {
        let mut builder = CsvWriterBuilder::new();

        if let Some(delim) = config.options.delimiter {
            builder = builder.with_delimiter(delim as u8);
        }
        if let Some(quote) = config.options.quote_char {
            builder = builder.with_quote(quote as u8);
        }

        // Write header only for the first batch (and not when appending).
        let write_header = has_header && i == 0 && !append;
        builder = builder.with_header(write_header);

        let mut writer = builder.build(&mut buf);
        writer
            .write(batch)
            .map_err(|e| format!("failed to write CSV batch: {e}"))?;

        rows_written += batch.num_rows() as u64;
    }

    buf.flush()
        .map_err(|e| format!("failed to flush CSV output: {e}"))?;

    let bytes_written = fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    Ok((rows_written, bytes_written))
}

/// Write record batches as Parquet.
fn write_parquet(
    path: &std::path::Path,
    data: &[RecordBatch],
    config: &FileConfig,
) -> Result<(u64, u64), ProviderError> {
    if matches!(config.options.write_mode, Some(WriteMode::Append)) && path.exists() {
        return Err(
            "parquet append mode is not supported; use overwrite or write to a new file".into(),
        );
    }

    let schema = data[0].schema();

    let compression = match &config.options.compression {
        Some(codec) => parse_compression(codec)?,
        None => Compression::SNAPPY,
    };

    let mut props_builder = WriterProperties::builder().set_compression(compression);

    if let Some(rg_size) = config.options.row_group_size {
        props_builder = props_builder.set_max_row_group_row_count(Some(rg_size));
    }

    let props = props_builder.build();

    let file = fs::File::create(path)
        .map_err(|e| format!("failed to create '{}': {e}", path.display()))?;

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| format!("failed to create Parquet writer: {e}"))?;

    let mut rows_written: u64 = 0;
    for batch in data {
        writer
            .write(batch)
            .map_err(|e| format!("failed to write Parquet batch: {e}"))?;
        rows_written += batch.num_rows() as u64;
    }

    let _metadata = writer
        .close()
        .map_err(|e| format!("failed to close Parquet writer: {e}"))?;

    // Use the Parquet footer's serialized size as bytes estimate.
    let bytes_written = fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    Ok((rows_written, bytes_written))
}

/// Parse a compression codec string into a Parquet `Compression` variant.
fn parse_compression(codec: &str) -> Result<Compression, ProviderError> {
    match codec.to_lowercase().as_str() {
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        "lz4" => Ok(Compression::LZ4),
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        other => Err(format!(
            "unsupported parquet compression codec '{other}'; expected one of: snappy, gzip, zstd, lz4, none"
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_sink_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FileSink>();
    }
}
