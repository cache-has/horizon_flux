// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Reference Horizon Flux sink plugin: writes incoming Arrow record batches
//! to a Parquet file.
//!
//! This is the **canonical example for plugin authors using the Rust SDK.**
//! It depends only on `arrow`, `parquet`, and `flux-plugin-sdk` — no flux
//! internal crates. The entire v1 wire protocol (handshake, configure,
//! stream, commit/abort, shutdown) is handled by `flux_plugin_sdk::run`;
//! this plugin only implements the [`Sink`] trait.

use std::fs::File;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::Deserialize;

use flux_plugin_sdk::{PluginInfo, Sink, SinkError, WriteStats, log, run};

const PLUGIN_NAME: &str = "parquet";
const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Deserialize)]
struct PluginConfig {
    path: PathBuf,
    #[serde(default)]
    compression: Option<String>,
}

fn parse_compression(name: Option<&str>) -> Result<Compression, String> {
    match name.unwrap_or("snappy").to_ascii_lowercase().as_str() {
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        other => Err(format!("unsupported compression '{other}'")),
    }
}

#[derive(Default)]
struct ParquetSink {
    writer: Option<ArrowWriter<File>>,
    rows: u64,
    bytes: u64,
    started: Option<Instant>,
}

impl Sink for ParquetSink {
    type Config = PluginConfig;

    fn configure(&mut self, config: PluginConfig, schema: &Schema) -> Result<(), SinkError> {
        if schema.fields().is_empty() {
            return Err(SinkError::InvalidConfig(
                "plugin requires a non-empty input schema".into(),
            ));
        }
        let compression =
            parse_compression(config.compression.as_deref()).map_err(SinkError::InvalidConfig)?;
        if let Some(parent) = config.path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                SinkError::InvalidConfig(format!(
                    "failed to create parent dir '{}': {e}",
                    parent.display()
                ))
            })?;
        }
        let file = File::create(&config.path).map_err(|e| {
            SinkError::InvalidConfig(format!("failed to create '{}': {e}", config.path.display()))
        })?;
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let writer = ArrowWriter::try_new(file, std::sync::Arc::new(schema.clone()), Some(props))
            .map_err(|e| {
            SinkError::InvalidConfig(format!("failed to open parquet writer: {e}"))
        })?;
        log::info(format!(
            "parquet plugin writing to {}",
            config.path.display()
        ));
        self.writer = Some(writer);
        self.started = Some(Instant::now());
        Ok(())
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SinkError> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| SinkError::Fatal("write_batch called before configure".into()))?;
        writer
            .write(batch)
            .map_err(|e| SinkError::WriteFailed(Box::new(e)))?;
        self.rows += batch.num_rows() as u64;
        // Approximate on-wire bytes from the batch's in-memory footprint —
        // good enough for the CommitAck stat, exactness is not promised.
        self.bytes += batch.get_array_memory_size() as u64;
        Ok(())
    }

    fn commit(&mut self) -> Result<WriteStats, SinkError> {
        if let Some(writer) = self.writer.take() {
            writer
                .close()
                .map_err(|e| SinkError::WriteFailed(Box::new(e)))?;
        }
        let duration = self
            .started
            .take()
            .map(|t| t.elapsed())
            .unwrap_or_else(|| Duration::from_millis(0));
        Ok(WriteStats {
            rows_written: self.rows,
            bytes_written: self.bytes,
            duration,
        })
    }

    fn abort(&mut self, _reason: &str) -> Result<(), SinkError> {
        // Drop the partial writer; the file is left as-is on disk. A more
        // sophisticated plugin could stage to a temp file and only rename on
        // commit.
        drop(self.writer.take());
        Ok(())
    }
}

fn main() -> ExitCode {
    run(
        PluginInfo {
            name: PLUGIN_NAME.into(),
            version: PLUGIN_VERSION.into(),
        },
        ParquetSink::default(),
    )
}
