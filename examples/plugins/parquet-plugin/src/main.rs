// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Reference Horizon Flux sink plugin: writes incoming Arrow record batches
//! to a Parquet file.
//!
//! This binary speaks the v1 plugin protocol on stdin/stdout. It exists to
//! validate the plugin protocol end-to-end with first-party code, and to
//! serve as a template for third-party plugin authors. It depends only on
//! `arrow`, `parquet`, and `flux-plugin-host` (for protocol primitives) — no
//! transitive dependency on `flux-connectors` or any other internal crate.

use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::Deserialize;

use flux_plugin_host::arrow_ipc::{decode_record_batch, decode_schema_b64};
use flux_plugin_host::protocol::control::{
    BatchAck, CommitAck, ConfigureAck, ConfigureSink, ErrorMsg, Hello, HelloAck, Log, LogLevel,
};
use flux_plugin_host::protocol::{MessageKind, read_frame, write_frame};

const PROTOCOL_VERSION: u32 = 1;
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

fn write_json<W: Write, V: serde::Serialize>(w: &mut W, kind: MessageKind, v: &V) -> io::Result<()> {
    let bytes = serde_json::to_vec(v).map_err(io::Error::other)?;
    write_frame(w, kind, &bytes).map_err(io::Error::other)?;
    w.flush()
}

fn send_log<W: Write>(w: &mut W, level: LogLevel, message: impl Into<String>) {
    let _ = write_json(w, MessageKind::Log, &Log { level, message: message.into() });
}

fn send_error<W: Write>(w: &mut W, message: impl Into<String>) {
    let _ = write_json(
        w,
        MessageKind::Error,
        &ErrorMsg { message: message.into(), details: None },
    );
}

/// Open the writer and verify the incoming schema is non-empty. Returns the
/// active writer plus the resolved schema.
fn open_writer(
    config: &PluginConfig,
    schema: Arc<Schema>,
) -> Result<ArrowWriter<File>, String> {
    if schema.fields().is_empty() {
        return Err("plugin requires a non-empty input schema".into());
    }
    let compression = parse_compression(config.compression.as_deref())?;
    if let Some(parent) = config.path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create parent dir '{}': {e}", parent.display()))?;
    }
    let file = File::create(&config.path)
        .map_err(|e| format!("failed to create '{}': {e}", config.path.display()))?;
    let props = WriterProperties::builder().set_compression(compression).build();
    ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| format!("failed to open parquet writer: {e}"))
}

fn run() -> Result<(), String> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut r = stdin.lock();
    let mut w = stdout.lock();

    // 1. Handshake.
    let frame = read_frame(&mut r).map_err(|e| format!("read hello: {e}"))?;
    if frame.kind != MessageKind::Hello {
        return Err(format!("expected Hello, got {:?}", frame.kind));
    }
    let _hello: Hello = serde_json::from_slice(&frame.payload)
        .map_err(|e| format!("decode hello: {e}"))?;
    write_json(
        &mut w,
        MessageKind::HelloAck,
        &HelloAck {
            protocol: PROTOCOL_VERSION,
            plugin_name: PLUGIN_NAME.into(),
            plugin_version: PLUGIN_VERSION.into(),
            capabilities: Default::default(),
        },
    )
    .map_err(|e| format!("write hello-ack: {e}"))?;

    // 2. Configure.
    let frame = read_frame(&mut r).map_err(|e| format!("read configure: {e}"))?;
    if frame.kind != MessageKind::ConfigureSink {
        return Err(format!("expected ConfigureSink, got {:?}", frame.kind));
    }
    let cfg_msg: ConfigureSink = serde_json::from_slice(&frame.payload)
        .map_err(|e| format!("decode configure: {e}"))?;
    let parsed: Result<PluginConfig, _> = serde_json::from_value(cfg_msg.config.clone());
    let schema_decoded = decode_schema_b64(&cfg_msg.input_schema_ipc_b64);

    let writer = match (parsed, schema_decoded) {
        (Ok(config), Ok(schema)) => match open_writer(&config, schema) {
            Ok(writer) => {
                write_json(
                    &mut w,
                    MessageKind::ConfigureAck,
                    &ConfigureAck { accepted: true, reason: None },
                )
                .map_err(|e| format!("write configure-ack: {e}"))?;
                send_log(
                    &mut w,
                    LogLevel::Info,
                    format!("parquet plugin writing to {}", config.path.display()),
                );
                writer
            }
            Err(reason) => {
                let _ = write_json(
                    &mut w,
                    MessageKind::ConfigureAck,
                    &ConfigureAck { accepted: false, reason: Some(reason.clone()) },
                );
                return Err(reason);
            }
        },
        (Err(e), _) => {
            let reason = format!("invalid plugin config: {e}");
            let _ = write_json(
                &mut w,
                MessageKind::ConfigureAck,
                &ConfigureAck { accepted: false, reason: Some(reason.clone()) },
            );
            return Err(reason);
        }
        (_, Err(e)) => {
            let reason = format!("invalid input schema: {e}");
            let _ = write_json(
                &mut w,
                MessageKind::ConfigureAck,
                &ConfigureAck { accepted: false, reason: Some(reason.clone()) },
            );
            return Err(reason);
        }
    };

    // 3. Stream → Commit/Abort → Shutdown.
    let started = Instant::now();
    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut writer_slot = Some(writer);
    loop {
        let frame = read_frame(&mut r).map_err(|e| format!("read frame: {e}"))?;
        match frame.kind {
            MessageKind::RecordBatch => {
                let writer = writer_slot
                    .as_mut()
                    .ok_or_else(|| "RecordBatch after commit".to_string())?;
                total_bytes += frame.payload.len() as u64;
                let batch: RecordBatch = decode_record_batch(&frame.payload)
                    .map_err(|e| format!("decode batch: {e}"))?;
                let rows = batch.num_rows() as u64;
                if let Err(e) = writer.write(&batch) {
                    return Err(format!("parquet write failed: {e}"));
                }
                total_rows += rows;
                write_json(
                    &mut w,
                    MessageKind::BatchAck,
                    &BatchAck { rows_accepted: rows, warning: None },
                )
                .map_err(|e| format!("write batch-ack: {e}"))?;
            }
            MessageKind::Commit => {
                if let Some(writer) = writer_slot.take() {
                    writer
                        .close()
                        .map_err(|e| format!("parquet close failed: {e}"))?;
                }
                let duration_ms = started.elapsed().as_millis() as u64;
                write_json(
                    &mut w,
                    MessageKind::CommitAck,
                    &CommitAck { rows: total_rows, bytes: total_bytes, duration_ms },
                )
                .map_err(|e| format!("write commit-ack: {e}"))?;
            }
            MessageKind::Abort => {
                // Best-effort: drop the partial writer (truncates on close).
                drop(writer_slot.take());
                let _ = write_json(&mut w, MessageKind::AbortAck, &serde_json::json!({}));
                return Ok(());
            }
            MessageKind::Shutdown => return Ok(()),
            other => return Err(format!("unexpected frame {other:?}")),
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Best-effort error report — host may already have closed the pipe.
            let stdout = io::stdout();
            let mut w = stdout.lock();
            send_error(&mut w, e.clone());
            eprintln!("flux-parquet-plugin: {e}");
            ExitCode::FAILURE
        }
    }
}
