// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Rust SDK for writing Horizon Flux sink plugins.
//!
//! A complete plugin is one struct that implements [`Sink`] plus a `main`
//! that calls [`run`]. The SDK handles the full wire protocol — handshake,
//! configuration, batch streaming, commit/abort, and shutdown — so plugin
//! authors only ever deal with `arrow::record_batch::RecordBatch` and their
//! own typed config.
//!
//! ```no_run
//! use flux_plugin_sdk::{run, PluginInfo, Sink, SinkError, WriteStats};
//! use serde::Deserialize;
//! use arrow::{datatypes::Schema, record_batch::RecordBatch};
//!
//! #[derive(Deserialize)]
//! struct MyConfig { output_path: String }
//!
//! struct MySink { config: Option<MyConfig>, rows: u64 }
//!
//! impl Sink for MySink {
//!     type Config = MyConfig;
//!     fn configure(&mut self, c: MyConfig, _s: &Schema) -> Result<(), SinkError> {
//!         self.config = Some(c);
//!         Ok(())
//!     }
//!     fn write_batch(&mut self, b: &RecordBatch) -> Result<(), SinkError> {
//!         self.rows += b.num_rows() as u64;
//!         Ok(())
//!     }
//!     fn commit(&mut self) -> Result<WriteStats, SinkError> {
//!         Ok(WriteStats { rows_written: self.rows, bytes_written: 0,
//!                         duration: std::time::Duration::default() })
//!     }
//! }
//!
//! fn main() -> std::process::ExitCode {
//!     run(
//!         PluginInfo { name: "my-sink".into(), version: "0.1.0".into() },
//!         MySink { config: None, rows: 0 },
//!     )
//! }
//! ```
//!
//! See `docs/plugins/protocol-v1.md` for the wire-level reference.

use std::io::{Read, Write};
use std::process::ExitCode;
use std::time::Duration;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use thiserror::Error;

use flux_plugin_protocol::arrow_ipc::{ArrowIpcError, decode_record_batch, decode_schema_b64};
use flux_plugin_protocol::{
    Abort, BatchAck, CommitAck, ConfigureAck, ConfigureSink, ControlError, ErrorMsg, Frame, Hello,
    HelloAck, MessageKind, PROTOCOL_VERSION, read_frame, read_json_frame, write_frame,
    write_json_frame,
};

pub mod log;

/// Identifying metadata returned by the plugin during the handshake.
#[derive(Debug, Clone)]
pub struct PluginInfo {
    pub name: String,
    pub version: String,
}

/// Statistics returned by [`Sink::commit`].
#[derive(Debug, Clone, Default)]
pub struct WriteStats {
    pub rows_written: u64,
    pub bytes_written: u64,
    pub duration: Duration,
}

/// Errors a [`Sink`] can return. They become protocol-level `Error` or
/// `ConfigureAck { accepted: false }` messages depending on the lifecycle
/// phase.
#[derive(Debug, Error)]
pub enum SinkError {
    #[error("schema mismatch: expected {expected}, got {got}")]
    SchemaMismatch { expected: String, got: String },

    #[error("invalid config: {0}")]
    InvalidConfig(String),

    #[error("write failed: {0}")]
    WriteFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("fatal: {0}")]
    Fatal(String),
}

/// The single trait a sink plugin implements.
///
/// Lifecycle: [`configure`](Self::configure) is called once, then
/// [`write_batch`](Self::write_batch) zero or more times, then exactly one of
/// [`commit`](Self::commit) (success) or [`abort`](Self::abort) (failure).
pub trait Sink: Send {
    type Config: for<'de> serde::Deserialize<'de>;

    /// Called once after handshake. Validate the config and the upstream
    /// schema; return `Err` to reject this pipeline node cleanly.
    fn configure(&mut self, config: Self::Config, schema: &Schema) -> Result<(), SinkError>;

    /// Called for each `RecordBatch` produced by the upstream.
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SinkError>;

    /// Called when the upstream is exhausted. Returns final stats.
    fn commit(&mut self) -> Result<WriteStats, SinkError>;

    /// Called if the host aborts the pipeline. Default is a no-op.
    fn abort(&mut self, reason: &str) -> Result<(), SinkError> {
        let _ = reason;
        Ok(())
    }
}

/// Errors that escape the SDK's I/O loop. These are SDK-internal — sink
/// authors do not normally see them. They map to a non-zero exit code.
#[derive(Debug, Error)]
pub enum RunError {
    #[error(transparent)]
    Control(#[from] ControlError),

    #[error("frame error: {0}")]
    Frame(#[from] flux_plugin_protocol::FrameError),

    #[error("arrow ipc: {0}")]
    ArrowIpc(#[from] ArrowIpcError),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("unexpected frame in stream phase: {0:?}")]
    UnexpectedFrame(MessageKind),

    #[error("host protocol version {host} is not supported by SDK ({sdk})")]
    ProtocolMismatch { host: u32, sdk: u32 },
}

/// Plugin entry point. Reads the protocol from stdin, writes responses to
/// stdout. Call this from `main` and return its result as the process exit
/// code. Any unexpected I/O failure is logged to stderr and surfaces as a
/// non-zero exit.
pub fn run<S: Sink>(info: PluginInfo, sink: S) -> ExitCode {
    // Use the unlocked stdin/stdout — each frame read/write acquires the
    // line-level lock automatically. This keeps stderr (used by [`log`])
    // independent and avoids deadlocking single-threaded plugins.
    let mut stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    match run_io(info, sink, &mut stdin, &mut stdout) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("flux-plugin-sdk: fatal: {e}");
            ExitCode::FAILURE
        }
    }
}

/// I/O-generic version of [`run`]. Public so plugin authors can write
/// integration tests against in-memory pipes without spawning a process.
///
/// # Tutorial example
///
/// This is the in-process round-trip pattern documented in
/// `docs/plugins/testing.md` and used as the canonical author-facing
/// example. It builds the byte stream a host would send (`Hello →
/// ConfigureSink → RecordBatch → Commit → Shutdown`), runs an arbitrary
/// `Sink` against it via [`run_io`], and asserts the SDK wrote the
/// expected `CommitAck` back. It is exercised by `cargo test --doc` in
/// CI so the tutorial code cannot silently rot.
///
/// ```
/// use std::io::Cursor;
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use arrow::array::Int32Array;
/// use arrow::datatypes::{DataType, Field, Schema};
/// use arrow::record_batch::RecordBatch;
///
/// use flux_plugin_protocol::arrow_ipc::{encode_record_batch, encode_schema_b64};
/// use flux_plugin_protocol::{
///     CommitAck, ConfigureAck, ConfigureSink, Hello, HelloAck, MessageKind,
///     PROTOCOL_VERSION, read_json_frame, write_frame, write_json_frame,
/// };
/// use flux_plugin_sdk::{PluginInfo, Sink, SinkError, WriteStats, run_io};
///
/// // The minimal sink: count rows, write nothing.
/// #[derive(Default)]
/// struct CountingSink { rows: u64 }
///
/// impl Sink for CountingSink {
///     type Config = serde_json::Value;
///     fn configure(&mut self, _: serde_json::Value, _: &Schema) -> Result<(), SinkError> {
///         Ok(())
///     }
///     fn write_batch(&mut self, b: &RecordBatch) -> Result<(), SinkError> {
///         self.rows += b.num_rows() as u64;
///         Ok(())
///     }
///     fn commit(&mut self) -> Result<WriteStats, SinkError> {
///         Ok(WriteStats { rows_written: self.rows, bytes_written: 0,
///                         duration: Duration::from_millis(1) })
///     }
/// }
///
/// // Build the host's input stream.
/// let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
/// let batch = RecordBatch::try_new(
///     schema.clone(),
///     vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4]))],
/// ).unwrap();
///
/// let mut input: Vec<u8> = Vec::new();
/// write_json_frame(&mut input, MessageKind::Hello, &Hello {
///     protocol: PROTOCOL_VERSION, flux_version: "doctest".into(),
/// }).unwrap();
/// write_json_frame(&mut input, MessageKind::ConfigureSink, &ConfigureSink {
///     sink_type: "counting".into(),
///     config: serde_json::json!({}),
///     input_schema_ipc_b64: encode_schema_b64(&schema).unwrap(),
/// }).unwrap();
/// write_frame(&mut input, MessageKind::RecordBatch,
///             &encode_record_batch(&batch).unwrap()).unwrap();
/// write_json_frame(&mut input, MessageKind::Commit, &serde_json::json!({})).unwrap();
/// write_json_frame(&mut input, MessageKind::Shutdown, &serde_json::json!({})).unwrap();
///
/// // Drive the SDK against the in-memory streams.
/// let mut output: Vec<u8> = Vec::new();
/// run_io(
///     PluginInfo { name: "counting".into(), version: "0.1.0".into() },
///     CountingSink::default(),
///     &mut Cursor::new(input),
///     &mut output,
/// ).unwrap();
///
/// // Decode the SDK's responses and assert the lifecycle ran end-to-end.
/// let mut out = Cursor::new(output);
/// let _: HelloAck = read_json_frame(&mut out, MessageKind::HelloAck).unwrap();
/// let cfg: ConfigureAck = read_json_frame(&mut out, MessageKind::ConfigureAck).unwrap();
/// assert!(cfg.accepted);
/// // Skip the BatchAck frame.
/// let _ = flux_plugin_protocol::read_frame(&mut out).unwrap();
/// let commit: CommitAck = read_json_frame(&mut out, MessageKind::CommitAck).unwrap();
/// assert_eq!(commit.rows, 4);
/// ```
pub fn run_io<S, R, W>(
    info: PluginInfo,
    mut sink: S,
    reader: &mut R,
    writer: &mut W,
) -> Result<(), RunError>
where
    S: Sink,
    R: Read,
    W: Write,
{
    // ── 1. Handshake ────────────────────────────────────────────────────
    let hello: Hello = read_json_frame(reader, MessageKind::Hello)?;
    if hello.protocol != PROTOCOL_VERSION {
        // Tell the host we don't speak this version, then bail.
        let _ = send_error(
            writer,
            &format!(
                "plugin protocol mismatch: host speaks {}, SDK speaks {}",
                hello.protocol, PROTOCOL_VERSION
            ),
        );
        return Err(RunError::ProtocolMismatch {
            host: hello.protocol,
            sdk: PROTOCOL_VERSION,
        });
    }
    let ack = HelloAck {
        protocol: PROTOCOL_VERSION,
        plugin_name: info.name.clone(),
        plugin_version: info.version.clone(),
        capabilities: Default::default(),
    };
    write_json_frame(writer, MessageKind::HelloAck, &ack)?;
    writer.flush()?;

    // ── 2. Configure ────────────────────────────────────────────────────
    let cfg: ConfigureSink = read_json_frame(reader, MessageKind::ConfigureSink)?;
    let schema = decode_schema_b64(&cfg.input_schema_ipc_b64)?;
    let parsed: S::Config = match serde_json::from_value(cfg.config) {
        Ok(c) => c,
        Err(e) => {
            write_json_frame(
                writer,
                MessageKind::ConfigureAck,
                &ConfigureAck {
                    accepted: false,
                    reason: Some(format!("invalid config: {e}")),
                },
            )?;
            writer.flush()?;
            return Ok(());
        }
    };
    if let Err(e) = sink.configure(parsed, schema.as_ref()) {
        write_json_frame(
            writer,
            MessageKind::ConfigureAck,
            &ConfigureAck {
                accepted: false,
                reason: Some(e.to_string()),
            },
        )?;
        writer.flush()?;
        return Ok(());
    }
    write_json_frame(
        writer,
        MessageKind::ConfigureAck,
        &ConfigureAck {
            accepted: true,
            reason: None,
        },
    )?;
    writer.flush()?;

    // ── 3. Stream / commit / abort loop ─────────────────────────────────
    loop {
        let frame: Frame = read_frame(reader)?;
        match frame.kind {
            MessageKind::RecordBatch => {
                let batch = decode_record_batch(&frame.payload)?;
                let rows = batch.num_rows() as u64;
                match sink.write_batch(&batch) {
                    Ok(()) => {
                        write_json_frame(
                            writer,
                            MessageKind::BatchAck,
                            &BatchAck {
                                rows_accepted: rows,
                                warning: None,
                            },
                        )?;
                        writer.flush()?;
                    }
                    Err(e) => {
                        send_error(writer, &e.to_string())?;
                        writer.flush()?;
                        return Ok(());
                    }
                }
            }
            MessageKind::Commit => {
                match sink.commit() {
                    Ok(stats) => {
                        let ack = CommitAck {
                            rows: stats.rows_written,
                            bytes: stats.bytes_written,
                            duration_ms: stats.duration.as_millis() as u64,
                        };
                        write_json_frame(writer, MessageKind::CommitAck, &ack)?;
                    }
                    Err(e) => send_error(writer, &e.to_string())?,
                }
                writer.flush()?;
                // Wait for an explicit Shutdown (or pipe close) before returning.
                return drain_until_shutdown(reader);
            }
            MessageKind::Abort => {
                let abort: Abort = serde_json::from_slice(&frame.payload)?;
                let _ = sink.abort(&abort.reason);
                write_frame(writer, MessageKind::AbortAck, &[])?;
                writer.flush()?;
                return Ok(());
            }
            MessageKind::Shutdown => return Ok(()),
            other => return Err(RunError::UnexpectedFrame(other)),
        }
    }
}

fn send_error<W: Write>(writer: &mut W, message: &str) -> Result<(), RunError> {
    write_json_frame(
        writer,
        MessageKind::Error,
        &ErrorMsg {
            message: message.to_string(),
            details: None,
        },
    )?;
    Ok(())
}

/// After a successful commit, hold the pipe open until the host sends
/// `Shutdown` (or the pipe closes). Anything else in this state is ignored —
/// the lifecycle is over.
fn drain_until_shutdown<R: Read>(reader: &mut R) -> Result<(), RunError> {
    loop {
        match read_frame(reader) {
            Ok(f) if f.kind == MessageKind::Shutdown => return Ok(()),
            Ok(_) => continue,
            Err(flux_plugin_protocol::FrameError::UnexpectedEof { .. })
            | Err(flux_plugin_protocol::FrameError::Io(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use flux_plugin_protocol::arrow_ipc::{encode_record_batch, encode_schema_b64};
    use serde::Deserialize;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[derive(Default)]
    struct CapturingSink {
        rows: u64,
        committed: bool,
    }

    #[derive(Deserialize)]
    struct Cfg {
        path: String,
    }

    impl Sink for CapturingSink {
        type Config = Cfg;
        fn configure(&mut self, c: Cfg, _: &Schema) -> Result<(), SinkError> {
            assert_eq!(c.path, "/tmp/x");
            Ok(())
        }
        fn write_batch(&mut self, b: &RecordBatch) -> Result<(), SinkError> {
            self.rows += b.num_rows() as u64;
            Ok(())
        }
        fn commit(&mut self) -> Result<WriteStats, SinkError> {
            self.committed = true;
            Ok(WriteStats {
                rows_written: self.rows,
                bytes_written: 42,
                duration: Duration::from_millis(7),
            })
        }
    }

    /// Build the input stream a host would send for a normal happy-path
    /// lifecycle: Hello → ConfigureSink → RecordBatch → Commit → Shutdown.
    fn host_input() -> Vec<u8> {
        let mut buf = Vec::new();
        write_json_frame(
            &mut buf,
            MessageKind::Hello,
            &Hello {
                protocol: PROTOCOL_VERSION,
                flux_version: "test".into(),
            },
        )
        .unwrap();
        write_json_frame(
            &mut buf,
            MessageKind::ConfigureSink,
            &ConfigureSink {
                sink_type: "capturing".into(),
                config: serde_json::json!({ "path": "/tmp/x" }),
                input_schema_ipc_b64: encode_schema_b64(&schema()).unwrap(),
            },
        )
        .unwrap();
        let bytes = encode_record_batch(&batch()).unwrap();
        write_frame(&mut buf, MessageKind::RecordBatch, &bytes).unwrap();
        write_json_frame(&mut buf, MessageKind::Commit, &serde_json::json!({})).unwrap();
        write_json_frame(&mut buf, MessageKind::Shutdown, &serde_json::json!({})).unwrap();
        buf
    }

    #[test]
    fn happy_path_round_trip() {
        let input = host_input();
        let mut reader = Cursor::new(input);
        let mut writer: Vec<u8> = Vec::new();
        run_io(
            PluginInfo {
                name: "capturing".into(),
                version: "0.1.0".into(),
            },
            CapturingSink::default(),
            &mut reader,
            &mut writer,
        )
        .unwrap();

        // Replay what the SDK wrote and check it matches the protocol.
        let mut out = Cursor::new(writer);
        let hello_ack: HelloAck = read_json_frame(&mut out, MessageKind::HelloAck).unwrap();
        assert_eq!(hello_ack.protocol, PROTOCOL_VERSION);
        assert_eq!(hello_ack.plugin_name, "capturing");

        let cfg_ack: ConfigureAck = read_json_frame(&mut out, MessageKind::ConfigureAck).unwrap();
        assert!(cfg_ack.accepted);

        let batch_ack: BatchAck = read_json_frame(&mut out, MessageKind::BatchAck).unwrap();
        assert_eq!(batch_ack.rows_accepted, 3);

        let commit_ack: CommitAck = read_json_frame(&mut out, MessageKind::CommitAck).unwrap();
        assert_eq!(commit_ack.rows, 3);
        assert_eq!(commit_ack.bytes, 42);
        assert_eq!(commit_ack.duration_ms, 7);
    }

    struct RejectingSink;
    impl Sink for RejectingSink {
        type Config = serde_json::Value;
        fn configure(&mut self, _: serde_json::Value, _: &Schema) -> Result<(), SinkError> {
            Err(SinkError::InvalidConfig("nope".into()))
        }
        fn write_batch(&mut self, _: &RecordBatch) -> Result<(), SinkError> {
            unreachable!()
        }
        fn commit(&mut self) -> Result<WriteStats, SinkError> {
            unreachable!()
        }
    }

    #[test]
    fn configure_rejection_is_reported_cleanly() {
        let mut buf = Vec::new();
        write_json_frame(
            &mut buf,
            MessageKind::Hello,
            &Hello {
                protocol: PROTOCOL_VERSION,
                flux_version: "test".into(),
            },
        )
        .unwrap();
        write_json_frame(
            &mut buf,
            MessageKind::ConfigureSink,
            &ConfigureSink {
                sink_type: "rejecting".into(),
                config: serde_json::json!({}),
                input_schema_ipc_b64: encode_schema_b64(&schema()).unwrap(),
            },
        )
        .unwrap();
        let mut writer: Vec<u8> = Vec::new();
        run_io(
            PluginInfo {
                name: "r".into(),
                version: "0".into(),
            },
            RejectingSink,
            &mut Cursor::new(buf),
            &mut writer,
        )
        .unwrap();

        let mut out = Cursor::new(writer);
        let _: HelloAck = read_json_frame(&mut out, MessageKind::HelloAck).unwrap();
        let cfg_ack: ConfigureAck = read_json_frame(&mut out, MessageKind::ConfigureAck).unwrap();
        assert!(!cfg_ack.accepted);
        assert!(cfg_ack.reason.unwrap().contains("nope"));
    }

    struct AbortingSink;
    impl Sink for AbortingSink {
        type Config = serde_json::Value;
        fn configure(&mut self, _: serde_json::Value, _: &Schema) -> Result<(), SinkError> {
            Ok(())
        }
        fn write_batch(&mut self, _: &RecordBatch) -> Result<(), SinkError> {
            Ok(())
        }
        fn commit(&mut self) -> Result<WriteStats, SinkError> {
            unreachable!()
        }
    }

    #[test]
    fn host_abort_yields_abort_ack() {
        let mut buf = Vec::new();
        write_json_frame(
            &mut buf,
            MessageKind::Hello,
            &Hello {
                protocol: PROTOCOL_VERSION,
                flux_version: "test".into(),
            },
        )
        .unwrap();
        write_json_frame(
            &mut buf,
            MessageKind::ConfigureSink,
            &ConfigureSink {
                sink_type: "abort".into(),
                config: serde_json::json!({}),
                input_schema_ipc_b64: encode_schema_b64(&schema()).unwrap(),
            },
        )
        .unwrap();
        write_json_frame(
            &mut buf,
            MessageKind::Abort,
            &Abort {
                reason: "user cancelled".into(),
            },
        )
        .unwrap();

        let mut writer: Vec<u8> = Vec::new();
        run_io(
            PluginInfo {
                name: "a".into(),
                version: "0".into(),
            },
            AbortingSink,
            &mut Cursor::new(buf),
            &mut writer,
        )
        .unwrap();

        let mut out = Cursor::new(writer);
        let _: HelloAck = read_json_frame(&mut out, MessageKind::HelloAck).unwrap();
        let _: ConfigureAck = read_json_frame(&mut out, MessageKind::ConfigureAck).unwrap();
        // AbortAck has empty payload, so use the raw frame reader.
        let frame = read_frame(&mut out).unwrap();
        assert_eq!(frame.kind, MessageKind::AbortAck);
        assert!(frame.payload.is_empty());
    }
}
