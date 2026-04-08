// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! JSON control message types for the plugin protocol.
//!
//! Each struct here corresponds to one section of `docs/plugins/protocol-v1.md`
//! §3. All payloads are UTF-8 JSON; unknown fields are ignored on the read
//! side so the protocol can grow additively within v1.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frame::{Frame, FrameError, MessageKind, read_frame, write_frame};

/// `Hello` (host → plugin) — §3.1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub protocol: u32,
    pub flux_version: String,
}

/// `HelloAck` (plugin → host) — §3.2.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloAck {
    pub protocol: u32,
    pub plugin_name: String,
    pub plugin_version: String,
    #[serde(default)]
    pub capabilities: BTreeMap<String, bool>,
}

/// `ConfigureSink` (host → plugin) — §3.3. The upstream Arrow schema is
/// delivered as a base64-encoded Arrow IPC `Schema` message inside this
/// payload so it can travel without an extra frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigureSink {
    pub sink_type: String,
    pub config: Value,
    pub input_schema_ipc_b64: String,
}

/// `ConfigureAck` (plugin → host) — §3.4.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigureAck {
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// `BatchAck` (plugin → host) — §3.5.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchAck {
    pub rows_accepted: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// `CommitAck` (plugin → host) — §3.6.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitAck {
    pub rows: u64,
    pub bytes: u64,
    pub duration_ms: u64,
}

/// `Abort` (host → plugin) — §3.7.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Abort {
    pub reason: String,
}

/// `Log` (plugin → host) — §3.8.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// `Error` (plugin → host) — §3.9. Receipt fails the pipeline node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMsg {
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// Errors specific to (de)serializing control messages on top of framing.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error(transparent)]
    Frame(#[from] FrameError),

    #[error("control payload was not valid JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("expected message kind {expected:?} but got {actual:?}")]
    UnexpectedKind {
        expected: MessageKind,
        actual: MessageKind,
    },
}

/// Serialize `value` as JSON and write it as a frame of `kind`.
pub fn write_json_frame<W: std::io::Write, T: Serialize>(
    w: &mut W,
    kind: MessageKind,
    value: &T,
) -> Result<(), ControlError> {
    let bytes = serde_json::to_vec(value)?;
    write_frame(w, kind, &bytes)?;
    Ok(())
}

/// Read a frame and deserialize its JSON payload as `T`. Errors if the frame
/// kind does not match `expected`.
pub fn read_json_frame<R: std::io::Read, T: for<'de> Deserialize<'de>>(
    r: &mut R,
    expected: MessageKind,
) -> Result<T, ControlError> {
    let frame: Frame = read_frame(r)?;
    if frame.kind != expected {
        return Err(ControlError::UnexpectedKind {
            expected,
            actual: frame.kind,
        });
    }
    Ok(serde_json::from_slice(&frame.payload)?)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn hello_round_trip() {
        let h = Hello {
            protocol: 1,
            flux_version: "0.5.0".into(),
        };
        let mut buf = Vec::new();
        write_json_frame(&mut buf, MessageKind::Hello, &h).unwrap();
        let mut cur = Cursor::new(buf);
        let got: Hello = read_json_frame(&mut cur, MessageKind::Hello).unwrap();
        assert_eq!(got.protocol, 1);
        assert_eq!(got.flux_version, "0.5.0");
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let json = br#"{"protocol":1,"flux_version":"0.5.0","extra":"ok"}"#;
        let mut buf = Vec::new();
        write_frame(&mut buf, MessageKind::Hello, json).unwrap();
        let mut cur = Cursor::new(buf);
        let _h: Hello = read_json_frame(&mut cur, MessageKind::Hello).unwrap();
    }

    #[test]
    fn kind_mismatch_is_an_error() {
        let mut buf = Vec::new();
        write_json_frame(
            &mut buf,
            MessageKind::Log,
            &Log {
                level: LogLevel::Info,
                message: "hi".into(),
            },
        )
        .unwrap();
        let mut cur = Cursor::new(buf);
        let err = read_json_frame::<_, Hello>(&mut cur, MessageKind::Hello).unwrap_err();
        assert!(matches!(err, ControlError::UnexpectedKind { .. }));
    }

    #[test]
    fn configure_ack_omits_none_reason() {
        let ack = ConfigureAck {
            accepted: true,
            reason: None,
        };
        let s = serde_json::to_string(&ack).unwrap();
        assert_eq!(s, r#"{"accepted":true}"#);
    }
}
