// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Sink lifecycle state machine on top of a [`Transport`].
//!
//! Implements the host side of `docs/plugins/protocol-v1.md` §5: handshake →
//! configure → stream → commit/abort → shutdown. Generic over the transport
//! so unit tests can drive it with an in-memory mock and the production path
//! drives it with a real child process.

use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use serde_json::Value;
use thiserror::Error;
use tracing::warn;

use crate::arrow_ipc::{ArrowIpcError, encode_record_batch, encode_schema_b64};
use crate::protocol::control::{
    Abort, BatchAck, ConfigureAck, ConfigureSink, ControlError, DeclareResource, ErrorMsg, Hello,
    HelloAck,
};
use crate::protocol::{Frame, MessageKind};
use crate::transport::{Transport, TransportError};

/// Default timeouts (see protocol §5.1).
pub mod timeouts {
    use std::time::Duration;
    pub const HANDSHAKE: Duration = Duration::from_secs(5);
    pub const CONFIGURE_ACK: Duration = Duration::from_secs(30);
    pub const BATCH_ACK: Duration = Duration::from_secs(60);
    pub const COMMIT_ACK: Duration = Duration::from_secs(300);
    pub const ABORT_ACK: Duration = Duration::from_secs(5);
    pub const SHUTDOWN: Duration = Duration::from_secs(5);
}

/// Errors surfaced by the lifecycle state machine.
#[derive(Debug, Error)]
pub enum SessionError {
    #[error(transparent)]
    Transport(#[from] TransportError),

    #[error("control payload error: {0}")]
    Control(#[from] ControlError),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("arrow ipc error: {0}")]
    ArrowIpc(#[from] ArrowIpcError),

    #[error("plugin protocol mismatch: plugin speaks {plugin}, host speaks {host}")]
    ProtocolMismatch { host: u32, plugin: u32 },

    #[error("plugin rejected configuration: {reason}")]
    ConfigureRejected { reason: String },

    #[error("plugin reported error: {message}")]
    PluginError { message: String },

    #[error("unexpected frame from plugin: kind={kind:?} during {phase}")]
    UnexpectedFrame {
        kind: MessageKind,
        phase: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Spawned,
    Handshaked,
    Configured,
    Committed,
    Aborted,
    Closed,
}

/// Host-side sink session. Owns a [`Transport`] and steps through the
/// protocol lifecycle. Drop-without-shutdown closes the transport implicitly,
/// which (for the child-process transport) kills the plugin.
pub struct PluginSession<T: Transport> {
    transport: T,
    state: State,
    host_protocol: u32,
    flux_version: String,
    plugin_info: Option<HelloAck>,
    /// Resource fingerprint declared by the plugin via `DeclareResource`
    /// (kind 0x15). `None` if the plugin does not participate in lineage.
    declared_resource: Option<String>,
}

impl<T: Transport> PluginSession<T> {
    pub fn new(transport: T, host_protocol: u32, flux_version: impl Into<String>) -> Self {
        Self {
            transport,
            state: State::Spawned,
            host_protocol,
            flux_version: flux_version.into(),
            plugin_info: None,
            declared_resource: None,
        }
    }

    pub fn plugin_info(&self) -> Option<&HelloAck> {
        self.plugin_info.as_ref()
    }

    /// Returns the resource fingerprint declared by the plugin via
    /// `DeclareResource`, or `None` if the plugin does not participate
    /// in lineage tracking.
    pub fn declared_resource(&self) -> Option<&str> {
        self.declared_resource.as_deref()
    }

    /// Step 1: send `Hello`, expect `HelloAck`, validate protocol version.
    pub fn handshake(&mut self) -> Result<&HelloAck, SessionError> {
        self.handshake_with_timeout(timeouts::HANDSHAKE)
    }

    /// Variant of [`Self::handshake`] with an explicit recv timeout — used by
    /// tests that need to fail fast on a hung plugin.
    pub fn handshake_with_timeout(&mut self, timeout: Duration) -> Result<&HelloAck, SessionError> {
        debug_assert_eq!(self.state, State::Spawned);
        let hello = Hello {
            protocol: self.host_protocol,
            flux_version: self.flux_version.clone(),
        };
        self.send_json(MessageKind::Hello, &hello)?;
        let frame = self.recv(timeout, "handshake")?;
        let ack = self.expect_json::<HelloAck>(frame, MessageKind::HelloAck, "handshake")?;
        if ack.protocol != self.host_protocol {
            return Err(SessionError::ProtocolMismatch {
                host: self.host_protocol,
                plugin: ack.protocol,
            });
        }
        self.plugin_info = Some(ack);
        self.state = State::Handshaked;
        Ok(self.plugin_info.as_ref().unwrap())
    }

    /// Step 2: send `ConfigureSink` with the resolved config + upstream
    /// schema, expect `ConfigureAck`.
    pub fn configure(
        &mut self,
        sink_type: &str,
        config: Value,
        input_schema: &Schema,
        materialization: Option<Value>,
    ) -> Result<(), SessionError> {
        debug_assert_eq!(self.state, State::Handshaked);
        let msg = ConfigureSink {
            sink_type: sink_type.to_string(),
            config,
            input_schema_ipc_b64: encode_schema_b64(input_schema)?,
            materialization,
        };
        self.send_json(MessageKind::ConfigureSink, &msg)?;
        let frame = self.recv(timeouts::CONFIGURE_ACK, "configure")?;
        let ack =
            self.expect_json::<ConfigureAck>(frame, MessageKind::ConfigureAck, "configure")?;
        if !ack.accepted {
            return Err(SessionError::ConfigureRejected {
                reason: ack.reason.unwrap_or_else(|| "<no reason>".into()),
            });
        }
        self.state = State::Configured;
        Ok(())
    }

    /// Stream one record batch and wait for `BatchAck`. Warnings are logged.
    pub fn send_batch(&mut self, batch: &RecordBatch) -> Result<BatchAck, SessionError> {
        debug_assert_eq!(self.state, State::Configured);
        let bytes = encode_record_batch(batch)?;
        self.transport.send(MessageKind::RecordBatch, &bytes)?;
        let frame = self.recv(timeouts::BATCH_ACK, "batch_ack")?;
        let ack = self.expect_json::<BatchAck>(frame, MessageKind::BatchAck, "batch_ack")?;
        if let Some(w) = &ack.warning {
            warn!(target: "flux::plugin", warning = %w, "plugin batch warning");
        }
        Ok(ack)
    }

    /// Send `Commit`, expect `CommitAck`. After this returns the session is
    /// done; call [`Self::shutdown`] to terminate the plugin cleanly.
    pub fn commit(&mut self) -> Result<crate::protocol::control::CommitAck, SessionError> {
        debug_assert_eq!(self.state, State::Configured);
        self.send_json(MessageKind::Commit, &serde_json::json!({}))?;
        let frame = self.recv(timeouts::COMMIT_ACK, "commit")?;
        let ack = self.expect_json::<crate::protocol::control::CommitAck>(
            frame,
            MessageKind::CommitAck,
            "commit",
        )?;
        self.state = State::Committed;
        Ok(ack)
    }

    /// Send `Abort` with a reason, wait for `AbortAck`. Errors are swallowed
    /// — caller is already in a failure path.
    pub fn abort(&mut self, reason: impl Into<String>) -> Result<(), SessionError> {
        if matches!(self.state, State::Aborted | State::Closed) {
            return Ok(());
        }
        let payload = Abort {
            reason: reason.into(),
        };
        let _ = self.send_json(MessageKind::Abort, &payload);
        match self.recv(timeouts::ABORT_ACK, "abort") {
            Ok(frame) if frame.kind == MessageKind::AbortAck => {}
            Ok(other) => warn!(
                target: "flux::plugin",
                kind = ?other.kind,
                "unexpected frame while waiting for AbortAck"
            ),
            Err(e) => warn!(target: "flux::plugin", error = %e, "abort wait failed"),
        }
        self.state = State::Aborted;
        Ok(())
    }

    /// Send `Shutdown`. Does not wait for the child to exit; the transport
    /// implementation is responsible for that on drop.
    pub fn shutdown(&mut self) -> Result<(), SessionError> {
        if self.state == State::Closed {
            return Ok(());
        }
        let _ = self.send_json(MessageKind::Shutdown, &serde_json::json!({}));
        self.state = State::Closed;
        Ok(())
    }

    fn send_json<V: serde::Serialize>(
        &mut self,
        kind: MessageKind,
        value: &V,
    ) -> Result<(), SessionError> {
        let bytes = serde_json::to_vec(value)?;
        self.transport.send(kind, &bytes)?;
        Ok(())
    }

    fn recv(&mut self, timeout: Duration, phase: &'static str) -> Result<Frame, SessionError> {
        loop {
            let frame = self.transport.recv(timeout, phase)?;
            if frame.kind == MessageKind::Error {
                let err: ErrorMsg = serde_json::from_slice(&frame.payload)?;
                return Err(SessionError::PluginError {
                    message: err.message,
                });
            }
            if frame.kind == MessageKind::DeclareResource {
                let dr: DeclareResource = serde_json::from_slice(&frame.payload)?;
                self.declared_resource = Some(dr.resource_fingerprint);
                continue;
            }
            return Ok(frame);
        }
    }

    fn expect_json<V: for<'de> serde::Deserialize<'de>>(
        &self,
        frame: Frame,
        expected: MessageKind,
        phase: &'static str,
    ) -> Result<V, SessionError> {
        if frame.kind != expected {
            return Err(SessionError::UnexpectedFrame {
                kind: frame.kind,
                phase,
            });
        }
        Ok(serde_json::from_slice(&frame.payload)?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;

    use super::*;
    use crate::protocol::control::{CommitAck, DeclareResource, Hello as _Hello};
    use crate::transport::mock::MockTransport;

    fn schema() -> Schema {
        Schema::new(vec![Field::new("v", DataType::Int32, false)])
    }

    fn batch(s: &Schema) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(s.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    fn json_frame<V: serde::Serialize>(kind: MessageKind, v: &V) -> Frame {
        Frame {
            kind,
            payload: serde_json::to_vec(v).unwrap(),
        }
    }

    #[test]
    fn full_lifecycle_happy_path() {
        let s = schema();
        let incoming = vec![
            json_frame(
                MessageKind::HelloAck,
                &HelloAck {
                    protocol: 1,
                    plugin_name: "mock".into(),
                    plugin_version: "0.0.1".into(),
                    capabilities: Default::default(),
                },
            ),
            json_frame(
                MessageKind::ConfigureAck,
                &ConfigureAck {
                    accepted: true,
                    reason: None,
                },
            ),
            json_frame(
                MessageKind::BatchAck,
                &BatchAck {
                    rows_accepted: 3,
                    warning: None,
                },
            ),
            json_frame(
                MessageKind::CommitAck,
                &CommitAck {
                    rows: 3,
                    bytes: 100,
                    duration_ms: 1,
                    rows_updated: 0,
                    rows_deleted: 0,
                },
            ),
        ];
        let transport = MockTransport::new(incoming);
        let mut session = PluginSession::new(transport, 1, "0.0.0-test");

        session.handshake().unwrap();
        assert_eq!(session.plugin_info().unwrap().plugin_name, "mock");
        session
            .configure("mock_sink", json!({"path": "/tmp/x"}), &s, None)
            .unwrap();
        let ack = session.send_batch(&batch(&s)).unwrap();
        assert_eq!(ack.rows_accepted, 3);
        let commit = session.commit().unwrap();
        assert_eq!(commit.rows, 3);
        session.shutdown().unwrap();

        // Verify the host actually sent the expected frame kinds in order.
        let kinds: Vec<MessageKind> = session.transport.sent.iter().map(|f| f.kind).collect();
        assert_eq!(
            kinds,
            vec![
                MessageKind::Hello,
                MessageKind::ConfigureSink,
                MessageKind::RecordBatch,
                MessageKind::Commit,
                MessageKind::Shutdown,
            ]
        );
    }

    #[test]
    fn protocol_mismatch_is_an_error() {
        let incoming = vec![json_frame(
            MessageKind::HelloAck,
            &HelloAck {
                protocol: 99,
                plugin_name: "mock".into(),
                plugin_version: "0".into(),
                capabilities: Default::default(),
            },
        )];
        let mut session = PluginSession::new(MockTransport::new(incoming), 1, "0.0.0");
        let err = session.handshake().unwrap_err();
        assert!(matches!(err, SessionError::ProtocolMismatch { .. }));
    }

    #[test]
    fn configure_rejection_propagates() {
        let s = schema();
        let incoming = vec![
            json_frame(
                MessageKind::HelloAck,
                &HelloAck {
                    protocol: 1,
                    plugin_name: "m".into(),
                    plugin_version: "0".into(),
                    capabilities: Default::default(),
                },
            ),
            json_frame(
                MessageKind::ConfigureAck,
                &ConfigureAck {
                    accepted: false,
                    reason: Some("nope".into()),
                },
            ),
        ];
        let mut session = PluginSession::new(MockTransport::new(incoming), 1, "0");
        session.handshake().unwrap();
        let err = session.configure("x", json!({}), &s, None).unwrap_err();
        assert!(matches!(err, SessionError::ConfigureRejected { .. }));
    }

    #[test]
    fn plugin_error_frame_fails_the_phase() {
        let incoming = vec![json_frame(
            MessageKind::Error,
            &ErrorMsg {
                message: "boom".into(),
                details: None,
            },
        )];
        let mut session = PluginSession::new(MockTransport::new(incoming), 1, "0");
        let err = session.handshake().unwrap_err();
        assert!(matches!(err, SessionError::PluginError { .. }));
    }

    #[test]
    fn declare_resource_captured_transparently() {
        let s = schema();
        let incoming = vec![
            json_frame(
                MessageKind::HelloAck,
                &HelloAck {
                    protocol: 1,
                    plugin_name: "lineage".into(),
                    plugin_version: "0.1.0".into(),
                    capabilities: Default::default(),
                },
            ),
            // Plugin sends DeclareResource before ConfigureAck.
            json_frame(
                MessageKind::DeclareResource,
                &DeclareResource {
                    resource_fingerprint: "postgres://db:5432/app/public.events".into(),
                },
            ),
            json_frame(
                MessageKind::ConfigureAck,
                &ConfigureAck {
                    accepted: true,
                    reason: None,
                },
            ),
            json_frame(
                MessageKind::BatchAck,
                &BatchAck {
                    rows_accepted: 3,
                    warning: None,
                },
            ),
            json_frame(
                MessageKind::CommitAck,
                &CommitAck {
                    rows: 3,
                    bytes: 50,
                    duration_ms: 1,
                    rows_updated: 0,
                    rows_deleted: 0,
                },
            ),
        ];
        let transport = MockTransport::new(incoming);
        let mut session = PluginSession::new(transport, 1, "0.0.0-test");

        session.handshake().unwrap();
        assert!(session.declared_resource().is_none());

        session
            .configure("lineage_sink", json!({}), &s, None)
            .unwrap();
        // DeclareResource was intercepted during configure.
        assert_eq!(
            session.declared_resource(),
            Some("postgres://db:5432/app/public.events")
        );

        let ack = session.send_batch(&batch(&s)).unwrap();
        assert_eq!(ack.rows_accepted, 3);
        let commit = session.commit().unwrap();
        assert_eq!(commit.rows, 3);
        session.shutdown().unwrap();
    }

    // Suppress unused-import warning when control re-export changes.
    #[allow(dead_code)]
    fn _force_imports(_: _Hello) {}
}
