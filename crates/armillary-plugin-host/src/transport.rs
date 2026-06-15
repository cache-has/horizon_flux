// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Abstraction over the host↔plugin byte stream.
//!
//! Real plugins are talked to over a child process's stdin/stdout (see
//! [`crate::process`]). Tests use an in-memory implementation
//! ([`MockTransport`]) so the lifecycle state machine can be exercised
//! without spawning subprocesses.

use std::time::Duration;

use thiserror::Error;

use crate::protocol::{Frame, FrameError, MessageKind};

/// Errors that can occur on the transport layer.
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("transport closed (plugin exited or pipe broken)")]
    Closed,

    #[error("timed out waiting for {phase} after {timeout:?}")]
    Timeout {
        phase: &'static str,
        timeout: Duration,
    },

    #[error(transparent)]
    Frame(#[from] FrameError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Sync, blocking host-side transport.
pub trait Transport: Send {
    /// Write one frame to the plugin.
    fn send(&mut self, kind: MessageKind, payload: &[u8]) -> Result<(), TransportError>;

    /// Read the next frame from the plugin, blocking up to `timeout`. `Log`
    /// frames are not delivered through this method — implementations forward
    /// them to `tracing` and continue waiting for the next non-log frame.
    fn recv(&mut self, timeout: Duration, phase: &'static str) -> Result<Frame, TransportError>;
}

#[cfg(test)]
pub mod mock {
    use std::collections::VecDeque;

    use super::*;

    /// In-memory transport for unit tests. Sent frames are appended to
    /// `sent`; `recv` pops from a pre-populated `incoming` queue.
    pub struct MockTransport {
        pub sent: Vec<Frame>,
        pub incoming: VecDeque<Frame>,
    }

    impl MockTransport {
        pub fn new(incoming: Vec<Frame>) -> Self {
            Self {
                sent: Vec::new(),
                incoming: incoming.into(),
            }
        }
    }

    impl Transport for MockTransport {
        fn send(&mut self, kind: MessageKind, payload: &[u8]) -> Result<(), TransportError> {
            self.sent.push(Frame {
                kind,
                payload: payload.to_vec(),
            });
            Ok(())
        }

        fn recv(
            &mut self,
            _timeout: Duration,
            _phase: &'static str,
        ) -> Result<Frame, TransportError> {
            self.incoming.pop_front().ok_or(TransportError::Closed)
        }
    }
}
