// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Host-side plugin runtime for Horizon Flux.
//!
//! See `planning/24-plugin-system.md` and `docs/plugins/` (in particular
//! `docs/plugins/architecture.md` for the layering rationale). Layers:
//!
//! - [`manifest`] / [`discovery`] — find and validate `plugin.toml` files.
//! - [`protocol`] — wire framing and JSON control messages.
//! - [`arrow_ipc`] — schema/batch (de)serialization for the data channel.
//! - [`transport`] — the byte-stream abstraction the session runs on.
//! - [`process`] — spawns plugin executables and implements the transport
//!   over a child's stdin/stdout, with stderr + `Log` forwarding into
//!   `tracing`.
//! - [`session`] — sink lifecycle state machine on top of any transport.

pub mod arrow_ipc;
pub mod discovery;
pub mod error;
pub mod manifest;
pub mod process;
pub mod protocol;
pub mod session;
pub mod transport;

pub use discovery::{DiscoveredPlugin, PluginRegistry, PluginStatus, discover_plugins};
pub use error::{Error, Result};
pub use manifest::{Manifest, SinkCapabilities, SinkDeclaration};
pub use process::{PluginProcess, SpawnOptions};
pub use protocol::{Frame, FrameError, MAX_PAYLOAD_LEN, MessageKind, PROTOCOL_VERSION, control};
pub use session::{PluginSession, SessionError};
pub use transport::{Transport, TransportError};
