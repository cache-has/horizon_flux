// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Host-side plugin runtime for Armillary.
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

pub mod discovery;
pub mod error;
pub mod manifest;
pub mod process;
pub mod session;
pub mod transport;

/// Re-export of the shared `armillary-plugin-protocol` crate so existing call
/// sites that use `armillary_plugin_host::protocol::*` continue to work and so
/// downstream code has a single import path for protocol types.
pub use armillary_plugin_protocol as protocol;
pub use armillary_plugin_protocol::arrow_ipc;

pub use armillary_plugin_protocol::{
    Frame, FrameError, MAX_PAYLOAD_LEN, MessageKind, PROTOCOL_VERSION, control,
};
pub use discovery::{
    DiscoveredPlugin, PluginRegistry, PluginStatus, discover_plugins, discover_plugins_in,
};
pub use error::{Error, Result};
pub use manifest::{Manifest, MaterializationCapabilities, SinkCapabilities, SinkDeclaration};
pub use process::{PluginProcess, SpawnOptions};
pub use session::{PluginSession, SessionError};
pub use transport::{Transport, TransportError};
