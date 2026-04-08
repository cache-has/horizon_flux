// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Wire protocol for the Horizon Flux plugin system — shared between
//! `flux-plugin-host` (host side) and `flux-plugin-sdk` (plugin side) so the
//! two cannot drift out of sync.
//!
//! See `docs/plugins/protocol-v1.md` for the normative reference.
//!
//! Layers:
//! - [`frame`] — length-prefixed framing on raw byte streams.
//! - [`control`] — JSON control message types.
//! - [`arrow_ipc`] — Arrow IPC schema/batch (de)serialization helpers.
//!
//! This crate has no async, no I/O abstractions beyond `std::io::{Read,
//! Write}`, and no dependency on any other flux crate. Plugin authors should
//! be able to depend on it directly without pulling in flux internals.

pub mod arrow_ipc;
pub mod control;
pub mod frame;

pub use control::{
    Abort, BatchAck, CommitAck, ConfigureAck, ConfigureSink, ControlError, ErrorMsg, Hello,
    HelloAck, Log, LogLevel, read_json_frame, write_json_frame,
};
pub use frame::{Frame, FrameError, MAX_PAYLOAD_LEN, MessageKind, read_frame, write_frame};

/// The plugin protocol major version this build of flux speaks.
pub const PROTOCOL_VERSION: u32 = 1;
