// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Wire protocol for the Horizon Flux plugin system.
//!
//! See `docs/plugins/protocol-v1.md` for the normative reference. This module
//! provides the framing layer ([`frame`]) and the JSON control message types
//! ([`control`]).

pub mod control;
pub mod frame;

pub use control::*;
pub use frame::{Frame, FrameError, MAX_PAYLOAD_LEN, MessageKind, read_frame, write_frame};

/// The plugin protocol major version this build of flux speaks.
pub const PROTOCOL_VERSION: u32 = 1;
