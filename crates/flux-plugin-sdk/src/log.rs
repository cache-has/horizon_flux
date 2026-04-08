// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Log forwarding helpers for plugin authors.
//!
//! Plugins write log lines to **stderr**, which the flux host captures and
//! re-emits through `tracing` (see `flux-plugin-host::process` for the
//! receive side). Using stderr keeps the stdout control channel free of
//! interleaving and means logging works from any thread the plugin spawns,
//! without the SDK having to coordinate locks.
//!
//! ```no_run
//! use flux_plugin_sdk::log;
//! log::info("starting batch write");
//! log::warn("falling back to single-row insert");
//! log::error("write failed; aborting");
//! ```

use std::io::Write;

fn emit(level: &str, msg: &str) {
    // Best-effort: a broken stderr is not a reason to fail the plugin.
    let _ = writeln!(std::io::stderr(), "[{level}] {msg}");
}

pub fn trace(msg: impl AsRef<str>) {
    emit("TRACE", msg.as_ref());
}

pub fn debug(msg: impl AsRef<str>) {
    emit("DEBUG", msg.as_ref());
}

pub fn info(msg: impl AsRef<str>) {
    emit("INFO", msg.as_ref());
}

pub fn warn(msg: impl AsRef<str>) {
    emit("WARN", msg.as_ref());
}

pub fn error(msg: impl AsRef<str>) {
    emit("ERROR", msg.as_ref());
}
