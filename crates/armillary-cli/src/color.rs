// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Colored output helpers that respect `NO_COLOR` and non-TTY environments.

use owo_colors::OwoColorize;

/// Initialize color support. Call once at startup.
/// Respects `NO_COLOR` env var (https://no-color.org/) and checks for TTY.
pub fn init() {
    if std::env::var_os("NO_COLOR").is_some()
        || !std::io::IsTerminal::is_terminal(&std::io::stdout())
    {
        owo_colors::set_override(false);
    } else {
        owo_colors::set_override(true);
    }
}

/// Format a value as bold (for headers).
pub fn bold(s: &str) -> String {
    format!("{}", s.bold())
}

/// Format a value as green (for success).
pub fn green(s: &str) -> String {
    format!("{}", s.green())
}

/// Format a value as red (for errors/failures).
pub fn red(s: &str) -> String {
    format!("{}", s.red())
}

/// Format a value as yellow (for warnings).
pub fn yellow(s: &str) -> String {
    format!("{}", s.yellow())
}

/// Format a value as dim/grey (for secondary info).
pub fn dim(s: &str) -> String {
    format!("{}", s.dimmed())
}
