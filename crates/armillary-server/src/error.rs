// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("no available port in range {0}..{1}")]
    NoAvailablePort(u16, u16),

    #[error("could not determine home directory")]
    NoHomeDir,

    #[error("lockfile error: {0}")]
    Lockfile(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("server error: {0}")]
    Serve(String),
}
