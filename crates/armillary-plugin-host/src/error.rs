// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::path::PathBuf;

use thiserror::Error;

use crate::protocol::FrameError;

/// Result alias used throughout the plugin host.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors surfaced by manifest parsing, discovery, and the protocol layer.
#[derive(Debug, Error)]
pub enum Error {
    #[error("plugin manifest at {path}: {message}")]
    Manifest { path: PathBuf, message: String },

    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("duplicate plugin name `{name}` in scan root {root}")]
    DuplicateName { name: String, root: PathBuf },

    #[error("duplicate sink type `{ty}` declared by plugin `{plugin}`")]
    DuplicateSinkType { ty: String, plugin: String },

    #[error(transparent)]
    Frame(#[from] FrameError),
}
