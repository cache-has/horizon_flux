// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Error types for the secret store.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SecretError {
    #[error("secret not found: {0}")]
    NotFound(String),

    #[error("encryption error: {0}")]
    Encryption(String),

    #[error("decryption error: {0}")]
    Decryption(String),

    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("secret store not initialized — run `horizon-flux secret init` first")]
    NotInitialized,

    #[error("secret store already initialized")]
    AlreadyInitialized,

    #[error("invalid secret reference: {0}")]
    InvalidReference(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
