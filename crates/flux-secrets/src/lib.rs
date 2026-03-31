// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Encrypted secret store for Horizon Flux.
//!
//! Provides AES-256-GCM encrypted storage for database passwords, API keys,
//! and other credentials. Secrets are environment-scoped and resolved at
//! pipeline execution time via `{{ secret:name }}` references in connector
//! configs.

pub mod crypto;
pub mod error;
pub mod resolve;
pub mod scrub;
pub mod store;

pub use error::SecretError;
pub use resolve::{
    has_secret_refs, resolve_json_secrets, resolve_json_secrets_collecting, resolve_secrets,
    resolve_secrets_collecting,
};
pub use scrub::{scrub_connection_strings, scrub_secrets};
pub use store::{SecretMetadata, SecretStore};

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
