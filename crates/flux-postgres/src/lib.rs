// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PostgreSQL storage backends for Horizon Flux metadata.
//!
//! This crate provides [`PostgresPipelineStore`], [`PostgresRunStore`], and
//! [`PostgresEnvironmentStore`] — drop-in replacements for the default SQLite
//! stores that persist metadata in a shared PostgreSQL database.

pub mod bulk;
pub mod environment;
pub mod pipeline_store;
pub(crate) mod retry;
pub mod run_store;
mod schema;

pub use environment::PostgresEnvironmentStore;
pub use pipeline_store::PostgresPipelineStore;
pub use run_store::PostgresRunStore;

use deadpool_postgres::{Config, Pool, Runtime};
use tokio_postgres::NoTls;

/// Create a connection pool from a PostgreSQL connection string.
///
/// The pool is created with sensible defaults (max size 5). Advanced
/// configuration (pool size, timeouts, retry) is handled by the
/// configuration layer.
pub fn create_pool(connection_string: &str) -> Result<Pool, String> {
    let mut cfg = Config::new();
    cfg.url = Some(connection_string.to_string());
    cfg.create_pool(Some(Runtime::Tokio1), NoTls)
        .map_err(|e| format!("failed to create PostgreSQL connection pool: {e}"))
}

/// Run a future on the current tokio runtime from a sync context.
///
/// Uses `block_in_place` so we don't block an async worker thread.
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

/// Ensure the PostgreSQL schema is up to date. Call this once at startup
/// before constructing any stores.
pub async fn ensure_schema(pool: &Pool) -> Result<(), String> {
    schema::ensure_schema(pool).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that PostgresPipelineStore implements PipelineStorage + Send + Sync.
    #[allow(dead_code)]
    fn assert_pipeline_storage_impl(
        s: &PostgresPipelineStore,
    ) -> &(dyn flux_engine::PipelineStorage + Send + Sync) {
        s
    }

    /// Verify that PostgresRunStore implements RunStorage + Send + Sync.
    #[allow(dead_code)]
    fn assert_run_storage_impl(
        s: &PostgresRunStore,
    ) -> &(dyn flux_datafusion::RunStorage + Send + Sync) {
        s
    }

    /// Verify that PostgresEnvironmentStore implements EnvironmentStorage + Send + Sync.
    #[allow(dead_code)]
    fn assert_env_storage_impl(
        s: &PostgresEnvironmentStore,
    ) -> &(dyn flux_datafusion::EnvironmentStorage + Send + Sync) {
        s
    }

    #[test]
    fn create_pool_rejects_invalid_url() {
        // Malformed URL should fail pool creation.
        let result = create_pool("not-a-valid-url");
        assert!(result.is_err());
    }

    #[test]
    fn create_pool_accepts_valid_url() {
        // Valid URL creates a pool (doesn't actually connect).
        let result = create_pool("postgresql://localhost:5432/test");
        assert!(result.is_ok());
    }
}
