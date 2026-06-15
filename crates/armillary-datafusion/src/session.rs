// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared DataFusion session configuration.
//!
//! [`SessionFactory`] produces pre-configured [`SessionContext`] instances that
//! share a single [`RuntimeEnv`] — enforcing a process-wide memory limit and
//! spill-to-disk policy across all pipeline nodes.

use datafusion::execution::memory_pool::{FairSpillPool, TrackConsumersPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

/// Default memory pool size: 512 MiB.
const DEFAULT_MEMORY_LIMIT: usize = 512 * 1024 * 1024;

/// Default maximum spill directory size: 2 GiB.
const DEFAULT_MAX_SPILL_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// Number of top memory consumers to track for error diagnostics.
const TRACKED_CONSUMERS: usize = 5;

/// Configuration for building a [`SessionFactory`].
#[derive(Debug, Clone)]
pub struct SessionFactoryConfig {
    /// Maximum memory (bytes) available to DataFusion operators.
    /// Operators that exceed this limit will spill to disk.
    pub memory_limit: usize,
    /// Directory for spill files. Defaults to the system temp directory when
    /// `None`.
    pub spill_path: Option<PathBuf>,
    /// Maximum total size (bytes) of spill files on disk.
    pub max_spill_size: u64,
}

impl Default for SessionFactoryConfig {
    fn default() -> Self {
        Self {
            memory_limit: DEFAULT_MEMORY_LIMIT,
            spill_path: None,
            max_spill_size: DEFAULT_MAX_SPILL_SIZE,
        }
    }
}

/// Produces [`SessionContext`] instances that share a process-wide
/// [`RuntimeEnv`] with memory limits and spill-to-disk configuration.
///
/// The underlying `FairSpillPool` divides memory fairly among spillable
/// operators (sorts, aggregations, joins), preventing any single operator
/// from starving the rest.
#[derive(Debug, Clone)]
pub struct SessionFactory {
    runtime: Arc<RuntimeEnv>,
}

impl SessionFactory {
    /// Build a new factory from the given configuration.
    pub fn new(config: &SessionFactoryConfig) -> datafusion::error::Result<Self> {
        let pool = TrackConsumersPool::new(
            FairSpillPool::new(config.memory_limit),
            NonZeroUsize::new(TRACKED_CONSUMERS).expect("non-zero"),
        );

        let mut builder = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(pool))
            .with_max_temp_directory_size(config.max_spill_size);

        if let Some(ref path) = config.spill_path {
            builder = builder.with_temp_file_path(path);
        }

        let runtime = builder.build_arc()?;
        Ok(Self { runtime })
    }

    /// Create a new [`SessionContext`] backed by the shared runtime.
    pub fn create_context(&self) -> SessionContext {
        let config = SessionConfig::new();
        SessionContext::new_with_config_rt(config, Arc::clone(&self.runtime))
    }
}

impl Default for SessionFactory {
    /// Creates a factory with [`SessionFactoryConfig::default()`] (512 MiB
    /// memory limit, system temp dir for spill, 2 GiB max spill).
    fn default() -> Self {
        Self::new(&SessionFactoryConfig::default())
            .expect("default SessionFactory configuration should not fail")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryLimit;

    #[test]
    fn default_factory_creates_working_context() {
        let factory = SessionFactory::default();
        let ctx = factory.create_context();
        let pool = ctx.runtime_env().memory_pool.clone();
        assert!(
            matches!(pool.memory_limit(), MemoryLimit::Finite(n) if n == DEFAULT_MEMORY_LIMIT),
            "memory pool should be bounded to the configured limit"
        );
    }

    #[test]
    fn custom_config() {
        let config = SessionFactoryConfig {
            memory_limit: 64 * 1024 * 1024,
            spill_path: Some(std::env::temp_dir().join("hf_test_spill")),
            max_spill_size: 500 * 1024 * 1024,
        };
        let factory = SessionFactory::new(&config).unwrap();
        let ctx = factory.create_context();
        let pool = ctx.runtime_env().memory_pool.clone();
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(n) if n == 64 * 1024 * 1024));
    }
}
