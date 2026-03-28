// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Traits for source and sink connectors, and a registry to look them up by name.

use arrow::record_batch::RecordBatch;
use flux_engine::node::{SinkConfig, SourceConfig};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Error type returned by provider/writer implementations.
pub type ProviderError = Box<dyn std::error::Error + Send + Sync>;

/// Reads data for a source node. Implementations live in `flux-connectors`.
pub trait SourceProvider: Send + Sync {
    fn read(
        &self,
        config: &SourceConfig,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, ProviderError>> + Send + '_>>;
}

/// Writes data for a sink node. Implementations live in `flux-connectors`.
///
/// Returns the number of rows written.
pub trait SinkWriter: Send + Sync {
    fn write(
        &self,
        config: &SinkConfig,
        batches: Vec<RecordBatch>,
    ) -> Pin<Box<dyn Future<Output = Result<u64, ProviderError>> + Send + '_>>;
}

/// Maps connector type names (e.g. `"csv"`, `"postgresql"`) to provider/writer
/// implementations.
#[derive(Default)]
pub struct ProviderRegistry {
    sources: HashMap<String, Arc<dyn SourceProvider>>,
    sinks: HashMap<String, Arc<dyn SinkWriter>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_source(
        &mut self,
        connector: impl Into<String>,
        provider: Arc<dyn SourceProvider>,
    ) {
        self.sources.insert(connector.into(), provider);
    }

    pub fn register_sink(
        &mut self,
        connector: impl Into<String>,
        writer: Arc<dyn SinkWriter>,
    ) {
        self.sinks.insert(connector.into(), writer);
    }

    pub fn get_source(&self, connector: &str) -> Option<&Arc<dyn SourceProvider>> {
        self.sources.get(connector)
    }

    pub fn get_sink(&self, connector: &str) -> Option<&Arc<dyn SinkWriter>> {
        self.sinks.get(connector)
    }
}
