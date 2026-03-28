// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Traits for source and sink connectors, and a registry to look them up by name.
//!
//! **Sources** implement the [`SourceConnector`] trait, which is a factory that
//! creates a DataFusion [`TableProvider`] from connector-specific JSON config.
//! This design gives sources automatic filter and projection pushdown through
//! DataFusion's query planning.
//!
//! **Sinks** implement the [`PipelineSink`] trait, which writes record batches
//! to an external system and returns [`WriteStats`].

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use flux_engine::node::{SinkConfig, SourceConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Error type returned by connector implementations.
pub type ProviderError = Box<dyn std::error::Error + Send + Sync>;

// ---------------------------------------------------------------------------
// Source connector
// ---------------------------------------------------------------------------

/// Factory that creates a DataFusion `TableProvider` from connector config.
///
/// Source connectors implement DataFusion's `TableProvider` trait directly,
/// which enables automatic filter and projection pushdown. The
/// `SourceConnector` is a factory registered once per connector type (e.g.
/// `"csv"`, `"postgresql"`); the executor calls [`create_table_provider`] for
/// each source node, passing the node's connector-specific JSON config.
///
/// [`create_table_provider`]: SourceConnector::create_table_provider
pub trait SourceConnector: Send + Sync {
    /// Create a [`TableProvider`] for the given source configuration.
    fn create_table_provider(
        &self,
        config: &SourceConfig,
    ) -> Result<Arc<dyn TableProvider>, ProviderError>;
}

// ---------------------------------------------------------------------------
// Sink connector
// ---------------------------------------------------------------------------

/// Statistics from a completed sink write operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteStats {
    /// Number of rows successfully written.
    pub rows_written: u64,
    /// Total bytes written (best-effort estimate; may be 0 when not measurable).
    pub bytes_written: u64,
    /// Wall-clock duration of the write operation.
    pub duration: Duration,
}

/// Options controlling how a sink writes data.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WriteOptions {
    /// Maximum rows per batch or insert statement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,

    /// Conflict handling for database sinks (e.g. ON CONFLICT behavior).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_conflict: Option<OnConflict>,

    /// Format-specific options (e.g. compression codec, CSV delimiter).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub format_options: HashMap<String, String>,
}

/// How to handle conflicts during database writes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnConflict {
    /// Fail the write on conflict.
    Fail,
    /// Skip conflicting rows.
    Skip,
    /// Update existing rows with new values.
    Update,
}

/// Writes pipeline data to an external system (file, database, stdout, etc.).
///
/// Sink connectors are registered once per connector type. The executor calls
/// [`write`] for each sink node, passing the node's config and upstream data.
///
/// [`write`]: PipelineSink::write
#[async_trait]
pub trait PipelineSink: Send + Sync {
    /// Write record batches to the sink, returning statistics on completion.
    async fn write(
        &self,
        config: &SinkConfig,
        data: Vec<RecordBatch>,
        options: &WriteOptions,
    ) -> Result<WriteStats, ProviderError>;

    /// Validate that a sink configuration is complete and well-formed.
    fn validate_config(&self, config: &SinkConfig) -> Result<(), ProviderError>;
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Maps connector type names (e.g. `"csv"`, `"postgresql"`) to source and
/// sink implementations.
#[derive(Default)]
pub struct ProviderRegistry {
    sources: HashMap<String, Arc<dyn SourceConnector>>,
    sinks: HashMap<String, Arc<dyn PipelineSink>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    // -- sources --

    pub fn register_source(
        &mut self,
        connector: impl Into<String>,
        source: Arc<dyn SourceConnector>,
    ) {
        self.sources.insert(connector.into(), source);
    }

    pub fn get_source(&self, connector: &str) -> Option<&Arc<dyn SourceConnector>> {
        self.sources.get(connector)
    }

    /// List registered source connector names.
    pub fn source_names(&self) -> Vec<&str> {
        self.sources.keys().map(|s| s.as_str()).collect()
    }

    // -- sinks --

    pub fn register_sink(&mut self, connector: impl Into<String>, sink: Arc<dyn PipelineSink>) {
        self.sinks.insert(connector.into(), sink);
    }

    pub fn get_sink(&self, connector: &str) -> Option<&Arc<dyn PipelineSink>> {
        self.sinks.get(connector)
    }

    /// List registered sink connector names.
    pub fn sink_names(&self) -> Vec<&str> {
        self.sinks.keys().map(|s| s.as_str()).collect()
    }

    /// List all registered connector names (sources + sinks, deduplicated).
    pub fn connector_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self
            .sources
            .keys()
            .chain(self.sinks.keys())
            .map(|s| s.as_str())
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    }
}
