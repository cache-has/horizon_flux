// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Connector registry — maps connector type names to factory implementations
//! and provides methods for creating sources/sinks and populating a
//! [`ProviderRegistry`].

use flux_datafusion::provider::{PipelineSink, ProviderError, ProviderRegistry, SourceConnector};
use std::collections::HashMap;
use std::sync::Arc;

/// High-level registry of all available connector types.
///
/// The `ConnectorRegistry` stores factory implementations (one per connector
/// type) and can populate a [`ProviderRegistry`] with them for pipeline
/// execution.
///
/// ```ignore
/// let mut cr = ConnectorRegistry::new();
/// cr.register_source("csv", Arc::new(CsvSource::new()));
/// cr.register_sink("csv", Arc::new(CsvSink::new()));
///
/// // Populate a ProviderRegistry for the executor.
/// let provider_registry = cr.into_provider_registry();
/// ```
#[derive(Default)]
pub struct ConnectorRegistry {
    sources: HashMap<String, Arc<dyn SourceConnector>>,
    sinks: HashMap<String, Arc<dyn PipelineSink>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a source connector factory.
    pub fn register_source(
        &mut self,
        connector: impl Into<String>,
        source: Arc<dyn SourceConnector>,
    ) {
        self.sources.insert(connector.into(), source);
    }

    /// Register a sink connector factory.
    pub fn register_sink(
        &mut self,
        connector: impl Into<String>,
        sink: Arc<dyn PipelineSink>,
    ) {
        self.sinks.insert(connector.into(), sink);
    }

    /// List all registered source connector names.
    pub fn source_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.sources.keys().map(|s| s.as_str()).collect();
        names.sort_unstable();
        names
    }

    /// List all registered sink connector names.
    pub fn sink_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.sinks.keys().map(|s| s.as_str()).collect();
        names.sort_unstable();
        names
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

    /// Get a registered source connector.
    pub fn get_source(&self, connector: &str) -> Option<&Arc<dyn SourceConnector>> {
        self.sources.get(connector)
    }

    /// Get a registered sink connector.
    pub fn get_sink(&self, connector: &str) -> Option<&Arc<dyn PipelineSink>> {
        self.sinks.get(connector)
    }

    /// Validate all connector configurations in a pipeline before execution.
    pub fn validate_pipeline(
        &self,
        pipeline: &flux_engine::Pipeline,
    ) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        for node in &pipeline.nodes {
            match &node.kind {
                flux_engine::node::NodeKind::Source(cfg) => {
                    if !self.sources.contains_key(&cfg.connector) {
                        errors.push(format!(
                            "node `{}`: source connector `{}` not registered",
                            node.id, cfg.connector,
                        ));
                    }
                }
                flux_engine::node::NodeKind::Sink(cfg) => {
                    if !self.sinks.contains_key(&cfg.connector) {
                        errors.push(format!(
                            "node `{}`: sink connector `{}` not registered",
                            node.id, cfg.connector,
                        ));
                    }
                    if let Some(sink) = self.sinks.get(&cfg.connector) {
                        if let Err(e) = sink.validate_config(cfg) {
                            errors.push(format!(
                                "node `{}`: sink config invalid: {}",
                                node.id, e,
                            ));
                        }
                    }
                }
                flux_engine::node::NodeKind::Transform(_) => {}
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Convert into a [`ProviderRegistry`] for use with the pipeline executor.
    pub fn into_provider_registry(self) -> ProviderRegistry {
        let mut registry = ProviderRegistry::new();
        for (name, source) in self.sources {
            registry.register_source(name, source);
        }
        for (name, sink) in self.sinks {
            registry.register_sink(name, sink);
        }
        registry
    }

    /// Build a [`ProviderRegistry`] by cloning Arc references (non-consuming).
    pub fn to_provider_registry(&self) -> ProviderRegistry {
        let mut registry = ProviderRegistry::new();
        for (name, source) in &self.sources {
            registry.register_source(name.clone(), Arc::clone(source));
        }
        for (name, sink) in &self.sinks {
            registry.register_sink(name.clone(), Arc::clone(sink));
        }
        registry
    }
}

/// Errors from connector validation.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("unknown connector type: {0}")]
    UnknownConnector(String),

    #[error("invalid connector configuration: {0}")]
    InvalidConfig(#[source] ProviderError),

    #[error("connector `{connector}` validation failed: {message}")]
    ValidationFailed {
        connector: String,
        message: String,
    },
}
