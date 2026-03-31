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
    pub fn register_sink(&mut self, connector: impl Into<String>, sink: Arc<dyn PipelineSink>) {
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
    ///
    /// Checks that referenced connector types are registered, sink configs are
    /// valid, and environment override keys match the target connector's schema.
    pub fn validate_pipeline(&self, pipeline: &flux_engine::Pipeline) -> Result<(), Vec<String>> {
        use std::collections::HashMap;

        let mut errors = Vec::new();

        // Build a node_id → connector_type lookup for override validation.
        let mut node_connectors: HashMap<&str, &str> = HashMap::new();

        for node in &pipeline.nodes {
            match &node.kind {
                flux_engine::node::NodeKind::Source(cfg) => {
                    node_connectors.insert(node.id.0.as_str(), cfg.connector.as_str());
                    if !self.sources.contains_key(&cfg.connector) {
                        errors.push(format!(
                            "node `{}`: source connector `{}` not registered",
                            node.id, cfg.connector,
                        ));
                    }
                }
                flux_engine::node::NodeKind::Sink(cfg) => {
                    node_connectors.insert(node.id.0.as_str(), cfg.connector.as_str());
                    if !self.sinks.contains_key(&cfg.connector) {
                        errors.push(format!(
                            "node `{}`: sink connector `{}` not registered",
                            node.id, cfg.connector,
                        ));
                    }
                    if let Some(sink) = self.sinks.get(&cfg.connector) {
                        if let Err(e) = sink.validate_config(cfg) {
                            errors.push(format!("node `{}`: sink config invalid: {}", node.id, e,));
                        }
                    }
                }
                flux_engine::node::NodeKind::Transform(_) => {}
            }
        }

        // Validate environment override keys against connector schemas.
        for (env_name, env_overrides) in &pipeline.environment_overrides {
            for (node_id, override_val) in env_overrides {
                let Some(connector_type) = node_connectors.get(node_id.as_str()) else {
                    // Node not found or is a transform — node-existence is
                    // checked by flux-engine's validate_import, and transforms
                    // don't have connector configs to override.
                    if pipeline.nodes.iter().any(|n| {
                        n.id.0 == *node_id
                            && matches!(n.kind, flux_engine::node::NodeKind::Transform(_))
                    }) {
                        errors.push(format!(
                            "environment `{env_name}`: override for transform node \
                             `{node_id}` is not supported (only source/sink nodes \
                             can have config overrides)",
                        ));
                    }
                    continue;
                };

                let Some(valid_keys) =
                    crate::config::ConnectorConfig::valid_config_keys(connector_type)
                else {
                    continue; // Unknown connector — already reported above.
                };

                if let Some(obj) = override_val.as_object() {
                    for key in obj.keys() {
                        if !valid_keys.contains(&key.as_str()) {
                            errors.push(format!(
                                "environment `{env_name}`, node `{node_id}`: \
                                 override key `{key}` is not a valid config field \
                                 for connector `{connector_type}` \
                                 (valid keys: {})",
                                valid_keys.join(", "),
                            ));
                        }
                    }
                }
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
    ValidationFailed { connector: String, message: String },
}

#[cfg(test)]
mod tests {
    use flux_engine::edge::Edge;
    use flux_engine::node::*;
    use flux_engine::pipeline::Pipeline;
    use std::collections::BTreeMap;

    fn simple_pipeline() -> Pipeline {
        Pipeline {
            name: "test".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: BTreeMap::new(),
            environment_overrides: BTreeMap::new(),
            sample_config: None,
            cache_row_limit: None,
            code_dir: None,
            nodes: vec![
                Node {
                    id: NodeId::new("src"),
                    name: "src".into(),
                    kind: NodeKind::Source(SourceConfig {
                        connector: "csv".into(),
                        config: serde_json::json!({"path": "/data/input.csv", "format": "csv"}),
                        cache_row_limit: None,
                    }),
                    position: Position::default(),
                    pinned_position: false,
                },
                Node {
                    id: NodeId::new("sink"),
                    name: "sink".into(),
                    kind: NodeKind::Sink(SinkConfig {
                        connector: "stdout".into(),
                        config: serde_json::json!({"format": "table"}),
                    }),
                    position: Position::default(),
                    pinned_position: false,
                },
            ],
            edges: vec![Edge::new("src", "sink")],
        }
    }

    #[test]
    fn valid_override_keys_pass() {
        let registry = crate::default_registry();
        let mut pipeline = simple_pipeline();

        let mut overrides = BTreeMap::new();
        overrides.insert(
            "src".to_string(),
            serde_json::json!({"path": "/data/prod.csv"}),
        );
        pipeline
            .environment_overrides
            .insert("prod".into(), overrides);

        assert!(registry.validate_pipeline(&pipeline).is_ok());
    }

    #[test]
    fn invalid_override_key_rejected() {
        let registry = crate::default_registry();
        let mut pipeline = simple_pipeline();

        let mut overrides = BTreeMap::new();
        overrides.insert(
            "sink".to_string(),
            serde_json::json!({"nonexistent_field": true}),
        );
        pipeline
            .environment_overrides
            .insert("prod".into(), overrides);

        let errors = registry.validate_pipeline(&pipeline).unwrap_err();
        assert!(
            errors.iter().any(|e| e.contains("nonexistent_field")
                && e.contains("not a valid config field")),
            "expected invalid key error, got: {errors:?}",
        );
    }

    #[test]
    fn override_on_transform_node_rejected() {
        let registry = crate::default_registry();
        let mut pipeline = simple_pipeline();

        // Add a transform node.
        pipeline.nodes.push(Node {
            id: NodeId::new("xform"),
            name: "xform".into(),
            kind: NodeKind::Transform(TransformConfig {
                mode: TransformMode::Sql,
                code: "SELECT * FROM src".into(),
                code_path: None,
                materialized: false,
                cache_row_limit: None,
            }),
            position: Position::default(),
            pinned_position: false,
        });
        pipeline.edges = vec![
            Edge::new("src", "xform"),
            Edge::new("xform", "sink"),
        ];

        let mut overrides = BTreeMap::new();
        overrides.insert(
            "xform".to_string(),
            serde_json::json!({"code": "SELECT 1"}),
        );
        pipeline
            .environment_overrides
            .insert("prod".into(), overrides);

        let errors = registry.validate_pipeline(&pipeline).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("transform") && e.contains("not supported")),
            "expected transform override error, got: {errors:?}",
        );
    }
}
