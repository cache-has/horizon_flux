// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Concrete source and sink connector implementations for Horizon Flux.
//!
//! This crate provides:
//! - [`ConnectorConfig`]: Typed, serializable configuration for each connector
//! - [`ConnectorRegistry`]: Factory that creates connectors from config and
//!   populates a [`ProviderRegistry`]

pub mod config;
pub mod file_sink;
pub mod file_source;
pub mod postgres_sink;
pub mod postgres_source;
pub mod registry;
pub mod rest_api_source;

pub use config::ConnectorConfig;
pub use file_sink::FileSink;
pub use file_source::FileSource;
pub use postgres_sink::PostgresSink;
pub use postgres_source::PostgresSource;
pub use registry::ConnectorRegistry;
pub use rest_api_source::RestApiSource;

use std::sync::Arc;

/// Create a [`ConnectorRegistry`] pre-populated with all built-in connectors.
pub fn default_registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();

    let file_source: Arc<dyn flux_datafusion::provider::SourceConnector> =
        Arc::new(FileSource::new());
    // Register under multiple aliases so pipeline JSON can use any of them.
    registry.register_source("file", Arc::clone(&file_source));
    registry.register_source("csv", Arc::clone(&file_source));
    registry.register_source("parquet", file_source);

    let pg_source: Arc<dyn flux_datafusion::provider::SourceConnector> =
        Arc::new(PostgresSource::new());
    registry.register_source("postgresql", Arc::clone(&pg_source));
    registry.register_source("postgres", pg_source);

    let rest_source: Arc<dyn flux_datafusion::provider::SourceConnector> =
        Arc::new(RestApiSource::new());
    registry.register_source("rest_api", Arc::clone(&rest_source));
    registry.register_source("rest", Arc::clone(&rest_source));
    registry.register_source("http", rest_source);

    let file_sink: Arc<dyn flux_datafusion::provider::PipelineSink> =
        Arc::new(FileSink::new());
    registry.register_sink("file", Arc::clone(&file_sink));
    registry.register_sink("csv", Arc::clone(&file_sink));
    registry.register_sink("parquet", file_sink);

    let pg_sink: Arc<dyn flux_datafusion::provider::PipelineSink> =
        Arc::new(PostgresSink::new());
    registry.register_sink("postgresql", Arc::clone(&pg_sink));
    registry.register_sink("postgres", pg_sink);

    registry
}

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
