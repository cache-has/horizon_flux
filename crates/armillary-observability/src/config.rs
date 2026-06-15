// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Observability configuration.
//!
//! Loaded from `~/.armillary/config.toml` under the `[logging]` section.
//! Environment variables and CLI flags take precedence per the standard
//! resolution order.

use serde::Deserialize;

/// Top-level observability configuration.
///
/// Covers logging, Prometheus metrics, OpenTelemetry tracing, and OpenLineage
/// event emission.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ObservabilityConfig {
    pub logging: LoggingConfig,
    pub metrics: MetricsConfig,
    pub tracing: TracingConfig,
    pub openlineage: OpenLineageConfig,
}

/// Logging configuration.
///
/// ```toml
/// [logging]
/// format = "json"    # or "pretty"
/// level = "info"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// Output format: `json` for structured JSON, `pretty` for human-readable.
    pub format: LogFormat,
    /// Minimum log level. Overridden by `RUST_LOG` if set.
    pub level: LogLevel,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            format: LogFormat::Pretty,
            level: LogLevel::Info,
        }
    }
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Structured JSON — one JSON object per line, matching the documented
    /// armillary event envelope schema.
    Json,
    /// Human-readable colored output for local development.
    Pretty,
}

/// Log level filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Prometheus metrics configuration.
///
/// ```toml
/// [metrics]
/// enabled = true
/// path = "/metrics"
/// include_labels = ["pipeline", "environment"]
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Whether to install the Prometheus metrics recorder.
    pub enabled: bool,
    /// HTTP path for the `/metrics` endpoint.
    pub path: String,
    /// Label allow-list for cardinality control. When set, only these labels
    /// are included in metric recordings. `None` means all labels are included.
    pub include_labels: Option<Vec<String>>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "/metrics".to_string(),
            include_labels: None,
        }
    }
}

/// OpenTelemetry tracing configuration.
///
/// ```toml
/// [tracing]
/// enabled = true
/// otlp_endpoint = "http://otel-collector:4317"
/// sampling_ratio = 1.0
/// service_name = "armillary"
/// service_namespace = "analytics"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TracingConfig {
    /// Whether to enable OpenTelemetry trace export.
    pub enabled: bool,
    /// OTLP gRPC endpoint (e.g. `http://localhost:4317`).
    pub otlp_endpoint: String,
    /// Sampling ratio (0.0–1.0). 1.0 = trace everything.
    pub sampling_ratio: f64,
    /// OTel service name attribute.
    pub service_name: String,
    /// OTel service namespace attribute.
    pub service_namespace: Option<String>,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: "http://localhost:4317".to_string(),
            sampling_ratio: 1.0,
            service_name: "armillary".to_string(),
            service_namespace: None,
        }
    }
}

/// OpenLineage event emission configuration.
///
/// ```toml
/// [openlineage]
/// enabled = true
/// endpoint = "http://marquez:5000/api/v1/lineage"
/// namespace = "analytics"
/// include_column_lineage = true
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OpenLineageConfig {
    /// Whether to emit OpenLineage events.
    pub enabled: bool,
    /// HTTP endpoint for the OpenLineage receiver (e.g. Marquez, DataHub).
    pub endpoint: String,
    /// OpenLineage job namespace.
    pub namespace: String,
    /// Whether to include column-level lineage facets on output datasets.
    pub include_column_lineage: bool,
}

impl Default for OpenLineageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://localhost:5000/api/v1/lineage".to_string(),
            namespace: "default".to_string(),
            include_column_lineage: true,
        }
    }
}

impl LogLevel {
    /// Convert to a `tracing_subscriber::EnvFilter`-compatible string.
    pub fn as_filter_str(&self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}
