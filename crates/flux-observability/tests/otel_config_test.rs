// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Tests for OpenTelemetry configuration parsing and traceparent formatting.

use flux_observability::config::TracingConfig;

#[test]
fn tracing_config_defaults() {
    let config = TracingConfig::default();
    assert!(!config.enabled);
    assert_eq!(config.otlp_endpoint, "http://localhost:4317");
    assert!((config.sampling_ratio - 1.0).abs() < f64::EPSILON);
    assert_eq!(config.service_name, "horizon-flux");
    assert!(config.service_namespace.is_none());
}

#[test]
fn tracing_config_deserializes_from_toml() {
    let toml_str = r#"
enabled = true
otlp_endpoint = "http://otel-collector:4317"
sampling_ratio = 0.5
service_name = "my-flux"
service_namespace = "analytics"
"#;
    let config: TracingConfig = toml::from_str(toml_str).unwrap();
    assert!(config.enabled);
    assert_eq!(config.otlp_endpoint, "http://otel-collector:4317");
    assert!((config.sampling_ratio - 0.5).abs() < f64::EPSILON);
    assert_eq!(config.service_name, "my-flux");
    assert_eq!(config.service_namespace.as_deref(), Some("analytics"));
}

#[test]
fn tracing_config_partial_toml_uses_defaults() {
    let toml_str = r#"
enabled = true
"#;
    let config: TracingConfig = toml::from_str(toml_str).unwrap();
    assert!(config.enabled);
    assert_eq!(config.otlp_endpoint, "http://localhost:4317");
    assert!((config.sampling_ratio - 1.0).abs() < f64::EPSILON);
}

#[test]
fn current_traceparent_returns_none_without_otel() {
    // When OTel is not initialized, there is no valid span context.
    let tp = flux_observability::otel::current_traceparent();
    assert!(tp.is_none());
}

#[test]
fn build_provider_disabled_returns_none() {
    let config = TracingConfig::default(); // enabled = false
    let result = flux_observability::otel::build_provider(&config);
    assert!(result.is_none());
}
