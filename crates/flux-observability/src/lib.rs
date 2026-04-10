// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Observability infrastructure for Horizon Flux.
//!
//! Provides structured JSON logging, Prometheus metrics, and OpenTelemetry
//! trace export, designed to integrate with external observability stacks
//! (Grafana, Datadog, Honeycomb, Jaeger, etc.).

pub mod config;
pub mod events;
pub mod json_format;
pub mod metrics;
pub mod openlineage;
pub mod otel;

use config::{LogFormat, LoggingConfig, MetricsConfig, TracingConfig};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

pub use otel::OtelGuard;

/// Initialize the global tracing subscriber based on the provided config.
///
/// This should be called once at application startup, replacing any ad-hoc
/// `tracing_subscriber::fmt().init()` calls.
///
/// If `config` is `None`, defaults are used (pretty format, info level, stdout).
pub fn init(config: Option<&LoggingConfig>) {
    let _ = init_all(config, None, None);
}

/// Initialize both the tracing subscriber and the Prometheus metrics recorder.
///
/// If `metrics_config` is `None` or metrics are disabled, only logging is set up.
/// Metrics initialization failure is logged but does not prevent startup.
pub fn init_with_metrics(
    logging_config: Option<&LoggingConfig>,
    metrics_config: Option<&MetricsConfig>,
) {
    let _ = init_all(logging_config, metrics_config, None);
}

/// Initialize logging, Prometheus metrics, and OpenTelemetry trace export.
///
/// Returns an [`OtelGuard`] if OTel was successfully configured. The caller
/// must hold this guard and call [`OtelGuard::shutdown`] before exit to flush
/// pending spans. If the guard is dropped without calling shutdown, a
/// best-effort flush is attempted.
pub fn init_all(
    logging_config: Option<&LoggingConfig>,
    metrics_config: Option<&MetricsConfig>,
    tracing_config: Option<&TracingConfig>,
) -> Option<OtelGuard> {
    let default_config = LoggingConfig::default();
    let config = logging_config.unwrap_or(&default_config);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.level.as_filter_str()));

    let otel_result = tracing_config.and_then(otel::build_provider);

    let guard = match (config.format, otel_result) {
        (LogFormat::Json, Some((provider, guard))) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(json_format::FluxJsonLayer::stdout())
                .with(otel::layer(&provider))
                .init();
            Some(guard)
        }
        (LogFormat::Json, None) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(json_format::FluxJsonLayer::stdout())
                .init();
            None
        }
        (LogFormat::Pretty, Some((provider, guard))) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer())
                .with(otel::layer(&provider))
                .init();
            Some(guard)
        }
        (LogFormat::Pretty, None) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer())
                .init();
            None
        }
    };

    // Install Prometheus metrics recorder if configured.
    if let Some(metrics_cfg) = metrics_config {
        if let Err(e) = metrics::init(metrics_cfg) {
            tracing::warn!("Failed to install Prometheus metrics recorder: {e}");
        }
    }

    guard
}

/// Initialize logging to a writer (for testing).
#[doc(hidden)]
pub fn init_with_writer<W: std::io::Write + Send + 'static>(
    config: Option<&LoggingConfig>,
    writer: W,
) {
    let default_config = LoggingConfig::default();
    let config = config.unwrap_or(&default_config);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.level.as_filter_str()));

    match config.format {
        LogFormat::Json => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(json_format::FluxJsonLayer::new(writer))
                .init();
        }
        LogFormat::Pretty => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer())
                .init();
        }
    }
}
