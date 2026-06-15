// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! OpenTelemetry trace export via OTLP.
//!
//! Configures a `tracing-opentelemetry` layer that bridges the existing
//! `tracing` spans into OTel spans exported over gRPC. The caller receives
//! an [`OtelGuard`] that must be held until shutdown to flush pending spans.

use crate::config::TracingConfig;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::registry::LookupSpan;

/// Guard that owns the OTel tracer provider. Dropping it or calling
/// [`shutdown`] flushes pending spans to the collector.
pub struct OtelGuard {
    provider: SdkTracerProvider,
}

impl OtelGuard {
    /// Flush all pending spans and shut down the OTLP exporter.
    pub fn shutdown(self) {
        if let Err(e) = self.provider.shutdown() {
            tracing::warn!("Failed to shutdown OpenTelemetry tracer provider: {e}");
        }
    }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        // Best-effort flush on drop if the caller forgets to call shutdown().
        // The provider's own Drop will also attempt this, but we log failures.
        let _ = self.provider.force_flush();
    }
}

/// Build the OTel tracer provider from config. Returns `None` if tracing is
/// disabled or initialization fails.
pub fn build_provider(config: &TracingConfig) -> Option<(SdkTracerProvider, OtelGuard)> {
    if !config.enabled {
        return None;
    }

    match try_build_provider(config) {
        Ok((provider, guard)) => Some((provider, guard)),
        Err(e) => {
            // Log to stderr since the tracing subscriber may not be fully
            // initialized yet when this runs.
            eprintln!("[armillary-observability] Failed to initialize OpenTelemetry: {e}");
            None
        }
    }
}

/// Create an [`OpenTelemetryLayer`] from a provider. This is a convenience
/// helper used by [`crate::init_all`] to add the layer to a composed subscriber.
pub fn layer<S>(
    provider: &SdkTracerProvider,
) -> OpenTelemetryLayer<S, opentelemetry_sdk::trace::SdkTracer>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
{
    use opentelemetry::trace::TracerProvider as _;
    let tracer = provider.tracer("armillary");
    OpenTelemetryLayer::new(tracer)
}

fn try_build_provider(
    config: &TracingConfig,
) -> Result<(SdkTracerProvider, OtelGuard), Box<dyn std::error::Error + Send + Sync>> {
    use opentelemetry_otlp::SpanExporter;

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint)
        .build()?;

    let sampler = if config.sampling_ratio >= 1.0 {
        Sampler::AlwaysOn
    } else if config.sampling_ratio <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::TraceIdRatioBased(config.sampling_ratio)
    };

    let mut resource_attrs = vec![KeyValue::new("service.name", config.service_name.clone())];
    if let Some(ref ns) = config.service_namespace {
        resource_attrs.push(KeyValue::new("service.namespace", ns.clone()));
    }

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_resource(Resource::builder().with_attributes(resource_attrs).build())
        .build();

    let guard = OtelGuard {
        provider: provider.clone(),
    };

    Ok((provider, guard))
}

/// Extract the current W3C traceparent header value from the active span
/// context, suitable for propagating to plugin subprocesses via environment
/// variable.
///
/// Format: `00-{trace_id}-{span_id}-{flags}`
///
/// Returns `None` if there is no active span or OTel is not configured.
pub fn current_traceparent() -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let current_span = tracing::Span::current();
    let cx = current_span.context();
    let span_ref = cx.span();
    let sc = span_ref.span_context();

    if !sc.is_valid() {
        return None;
    }

    let flags = if sc.is_sampled() { "01" } else { "00" };
    Some(format!("00-{}-{}-{}", sc.trace_id(), sc.span_id(), flags))
}
