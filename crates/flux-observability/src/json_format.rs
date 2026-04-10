// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Custom JSON logging layer producing the documented flux event envelope.
//!
//! Every log line is a JSON object:
//!
//! ```json
//! {
//!   "ts": "2026-04-08T14:32:18.421Z",
//!   "level": "info",
//!   "component": "flux_datafusion::executor",
//!   "event": "pipeline_run_started",
//!   "pipeline_id": "orders_ingest",
//!   "run_id": "...",
//!   "payload": { ... }
//! }
//! ```
//!
//! For non-structured events (plain `info!("message")`), the output is a
//! simpler envelope with `event` = `"log"` and `message` in the body.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};

use chrono::Utc;
use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::span;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// A tracing layer that writes events as the documented flux JSON envelope.
pub struct FluxJsonLayer<W: Write + Send + 'static = io::Stdout> {
    writer: std::sync::Mutex<W>,
}

impl FluxJsonLayer<io::Stdout> {
    /// Create a new JSON layer writing to stdout.
    pub fn stdout() -> Self {
        Self {
            writer: std::sync::Mutex::new(io::stdout()),
        }
    }
}

impl<W: Write + Send + 'static> FluxJsonLayer<W> {
    /// Create a new JSON layer writing to the given writer.
    pub fn new(writer: W) -> Self {
        Self {
            writer: std::sync::Mutex::new(writer),
        }
    }
}

/// Fields stored on a span for propagation to child events.
#[derive(Default, Clone)]
struct SpanFields(BTreeMap<String, String>);

impl<S, W> tracing_subscriber::Layer<S> for FluxJsonLayer<W>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    W: Write + Send + 'static,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);

        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            extensions.insert(SpanFields(visitor.fields));
        }
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        values.record(&mut visitor);

        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            if let Some(fields) = extensions.get_mut::<SpanFields>() {
                fields.0.extend(visitor.fields);
            }
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        let mut envelope = Map::new();

        // ts
        envelope.insert(
            "ts".into(),
            Value::String(Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()),
        );

        // level
        envelope.insert(
            "level".into(),
            Value::String(event.metadata().level().to_string().to_lowercase()),
        );

        // component — tracing target
        envelope.insert(
            "component".into(),
            Value::String(event.metadata().target().to_string()),
        );

        // Collect span context fields (pipeline_id, run_id, etc.).
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                let extensions = span.extensions();
                if let Some(fields) = extensions.get::<SpanFields>() {
                    for (k, v) in &fields.0 {
                        if !envelope.contains_key(k) {
                            envelope.insert(k.clone(), Value::String(v.clone()));
                        }
                    }
                }
            }
        }

        // Check if this is a structured flux event.
        if let Some(flux_event_json) = visitor.fields.remove("flux_event") {
            // Parse the serialized FluxEvent and merge into envelope.
            if let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(&flux_event_json) {
                for (k, v) in obj {
                    envelope.insert(k, v);
                }
            }
            // Remove the internal marker field.
            visitor.fields.remove("flux_event_type");
            visitor.fields.remove("message");
        } else {
            // General log event.
            let message = visitor.fields.remove("message").unwrap_or_default();
            envelope.insert("event".into(), Value::String("log".into()));
            envelope.insert("message".into(), Value::String(message));

            // Remaining fields become payload.
            if !visitor.fields.is_empty() {
                let mut payload = Map::new();
                for (k, v) in &visitor.fields {
                    payload.insert(k.clone(), Value::String(v.clone()));
                }
                envelope.insert("payload".into(), Value::Object(payload));
            }
        }

        let json = Value::Object(envelope);
        if let Ok(mut w) = self.writer.lock() {
            let _ = writeln!(w, "{json}");
        }
    }
}

/// Visitor that collects fields into a map.
#[derive(Default)]
struct FieldVisitor {
    fields: BTreeMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
}
