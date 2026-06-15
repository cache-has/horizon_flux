// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the structured JSON logging layer.

use std::io;
use std::sync::{Arc, Mutex};

use armillary_observability::config::{LogFormat, LogLevel, LoggingConfig};
use armillary_observability::events::*;
use serde_json::Value;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

/// Shared buffer writer for capturing log output.
#[derive(Clone)]
struct BufWriter(Arc<Mutex<Vec<u8>>>);

impl BufWriter {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn contents(&self) -> String {
        let buf = self.0.lock().unwrap();
        String::from_utf8_lossy(&buf).to_string()
    }
}

impl io::Write for BufWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Set up a subscriber with the JSON layer writing to a buffer.
/// Returns the buffer and a guard that must be held for the duration of the test.
fn setup_json_subscriber() -> (BufWriter, tracing::subscriber::DefaultGuard) {
    let buf = BufWriter::new();
    let layer = armillary_observability::json_format::FluxJsonLayer::new(buf.clone());
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    (buf, guard)
}

#[test]
fn general_log_event_produces_json_envelope() {
    let (buf, _guard) = setup_json_subscriber();

    tracing::info!("server started on port 8080");

    let output = buf.contents();
    let json: Value = serde_json::from_str(output.trim()).expect("valid JSON");
    let obj = json.as_object().expect("JSON object");

    assert!(obj.contains_key("ts"), "missing 'ts' field");
    assert_eq!(obj["level"], "info");
    assert!(obj.contains_key("component"), "missing 'component' field");
    assert_eq!(obj["event"], "log");
    assert_eq!(obj["message"], "server started on port 8080");
}

#[test]
fn general_log_with_fields_includes_payload() {
    let (buf, _guard) = setup_json_subscriber();

    tracing::info!(port = 8080, host = "0.0.0.0", "listening");

    let output = buf.contents();
    let json: Value = serde_json::from_str(output.trim()).expect("valid JSON");
    let obj = json.as_object().expect("JSON object");

    assert_eq!(obj["event"], "log");
    assert_eq!(obj["message"], "listening");
    let payload = obj["payload"].as_object().expect("payload object");
    assert_eq!(payload["port"], "8080");
    assert_eq!(payload["host"], "0.0.0.0");
}

#[test]
fn structured_flux_event_produces_documented_envelope() {
    let (buf, _guard) = setup_json_subscriber();

    let event = FluxEvent::PipelineRunStarted(PipelineRunStarted {
        pipeline_id: "orders_ingest".into(),
        run_id: "run-001".into(),
        environment: Some("prod".into()),
        triggered_by: "cron:6h".into(),
        variables: [("region".into(), "US".into())].into(),
    });
    armillary_observability::emit_event!(event);

    let output = buf.contents();
    let json: Value = serde_json::from_str(output.trim()).expect("valid JSON");
    let obj = json.as_object().expect("JSON object");

    assert!(obj.contains_key("ts"));
    assert_eq!(obj["level"], "info");
    assert_eq!(obj["event"], "pipeline_run_started");

    let payload = obj["payload"].as_object().expect("payload object");
    assert_eq!(payload["pipeline_id"], "orders_ingest");
    assert_eq!(payload["run_id"], "run-001");
    assert_eq!(payload["triggered_by"], "cron:6h");
}

#[test]
fn node_completed_event_serializes_correctly() {
    let (buf, _guard) = setup_json_subscriber();

    let event = FluxEvent::NodeCompleted(NodeCompleted {
        pipeline_id: "orders".into(),
        run_id: "run-002".into(),
        node_id: "clean".into(),
        duration_ms: 1234,
        rows: 50000,
        warnings: vec![],
    });
    armillary_observability::emit_event!(event);

    let output = buf.contents();
    let json: Value = serde_json::from_str(output.trim()).expect("valid JSON");
    let obj = json.as_object().expect("JSON object");

    assert_eq!(obj["event"], "node_completed");
    let payload = obj["payload"].as_object().expect("payload");
    assert_eq!(payload["node_id"], "clean");
    assert_eq!(payload["duration_ms"], 1234);
    assert_eq!(payload["rows"], 50000);
}

#[test]
fn config_deserializes_from_toml() {
    let toml_str = r#"
format = "json"
level = "debug"
"#;
    let config: LoggingConfig = toml::from_str(toml_str).expect("valid config");
    assert_eq!(config.format, LogFormat::Json);
    assert_eq!(config.level, LogLevel::Debug);
}

#[test]
fn config_defaults_to_pretty_info() {
    let config: LoggingConfig = toml::from_str("").expect("valid config");
    assert_eq!(config.format, LogFormat::Pretty);
    assert_eq!(config.level, LogLevel::Info);
}

#[test]
fn event_types_are_exhaustive() {
    // Verify all 14 documented event types have distinct type strings.
    let types = [
        FluxEvent::PipelineRunStarted(PipelineRunStarted {
            pipeline_id: String::new(),
            run_id: String::new(),
            environment: None,
            triggered_by: String::new(),
            variables: Default::default(),
        })
        .event_type(),
        FluxEvent::PipelineRunCompleted(PipelineRunCompleted {
            pipeline_id: String::new(),
            run_id: String::new(),
            environment: None,
            duration_ms: 0,
            rows_read: 0,
            rows_written: 0,
        })
        .event_type(),
        FluxEvent::PipelineRunFailed(PipelineRunFailed {
            pipeline_id: String::new(),
            run_id: String::new(),
            environment: None,
            failed_node_id: String::new(),
            error: String::new(),
            error_chain: vec![],
        })
        .event_type(),
        FluxEvent::NodeStarted(NodeStarted {
            pipeline_id: String::new(),
            run_id: String::new(),
            node_id: String::new(),
            node_kind: String::new(),
        })
        .event_type(),
        FluxEvent::NodeCompleted(NodeCompleted {
            pipeline_id: String::new(),
            run_id: String::new(),
            node_id: String::new(),
            duration_ms: 0,
            rows: 0,
            warnings: vec![],
        })
        .event_type(),
        FluxEvent::NodeFailed(NodeFailed {
            pipeline_id: String::new(),
            run_id: String::new(),
            node_id: String::new(),
            error: String::new(),
        })
        .event_type(),
        FluxEvent::SinkWriteCommitted(SinkWriteCommitted {
            pipeline_id: String::new(),
            run_id: String::new(),
            node_id: String::new(),
            fingerprint: String::new(),
            rows: 0,
            bytes: 0,
        })
        .event_type(),
        FluxEvent::SourceReadStarted(SourceReadStarted {
            pipeline_id: String::new(),
            run_id: String::new(),
            node_id: String::new(),
            fingerprint: String::new(),
            predicate_pushdown: false,
            projection_pushdown: false,
        })
        .event_type(),
        FluxEvent::IncrementalStateAdvanced(IncrementalStateAdvanced {
            pipeline_id: String::new(),
            run_id: String::new(),
            node_id: String::new(),
            receipt: serde_json::Value::Null,
        })
        .event_type(),
        FluxEvent::TriggerFired(TriggerFired {
            trigger_id: String::new(),
            kind: String::new(),
            pipeline_id: None,
            next_fire_at: None,
        })
        .event_type(),
        FluxEvent::PluginSpawned(PluginSpawned {
            plugin_name: String::new(),
            sink_type: String::new(),
            config_hash: String::new(),
        })
        .event_type(),
        FluxEvent::PluginCrashed(PluginCrashed {
            plugin_name: String::new(),
            exit_code: None,
            last_message: None,
        })
        .event_type(),
        FluxEvent::SchemaChangeDetected(SchemaChangeDetected {
            pipeline_id: String::new(),
            run_id: String::new(),
            node_id: String::new(),
            diff: serde_json::Value::Null,
        })
        .event_type(),
        FluxEvent::TestAssertionFailed(TestAssertionFailed {
            pipeline_id: String::new(),
            run_id: String::new(),
            node_id: String::new(),
            assertion: String::new(),
            violating_rows: 0,
        })
        .event_type(),
    ];

    assert_eq!(types.len(), 14);
    // All types should be unique.
    let mut unique = types.to_vec();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 14);
}
