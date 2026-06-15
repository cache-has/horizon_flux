// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Prometheus metrics for Armillary.
//!
//! Uses the `metrics` facade with a Prometheus exporter. All metric recording
//! functions are safe to call even if the exporter is not installed — the
//! `metrics` crate silently drops recordings when no recorder is set.

use crate::config::MetricsConfig;
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

/// Global handle for the Prometheus exporter. Used by the `/metrics` endpoint
/// to render the exposition format.
static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Global label allow-list for cardinality control. When set, only labels in
/// this list are included in metric recordings.
static LABEL_ALLOW_LIST: OnceLock<Option<Vec<String>>> = OnceLock::new();

/// Install the Prometheus metrics recorder.
///
/// Returns `Ok(())` if the recorder was installed (or was already installed).
/// Returns `Err` if installation fails.
pub fn init(config: &MetricsConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !config.enabled {
        return Ok(());
    }

    let handle = PrometheusBuilder::new().install_recorder()?;
    let _ = PROMETHEUS_HANDLE.set(handle);
    let _ = LABEL_ALLOW_LIST.set(config.include_labels.clone());
    Ok(())
}

/// Render the current metrics in Prometheus exposition format.
///
/// Returns `None` if the exporter was not installed.
pub fn render() -> Option<String> {
    PROMETHEUS_HANDLE.get().map(|h| h.render())
}

/// Check whether a label is allowed by the configured allow-list.
///
/// Returns `true` if no allow-list is configured (all labels allowed) or if
/// the label is in the list.
fn label_allowed(label: &str) -> bool {
    match LABEL_ALLOW_LIST.get() {
        Some(Some(list)) => list.iter().any(|l| l == label),
        _ => true,
    }
}

/// Build a label vector respecting the allow-list.
macro_rules! labels {
    ($($key:expr => $val:expr),+ $(,)?) => {{
        let mut pairs: Vec<(&str, String)> = Vec::new();
        $(
            if label_allowed($key) {
                pairs.push(($key, $val.to_string()));
            }
        )+
        pairs
    }};
}

// ── Pipeline metrics ─────────────────────────────────────────────────────

/// Record a pipeline run start/completion/failure.
pub fn record_pipeline_run(pipeline: &str, environment: &str, status: &str) {
    let labels = labels!("pipeline" => pipeline, "environment" => environment, "status" => status);
    counter!("armillary_pipeline_runs_total", &labels).increment(1);
}

/// Record pipeline run duration.
pub fn record_pipeline_duration(pipeline: &str, environment: &str, duration_secs: f64) {
    let labels = labels!("pipeline" => pipeline, "environment" => environment);
    histogram!("armillary_pipeline_run_duration_seconds", &labels).record(duration_secs);
}

/// Record rows read by a pipeline.
pub fn record_pipeline_rows_read(pipeline: &str, environment: &str, rows: u64) {
    let labels = labels!("pipeline" => pipeline, "environment" => environment);
    counter!("armillary_pipeline_rows_read_total", &labels).increment(rows);
}

/// Record rows written by a pipeline.
pub fn record_pipeline_rows_written(pipeline: &str, environment: &str, rows: u64) {
    let labels = labels!("pipeline" => pipeline, "environment" => environment);
    counter!("armillary_pipeline_rows_written_total", &labels).increment(rows);
}

/// Record the timestamp of the last successful pipeline run.
pub fn record_pipeline_last_success(pipeline: &str, environment: &str, timestamp_secs: f64) {
    let labels = labels!("pipeline" => pipeline, "environment" => environment);
    gauge!("armillary_pipeline_last_success_timestamp_seconds", &labels).set(timestamp_secs);
}

// ── Node metrics ─────────────────────────────────────────────────────────

/// Record a node execution.
pub fn record_node_execution(pipeline: &str, node_id: &str, kind: &str, status: &str) {
    let labels = labels!(
        "pipeline" => pipeline,
        "node_id" => node_id,
        "kind" => kind,
        "status" => status,
    );
    counter!("armillary_node_executions_total", &labels).increment(1);
}

/// Record node execution duration.
pub fn record_node_duration(pipeline: &str, node_id: &str, kind: &str, duration_secs: f64) {
    let labels = labels!("pipeline" => pipeline, "node_id" => node_id, "kind" => kind);
    histogram!("armillary_node_duration_seconds", &labels).record(duration_secs);
}

/// Record node rows (read or written).
pub fn record_node_rows(pipeline: &str, node_id: &str, direction: &str, rows: f64) {
    let labels = labels!("pipeline" => pipeline, "node_id" => node_id, "direction" => direction);
    histogram!("armillary_node_rows", &labels).record(rows);
}

/// Record a schema change detection.
pub fn record_schema_change(pipeline: &str, node_id: &str) {
    let labels = labels!("pipeline" => pipeline, "node_id" => node_id);
    counter!("armillary_node_schema_changes_total", &labels).increment(1);
}

// ── Trigger metrics ──────────────────────────────────────────────────────

/// Record a trigger firing.
pub fn record_trigger_firing(trigger_id: &str, kind: &str, outcome: &str) {
    let labels = labels!("trigger_id" => trigger_id, "kind" => kind, "outcome" => outcome);
    counter!("armillary_trigger_firings_total", &labels).increment(1);
}

/// Record the last-fired timestamp for a trigger.
pub fn record_trigger_last_fired(trigger_id: &str, timestamp_secs: f64) {
    let labels = labels!("trigger_id" => trigger_id);
    gauge!("armillary_trigger_last_fired_timestamp_seconds", &labels).set(timestamp_secs);
}

/// Set the consecutive error count for a trigger.
pub fn record_trigger_consecutive_errors(trigger_id: &str, count: f64) {
    let labels = labels!("trigger_id" => trigger_id);
    gauge!("armillary_trigger_consecutive_errors", &labels).set(count);
}

// ── Plugin metrics ───────────────────────────────────────────────────────

/// Record a plugin spawn.
pub fn record_plugin_spawn(plugin: &str, sink_type: &str, outcome: &str) {
    let labels = labels!("plugin" => plugin, "sink_type" => sink_type, "outcome" => outcome);
    counter!("armillary_plugin_spawns_total", &labels).increment(1);
}

/// Record plugin write duration.
pub fn record_plugin_write_duration(plugin: &str, sink_type: &str, duration_secs: f64) {
    let labels = labels!("plugin" => plugin, "sink_type" => sink_type);
    histogram!("armillary_plugin_write_duration_seconds", &labels).record(duration_secs);
}

/// Record a plugin crash.
pub fn record_plugin_crash(plugin: &str) {
    let labels = labels!("plugin" => plugin);
    counter!("armillary_plugin_crashes_total", &labels).increment(1);
}

// ── Test metrics ─────────────────────────────────────────────────────────

/// Record a test assertion result.
pub fn record_test_assertion(pipeline: &str, node_id: &str, kind: &str, status: &str) {
    let labels = labels!(
        "pipeline" => pipeline,
        "node_id" => node_id,
        "kind" => kind,
        "status" => status,
    );
    counter!("armillary_test_assertions_total", &labels).increment(1);
}

// ── SLA metrics ─────────────────────────────────────────────────────────

/// Set the SLA status for a resource. Encodes status as a numeric gauge:
/// 0 = ok, 1 = warning, 2 = breach, 3 = unknown.
pub fn record_sla_status(resource: &str, severity: &str) {
    let value = match severity {
        "ok" => 0.0,
        "warning" => 1.0,
        "breach" => 2.0,
        _ => 3.0,
    };
    let labels = labels!("resource" => resource, "severity" => severity);
    gauge!("armillary_sla_status", &labels).set(value);
}

// ── System metrics ───────────────────────────────────────────────────────

/// Increment the scheduler tick counter.
pub fn record_scheduler_tick() {
    counter!("armillary_scheduler_ticks_total").increment(1);
}

/// Set the number of active pipeline runs.
pub fn set_active_runs(environment: &str, count: f64) {
    let labels = labels!("environment" => environment);
    gauge!("armillary_active_runs", &labels).set(count);
}

/// Set the number of queued pipeline runs.
pub fn set_queued_runs(environment: &str, count: f64) {
    let labels = labels!("environment" => environment);
    gauge!("armillary_queued_runs", &labels).set(count);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_without_exporter_is_safe() {
        // When no recorder is installed, all recording calls are no-ops.
        record_pipeline_run("test_pipeline", "dev", "completed");
        record_pipeline_duration("test_pipeline", "dev", 1.5);
        record_pipeline_rows_read("test_pipeline", "dev", 100);
        record_pipeline_rows_written("test_pipeline", "dev", 50);
        record_pipeline_last_success("test_pipeline", "dev", 1712000000.0);
        record_node_execution("test_pipeline", "node1", "source", "completed");
        record_node_duration("test_pipeline", "node1", "source", 0.5);
        record_node_rows("test_pipeline", "node1", "written", 42.0);
        record_schema_change("test_pipeline", "node1");
        record_trigger_firing("t1", "cron:6h", "run_started");
        record_trigger_last_fired("t1", 1712000000.0);
        record_trigger_consecutive_errors("t1", 0.0);
        record_plugin_spawn("parquet", "parquet", "ok");
        record_plugin_write_duration("parquet", "parquet", 0.3);
        record_plugin_crash("parquet");
        record_test_assertion("test_pipeline", "node1", "assertion", "passed");
        record_sla_status("postgres://h:5432/db/public.orders", "ok");
        record_scheduler_tick();
        set_active_runs("dev", 1.0);
        set_queued_runs("dev", 0.0);
        // If we get here without panicking, the no-op path works.
    }

    #[test]
    fn render_returns_none_without_exporter() {
        assert!(render().is_none());
    }

    #[test]
    fn label_allowed_no_list() {
        // Without any allow-list, all labels should be allowed.
        // Note: this test may fail if LABEL_ALLOW_LIST was set by a prior
        // test in the same process. The OnceLock is process-global.
        // In practice this is fine because tests run in isolation.
        assert!(label_allowed("pipeline"));
        assert!(label_allowed("anything"));
    }

    #[test]
    fn metrics_config_default() {
        let config = MetricsConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.path, "/metrics");
        assert!(config.include_labels.is_none());
    }

    #[test]
    fn metrics_config_deserialize() {
        let toml_str = r#"
enabled = true
path = "/custom-metrics"
include_labels = ["pipeline", "environment"]
"#;
        let config: MetricsConfig = toml::from_str(toml_str).unwrap();
        assert!(config.enabled);
        assert_eq!(config.path, "/custom-metrics");
        assert_eq!(
            config.include_labels,
            Some(vec!["pipeline".to_string(), "environment".to_string()])
        );
    }
}
