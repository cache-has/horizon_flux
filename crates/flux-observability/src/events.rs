// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Stable, versioned flux event types with structured payloads.
//!
//! Each event type has a documented payload shape. These types are the
//! canonical schema — log consumers should rely on the field names and types
//! defined here.

use serde::Serialize;
use std::collections::HashMap;

/// Emit a structured flux event as a tracing event.
///
/// This produces a `tracing::info!` event with the `flux_event` field set to
/// the serialized JSON envelope. The JSON format layer picks this up and
/// writes it in the documented envelope format.
#[macro_export]
macro_rules! emit_event {
    ($event:expr) => {
        let serialized = serde_json::to_string(&$event).unwrap_or_default();
        tracing::info!(
            flux_event = %serialized,
            flux_event_type = $event.event_type(),
            "{}",
            $event.event_type()
        );
    };
}

/// All documented flux event types.
///
/// Breaking changes to these shapes require a major version bump.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", content = "payload")]
#[serde(rename_all = "snake_case")]
pub enum FluxEvent {
    PipelineRunStarted(PipelineRunStarted),
    PipelineRunCompleted(PipelineRunCompleted),
    PipelineRunFailed(PipelineRunFailed),
    NodeStarted(NodeStarted),
    NodeCompleted(NodeCompleted),
    NodeFailed(NodeFailed),
    SinkWriteCommitted(SinkWriteCommitted),
    SourceReadStarted(SourceReadStarted),
    IncrementalStateAdvanced(IncrementalStateAdvanced),
    TriggerFired(TriggerFired),
    PluginSpawned(PluginSpawned),
    PluginCrashed(PluginCrashed),
    SchemaChangeDetected(SchemaChangeDetected),
    TestAssertionFailed(TestAssertionFailed),
}

impl FluxEvent {
    /// The stable event type string used in the JSON envelope.
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::PipelineRunStarted(_) => "pipeline_run_started",
            Self::PipelineRunCompleted(_) => "pipeline_run_completed",
            Self::PipelineRunFailed(_) => "pipeline_run_failed",
            Self::NodeStarted(_) => "node_started",
            Self::NodeCompleted(_) => "node_completed",
            Self::NodeFailed(_) => "node_failed",
            Self::SinkWriteCommitted(_) => "sink_write_committed",
            Self::SourceReadStarted(_) => "source_read_started",
            Self::IncrementalStateAdvanced(_) => "incremental_state_advanced",
            Self::TriggerFired(_) => "trigger_fired",
            Self::PluginSpawned(_) => "plugin_spawned",
            Self::PluginCrashed(_) => "plugin_crashed",
            Self::SchemaChangeDetected(_) => "schema_change_detected",
            Self::TestAssertionFailed(_) => "test_assertion_failed",
        }
    }

    /// Correlation keys present on this event.
    pub fn correlation(&self) -> EventCorrelation<'_> {
        match self {
            Self::PipelineRunStarted(e) => EventCorrelation {
                pipeline_id: Some(&e.pipeline_id),
                environment: e.environment.as_deref(),
                run_id: Some(&e.run_id),
                node_id: None,
            },
            Self::PipelineRunCompleted(e) => EventCorrelation {
                pipeline_id: Some(&e.pipeline_id),
                environment: e.environment.as_deref(),
                run_id: Some(&e.run_id),
                node_id: None,
            },
            Self::PipelineRunFailed(e) => EventCorrelation {
                pipeline_id: Some(&e.pipeline_id),
                environment: e.environment.as_deref(),
                run_id: Some(&e.run_id),
                node_id: Some(&e.failed_node_id),
            },
            Self::NodeStarted(e) => EventCorrelation {
                pipeline_id: Some(&e.pipeline_id),
                environment: None,
                run_id: Some(&e.run_id),
                node_id: Some(&e.node_id),
            },
            Self::NodeCompleted(e) => EventCorrelation {
                pipeline_id: Some(&e.pipeline_id),
                environment: None,
                run_id: Some(&e.run_id),
                node_id: Some(&e.node_id),
            },
            Self::NodeFailed(e) => EventCorrelation {
                pipeline_id: Some(&e.pipeline_id),
                environment: None,
                run_id: Some(&e.run_id),
                node_id: Some(&e.node_id),
            },
            Self::SinkWriteCommitted(e) => EventCorrelation {
                pipeline_id: Some(&e.pipeline_id),
                environment: None,
                run_id: Some(&e.run_id),
                node_id: Some(&e.node_id),
            },
            Self::SourceReadStarted(e) => EventCorrelation {
                pipeline_id: Some(&e.pipeline_id),
                environment: None,
                run_id: Some(&e.run_id),
                node_id: Some(&e.node_id),
            },
            Self::IncrementalStateAdvanced(e) => EventCorrelation {
                pipeline_id: Some(&e.pipeline_id),
                environment: None,
                run_id: Some(&e.run_id),
                node_id: Some(&e.node_id),
            },
            Self::TriggerFired(e) => EventCorrelation {
                pipeline_id: e.pipeline_id.as_deref(),
                environment: None,
                run_id: None,
                node_id: None,
            },
            Self::PluginSpawned(_) => EventCorrelation {
                pipeline_id: None,
                environment: None,
                run_id: None,
                node_id: None,
            },
            Self::PluginCrashed(_) => EventCorrelation {
                pipeline_id: None,
                environment: None,
                run_id: None,
                node_id: None,
            },
            Self::SchemaChangeDetected(e) => EventCorrelation {
                pipeline_id: Some(&e.pipeline_id),
                environment: None,
                run_id: Some(&e.run_id),
                node_id: Some(&e.node_id),
            },
            Self::TestAssertionFailed(e) => EventCorrelation {
                pipeline_id: Some(&e.pipeline_id),
                environment: None,
                run_id: Some(&e.run_id),
                node_id: Some(&e.node_id),
            },
        }
    }
}

/// Correlation keys extracted from an event for the JSON envelope.
pub struct EventCorrelation<'a> {
    pub pipeline_id: Option<&'a str>,
    pub environment: Option<&'a str>,
    pub run_id: Option<&'a str>,
    pub node_id: Option<&'a str>,
}

// ── Payload types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct PipelineRunStarted {
    pub pipeline_id: String,
    pub run_id: String,
    pub environment: Option<String>,
    pub triggered_by: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub variables: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineRunCompleted {
    pub pipeline_id: String,
    pub run_id: String,
    pub environment: Option<String>,
    pub duration_ms: u64,
    pub rows_read: u64,
    pub rows_written: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineRunFailed {
    pub pipeline_id: String,
    pub run_id: String,
    pub environment: Option<String>,
    pub failed_node_id: String,
    pub error: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub error_chain: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeStarted {
    pub pipeline_id: String,
    pub run_id: String,
    pub node_id: String,
    pub node_kind: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeCompleted {
    pub pipeline_id: String,
    pub run_id: String,
    pub node_id: String,
    pub duration_ms: u64,
    pub rows: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeFailed {
    pub pipeline_id: String,
    pub run_id: String,
    pub node_id: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SinkWriteCommitted {
    pub pipeline_id: String,
    pub run_id: String,
    pub node_id: String,
    pub fingerprint: String,
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceReadStarted {
    pub pipeline_id: String,
    pub run_id: String,
    pub node_id: String,
    pub fingerprint: String,
    pub predicate_pushdown: bool,
    pub projection_pushdown: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct IncrementalStateAdvanced {
    pub pipeline_id: String,
    pub run_id: String,
    pub node_id: String,
    /// Serialized `MaterializationReceipt` from flux-datafusion.
    pub receipt: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct TriggerFired {
    pub trigger_id: String,
    pub kind: String,
    pub pipeline_id: Option<String>,
    pub next_fire_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PluginSpawned {
    pub plugin_name: String,
    pub sink_type: String,
    pub config_hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PluginCrashed {
    pub plugin_name: String,
    pub exit_code: Option<i32>,
    pub last_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchemaChangeDetected {
    pub pipeline_id: String,
    pub run_id: String,
    pub node_id: String,
    pub diff: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct TestAssertionFailed {
    pub pipeline_id: String,
    pub run_id: String,
    pub node_id: String,
    pub assertion: String,
    pub violating_rows: u64,
}
