// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline run tracking types.
//!
//! These types use wall-clock [`std::time::SystemTime`] (not [`std::time::Instant`])
//! so they can be serialized and persisted to SQLite.

use crate::provider::MaterializationReceipt;
use flux_engine::NodeId;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

/// Real-time execution event emitted by the executor as nodes progress.
///
/// These events are sent over an optional progress channel so that the
/// server layer can broadcast them to WebSocket clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionEvent {
    RunStarted {
        run_id: RunId,
        pipeline_name: String,
    },
    NodeStarted {
        run_id: RunId,
        node_id: NodeId,
    },
    NodeCompleted {
        run_id: RunId,
        node_id: NodeId,
        rows_out: u64,
        duration_ms: u64,
        /// Materialization receipt for sink nodes (doc 27). `None` for
        /// source/transform nodes; populated for every successful sink write.
        /// Boxed to keep `ExecutionEvent` small (clippy::large_enum_variant).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        materialization_receipt: Option<Box<MaterializationReceipt>>,
    },
    NodeFailed {
        run_id: RunId,
        node_id: NodeId,
        error: String,
    },
    RunCompleted {
        run_id: RunId,
        status: RunStatus,
        duration_ms: u64,
    },
}

/// Unique identifier for a pipeline run.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(pub Uuid);

impl RunId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Status of a pipeline run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Success,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "success" => Some(Self::Success),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// Per-node execution statistics with wall-clock times for persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRunStats {
    pub node_id: NodeId,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub rows_in: u64,
    pub rows_out: u64,
    pub error: Option<String>,
    /// Sink-only: structured materialization receipt (doc 27). `None` for
    /// source/transform nodes and for sinks that haven't yet been re-run
    /// since this field was introduced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization_receipt: Option<MaterializationReceipt>,
}

impl NodeRunStats {
    pub fn duration_ms(&self) -> u64 {
        self.end_time
            .duration_since(self.start_time)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

/// A complete record of a single pipeline execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRun {
    pub id: RunId,
    pub pipeline_name: String,
    pub environment: String,
    pub status: RunStatus,
    pub start_time: Option<SystemTime>,
    pub end_time: Option<SystemTime>,
    pub node_stats: Vec<NodeRunStats>,
    pub error: Option<String>,
}

impl PipelineRun {
    /// Create a new run in `Pending` status.
    pub fn new(pipeline_name: impl Into<String>, environment: impl Into<String>) -> Self {
        Self {
            id: RunId::new(),
            pipeline_name: pipeline_name.into(),
            environment: environment.into(),
            status: RunStatus::Pending,
            start_time: None,
            end_time: None,
            node_stats: Vec::new(),
            error: None,
        }
    }

    pub fn duration_ms(&self) -> Option<u64> {
        match (self.start_time, self.end_time) {
            (Some(start), Some(end)) => {
                Some(end.duration_since(start).unwrap_or_default().as_millis() as u64)
            }
            _ => None,
        }
    }
}
