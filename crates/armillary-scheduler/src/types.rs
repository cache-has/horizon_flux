// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Core domain types for scheduling and triggers.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

/// Unique identifier for a trigger.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TriggerId(pub Uuid);

impl TriggerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TriggerId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TriggerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TriggerId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

/// A trigger is a declarative description of "when to run this pipeline."
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trigger {
    pub id: TriggerId,
    pub name: String,
    pub pipeline_id: String,
    pub environment: String,
    pub enabled: bool,
    pub kind: TriggerKind,
    pub run_policy: RunPolicy,
    /// Optional variable overrides injected into the pipeline run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variable_overrides: Option<HashMap<String, serde_json::Value>>,
    /// Maximum queued runs when run_policy is Queue.
    #[serde(default = "default_max_queue_depth")]
    pub max_queue_depth: u32,
    pub created_at: String,
    pub updated_at: String,
}

fn default_max_queue_depth() -> u32 {
    3
}

/// The kind of trigger with its kind-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TriggerKind {
    /// Standard 5-field cron expression with timezone.
    Cron {
        expression: String,
        #[serde(default = "default_timezone")]
        timezone: String,
    },
    /// ISO 8601 duration interval.
    Interval {
        every: String,
        #[serde(default)]
        start_at: Option<String>,
    },
    /// Watch for new files matching a path/glob pattern.
    FileArrival {
        path: String,
        #[serde(default = "default_poll_interval")]
        poll_interval: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        variable_mapping: Option<HashMap<String, String>>,
    },
    /// HTTP endpoint that triggers a run when POSTed to.
    Webhook {
        path: String,
        #[serde(default = "default_auth")]
        auth: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        variable_mapping: Option<HashMap<String, String>>,
    },
    /// Fire when another pipeline completes.
    PipelineCompletion {
        upstream_pipeline: String,
        #[serde(default)]
        environment: Option<String>,
        #[serde(default = "default_on_status")]
        on_status: CompletionStatus,
    },
}

fn default_timezone() -> String {
    "UTC".to_string()
}

fn default_poll_interval() -> String {
    "PT1M".to_string()
}

fn default_auth() -> String {
    "token".to_string()
}

fn default_on_status() -> CompletionStatus {
    CompletionStatus::Success
}

/// Which pipeline completion status triggers a firing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompletionStatus {
    Success,
    Failure,
    Any,
}

/// What to do when a trigger fires and the pipeline is already running.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunPolicy {
    /// Enqueue the new run; it starts when the current run finishes.
    #[default]
    Queue,
    /// Silently skip this firing.
    Skip,
    /// Fail the firing loudly.
    Reject,
}

impl fmt::Display for RunPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queue => write!(f, "queue"),
            Self::Skip => write!(f, "skip"),
            Self::Reject => write!(f, "reject"),
        }
    }
}

impl FromStr for RunPolicy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "queue" => Ok(Self::Queue),
            "skip" => Ok(Self::Skip),
            "reject" => Ok(Self::Reject),
            other => Err(format!("unknown run policy: {other}")),
        }
    }
}

/// Mutable state tracked per trigger between evaluations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerState {
    pub trigger_id: TriggerId,
    pub last_evaluated_at: Option<String>,
    pub last_fired_at: Option<String>,
    /// Pre-computed next fire time for cron/interval triggers.
    pub next_fire_at: Option<String>,
    /// Kind-specific state (e.g., seen files for file_arrival sensor).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sensor_state: Option<serde_json::Value>,
    pub consecutive_errors: u32,
}

/// The outcome of a trigger evaluation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TriggerOutcome {
    /// A pipeline run was started.
    RunStarted,
    /// The run was queued behind an in-progress run.
    Queued,
    /// The firing was silently skipped (run_policy = skip).
    Skipped,
    /// The firing was rejected (run_policy = reject).
    Rejected,
    /// An error occurred evaluating or firing the trigger.
    Error,
}

impl fmt::Display for TriggerOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RunStarted => write!(f, "run_started"),
            Self::Queued => write!(f, "queued"),
            Self::Skipped => write!(f, "skipped"),
            Self::Rejected => write!(f, "rejected"),
            Self::Error => write!(f, "error"),
        }
    }
}

impl FromStr for TriggerOutcome {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "run_started" => Ok(Self::RunStarted),
            "queued" => Ok(Self::Queued),
            "skipped" => Ok(Self::Skipped),
            "rejected" => Ok(Self::Rejected),
            "error" => Ok(Self::Error),
            other => Err(format!("unknown trigger outcome: {other}")),
        }
    }
}

/// A record of a single trigger firing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerHistoryEntry {
    pub id: String,
    pub trigger_id: TriggerId,
    pub fired_at: String,
    pub outcome: TriggerOutcome,
    /// The run ID if a run was started or queued.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Additional details about the firing (e.g., matched file path).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    /// Error message if outcome is Error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
