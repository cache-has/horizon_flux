// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Error types for the scheduler crate.

use thiserror::Error;

/// Errors from trigger storage operations.
#[derive(Debug, Error)]
pub enum TriggerStoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("trigger not found: {0}")]
    NotFound(String),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Errors from scheduling operations.
#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("invalid cron expression: {0}")]
    InvalidCron(String),

    #[error("invalid interval: {0}")]
    InvalidInterval(String),

    #[error("storage error: {0}")]
    Store(#[from] TriggerStoreError),

    #[error("trigger disabled: {0}")]
    TriggerDisabled(String),

    #[error("run policy rejected: {0}")]
    RunPolicyRejected(String),

    #[error("queue full for trigger {0} (max depth {1})")]
    QueueFull(String, u32),

    #[error("sensor error: {0}")]
    Sensor(String),
}
