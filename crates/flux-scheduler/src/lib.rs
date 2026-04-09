// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Scheduling and trigger engine for Horizon Flux.
//!
//! Provides time-based (cron, interval) and event-driven (file arrival, webhook,
//! pipeline completion) trigger evaluation with pluggable storage and clock for
//! testability. The scheduler runs in-process with the Flux server — no separate
//! daemon required.

pub mod cron;
pub mod error;
pub mod interval;
pub mod scheduler;
pub mod sensors;
pub mod store;
pub mod types;
pub mod variable_mapping;

pub use error::{SchedulerError, TriggerStoreError};
pub use scheduler::{Clock, RunDispatcher, Scheduler, SystemClock, TickResult, run_scheduler_loop};
pub use store::{SqliteTriggerStore, TriggerStorage};
pub use types::{
    CompletionStatus, RunPolicy, Trigger, TriggerHistoryEntry, TriggerId, TriggerKind,
    TriggerOutcome, TriggerState,
};
pub use variable_mapping::{
    CompletionContext, FileArrivalContext, WebhookContext, evaluate_completion,
    evaluate_file_arrival, evaluate_webhook, merge_variables, validate_mapping_keys,
};
