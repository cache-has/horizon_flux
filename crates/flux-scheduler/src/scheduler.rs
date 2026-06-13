// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Scheduler tick loop that evaluates triggers and fires pipeline runs.

use crate::cron::CronSchedule;
use crate::error::SchedulerError;
use crate::interval::Iso8601Duration;
use crate::sensors::file_arrival::{FileArrivalState, detect_new_local_files};
use crate::store::TriggerStorage;
use crate::types::{
    CompletionStatus, RunPolicy, Trigger, TriggerHistoryEntry, TriggerKind, TriggerOutcome,
    TriggerState,
};
use crate::variable_mapping::{self, FileArrivalContext, WebhookContext};
use chrono::{DateTime, Utc};
use flux_observability::emit_event;
use flux_observability::events as obs;
use flux_observability::metrics as prom;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Pluggable clock for testability.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// Real wall-clock implementation.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Callback invoked when the scheduler decides a pipeline run should be started.
///
/// The implementor is responsible for actually executing the pipeline (or
/// enqueuing it). Returns the run ID on success, or an error message.
pub trait RunDispatcher: Send + Sync {
    fn dispatch_run(
        &self,
        pipeline_id: &str,
        environment: &str,
        variables: Option<&std::collections::HashMap<String, serde_json::Value>>,
        trigger_id: &str,
    ) -> Result<String, String>;

    /// Check if the pipeline already has an active (running) run in the
    /// given environment. Used for run-policy enforcement.
    fn is_pipeline_running(&self, pipeline_id: &str, environment: &str) -> bool;
}

/// The scheduler evaluates triggers on each tick and fires runs.
pub struct Scheduler {
    store: Arc<dyn TriggerStorage>,
    clock: Arc<dyn Clock>,
    dispatcher: Arc<dyn RunDispatcher>,
}

impl Scheduler {
    pub fn new(
        store: Arc<dyn TriggerStorage>,
        clock: Arc<dyn Clock>,
        dispatcher: Arc<dyn RunDispatcher>,
    ) -> Self {
        Self {
            store,
            clock,
            dispatcher,
        }
    }

    /// Run one evaluation tick: load all enabled triggers, check which should
    /// fire, and dispatch runs for those that match.
    pub fn tick(&self) -> Result<Vec<TickResult>, SchedulerError> {
        prom::record_scheduler_tick();
        let now = self.clock.now();
        let now_str = now.to_rfc3339();
        let triggers = self.store.list_enabled_triggers()?;
        let mut results = Vec::new();

        for trigger in &triggers {
            match self.evaluate_trigger(trigger, now, &now_str) {
                Ok(Some(result)) => results.push(result),
                Ok(None) => {} // Not time to fire yet.
                Err(e) => {
                    error!(trigger_id = %trigger.id, "trigger evaluation error: {e}");
                    self.record_error(trigger, &now_str, &e.to_string());
                    results.push(TickResult {
                        trigger_id: trigger.id.to_string(),
                        outcome: TriggerOutcome::Error,
                        run_id: None,
                        error: Some(e.to_string()),
                    });
                }
            }
        }

        Ok(results)
    }

    /// Manually fire a trigger, ignoring schedule but respecting run policy.
    pub fn manual_fire(
        &self,
        trigger_id: &crate::types::TriggerId,
    ) -> Result<TickResult, SchedulerError> {
        let trigger = self.store.get_trigger(trigger_id)?;
        let now = self.clock.now();
        let now_str = now.to_rfc3339();

        if !trigger.enabled {
            return Err(SchedulerError::TriggerDisabled(trigger_id.to_string()));
        }

        self.fire_trigger(&trigger, &now_str, None)
    }

    /// Evaluate a single trigger to decide if it should fire.
    fn evaluate_trigger(
        &self,
        trigger: &Trigger,
        now: DateTime<Utc>,
        now_str: &str,
    ) -> Result<Option<TickResult>, SchedulerError> {
        let state = self.store.get_state(&trigger.id)?;
        let should_fire = match &trigger.kind {
            TriggerKind::Cron {
                expression,
                timezone,
            } => self.should_fire_cron(expression, timezone, &state, now)?,
            TriggerKind::Interval { every, start_at } => {
                self.should_fire_interval(every, start_at.as_deref(), &state, now)?
            }
            // File arrival: poll-based sensor evaluated during tick.
            TriggerKind::FileArrival {
                path,
                poll_interval,
                ..
            } => {
                return self.evaluate_file_arrival(
                    trigger,
                    path,
                    poll_interval,
                    state,
                    now,
                    now_str,
                );
            }
            // Webhook and pipeline-completion are event-driven — they fire
            // via `fire_webhook` and `notify_run_completed` respectively,
            // not the tick loop.
            TriggerKind::Webhook { .. } | TriggerKind::PipelineCompletion { .. } => {
                // Update last_evaluated_at for monitoring.
                let new_state = TriggerState {
                    trigger_id: trigger.id.clone(),
                    last_evaluated_at: Some(now_str.to_string()),
                    last_fired_at: state.as_ref().and_then(|s| s.last_fired_at.clone()),
                    next_fire_at: None,
                    sensor_state: state.as_ref().and_then(|s| s.sensor_state.clone()),
                    consecutive_errors: state.as_ref().map_or(0, |s| s.consecutive_errors),
                };
                self.store.upsert_state(&new_state)?;
                return Ok(None);
            }
        };

        if !should_fire {
            return Ok(None);
        }

        let result = self.fire_trigger(trigger, now_str, None)?;

        // Update state with next fire time.
        let next_fire = self.compute_next_fire(trigger, now)?;
        let new_state = TriggerState {
            trigger_id: trigger.id.clone(),
            last_evaluated_at: Some(now_str.to_string()),
            last_fired_at: Some(now_str.to_string()),
            next_fire_at: next_fire.map(|dt| dt.to_rfc3339()),
            sensor_state: state.and_then(|s| s.sensor_state),
            consecutive_errors: 0,
        };
        self.store.upsert_state(&new_state)?;

        Ok(Some(result))
    }

    /// Check if a cron trigger should fire now.
    fn should_fire_cron(
        &self,
        expression: &str,
        timezone: &str,
        state: &Option<TriggerState>,
        now: DateTime<Utc>,
    ) -> Result<bool, SchedulerError> {
        let schedule = CronSchedule::parse(expression, timezone)?;

        // If we have a pre-computed next_fire_at, check against it.
        if let Some(s) = state {
            if let Some(ref next_str) = s.next_fire_at {
                if let Ok(next_fire) = DateTime::parse_from_rfc3339(next_str) {
                    return Ok(now >= next_fire.with_timezone(&Utc));
                }
            }
        }

        // No state yet — compute from the last evaluation or from now.
        let reference = state
            .as_ref()
            .and_then(|s| s.last_evaluated_at.as_ref())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or(now - chrono::Duration::seconds(1));

        if let Some(next) = schedule.next_after(reference) {
            Ok(now >= next)
        } else {
            Ok(false)
        }
    }

    /// Check if an interval trigger should fire now.
    fn should_fire_interval(
        &self,
        every: &str,
        start_at: Option<&str>,
        state: &Option<TriggerState>,
        now: DateTime<Utc>,
    ) -> Result<bool, SchedulerError> {
        // If we have a pre-computed next_fire_at, check against it.
        if let Some(s) = state {
            if let Some(ref next_str) = s.next_fire_at {
                if let Ok(next_fire) = DateTime::parse_from_rfc3339(next_str) {
                    return Ok(now >= next_fire.with_timezone(&Utc));
                }
            }
        }

        // Compute from scratch.
        let duration = Iso8601Duration::parse(every)?;
        let anchor = start_at
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or(now);

        let next = duration.next_after(anchor, now - chrono::Duration::seconds(1));
        Ok(now >= next)
    }

    /// Compute the next fire time for a trigger.
    fn compute_next_fire(
        &self,
        trigger: &Trigger,
        now: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, SchedulerError> {
        match &trigger.kind {
            TriggerKind::Cron {
                expression,
                timezone,
            } => {
                let schedule = CronSchedule::parse(expression, timezone)?;
                Ok(schedule.next_after(now))
            }
            TriggerKind::Interval { every, start_at } => {
                let duration = Iso8601Duration::parse(every)?;
                let anchor = start_at
                    .as_deref()
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or(now);
                Ok(Some(duration.next_after(anchor, now)))
            }
            // Sensor-based triggers don't have a pre-computed next fire time.
            _ => Ok(None),
        }
    }

    // -----------------------------------------------------------------
    // File arrival sensor
    // -----------------------------------------------------------------

    /// Evaluate a file-arrival trigger during the tick loop.
    ///
    /// Respects the trigger's `poll_interval` — if not enough time has elapsed
    /// since the last evaluation, returns `Ok(None)` immediately.
    fn evaluate_file_arrival(
        &self,
        trigger: &Trigger,
        path: &str,
        poll_interval: &str,
        state: Option<TriggerState>,
        now: DateTime<Utc>,
        now_str: &str,
    ) -> Result<Option<TickResult>, SchedulerError> {
        // Throttle: only poll when poll_interval has elapsed.
        if let Some(ref s) = state {
            if let Some(ref last_eval) = s.last_evaluated_at {
                if let Ok(last) = DateTime::parse_from_rfc3339(last_eval) {
                    let interval = Iso8601Duration::parse(poll_interval)?;
                    let next_poll = last.with_timezone(&Utc) + interval.to_chrono_duration();
                    if now < next_poll {
                        return Ok(None);
                    }
                }
            }
        }

        // Load persisted sensor state (set of previously seen files).
        let sensor_state: FileArrivalState = state
            .as_ref()
            .and_then(|s| s.sensor_state.as_ref())
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let new_files =
            detect_new_local_files(path, &sensor_state).map_err(SchedulerError::Sensor)?;

        if new_files.is_empty() {
            // No new files — just update last_evaluated_at.
            let new_state = TriggerState {
                trigger_id: trigger.id.clone(),
                last_evaluated_at: Some(now_str.to_string()),
                last_fired_at: state.as_ref().and_then(|s| s.last_fired_at.clone()),
                next_fire_at: None,
                sensor_state: state.and_then(|s| s.sensor_state),
                consecutive_errors: 0,
            };
            self.store.upsert_state(&new_state)?;
            return Ok(None);
        }

        info!(
            trigger = %trigger.id,
            count = new_files.len(),
            "file arrival sensor detected new files"
        );

        // Evaluate variable_mapping if configured.
        let mapped = if let TriggerKind::FileArrival {
            variable_mapping: Some(ref mapping),
            ..
        } = trigger.kind
        {
            let ctx = FileArrivalContext {
                new_files: new_files.clone(),
            };
            Some(
                variable_mapping::evaluate_file_arrival(mapping, &ctx)
                    .map_err(SchedulerError::Sensor)?,
            )
        } else {
            None
        };

        // Fire the trigger.
        let result = self.fire_trigger(trigger, now_str, mapped.as_ref())?;

        // Merge new files into the seen set and persist.
        let mut updated = sensor_state;
        for f in &new_files {
            updated.seen_files.insert(f.clone());
        }
        let new_state = TriggerState {
            trigger_id: trigger.id.clone(),
            last_evaluated_at: Some(now_str.to_string()),
            last_fired_at: Some(now_str.to_string()),
            next_fire_at: None,
            sensor_state: Some(serde_json::to_value(&updated).unwrap()),
            consecutive_errors: 0,
        };
        self.store.upsert_state(&new_state)?;

        Ok(Some(result))
    }

    // -----------------------------------------------------------------
    // Pipeline completion sensor
    // -----------------------------------------------------------------

    /// Notify the scheduler that a pipeline run has completed.
    ///
    /// Checks all enabled `PipelineCompletion` triggers and fires any whose
    /// `upstream_pipeline`, `environment`, and `on_status` match the completed
    /// run. Called by the server when it observes a `RunCompleted` event.
    pub fn notify_run_completed(
        &self,
        pipeline_name: &str,
        environment: &str,
        status: &str,
    ) -> Vec<TickResult> {
        let now = self.clock.now();
        let now_str = now.to_rfc3339();
        let mut results = Vec::new();

        let triggers = match self.store.list_enabled_triggers() {
            Ok(t) => t,
            Err(e) => {
                error!("failed to load triggers for completion check: {e}");
                return results;
            }
        };

        for trigger in &triggers {
            if let TriggerKind::PipelineCompletion {
                ref upstream_pipeline,
                environment: ref trigger_env,
                ref on_status,
            } = trigger.kind
            {
                if upstream_pipeline != pipeline_name {
                    continue;
                }
                // Environment filter: None means "any environment".
                if let Some(env) = trigger_env {
                    if env != environment {
                        continue;
                    }
                }
                let status_matches = match on_status {
                    CompletionStatus::Success => status == "success",
                    CompletionStatus::Failure => status == "failed",
                    CompletionStatus::Any => true,
                };
                if !status_matches {
                    continue;
                }

                info!(
                    trigger = %trigger.id,
                    upstream = pipeline_name,
                    "pipeline completion sensor firing"
                );

                match self.fire_trigger(trigger, &now_str, None) {
                    Ok(result) => {
                        let new_state = TriggerState {
                            trigger_id: trigger.id.clone(),
                            last_evaluated_at: Some(now_str.clone()),
                            last_fired_at: Some(now_str.clone()),
                            next_fire_at: None,
                            sensor_state: None,
                            consecutive_errors: 0,
                        };
                        let _ = self.store.upsert_state(&new_state);
                        results.push(result);
                    }
                    Err(e) => {
                        error!(trigger = %trigger.id, "completion trigger fire failed: {e}");
                        self.record_error(trigger, &now_str, &e.to_string());
                    }
                }
            }
        }

        results
    }

    // -----------------------------------------------------------------
    // Webhook sensor
    // -----------------------------------------------------------------

    /// Fire a webhook trigger.
    ///
    /// Called from the HTTP webhook endpoint after authentication. The request
    /// `body` is stored in the trigger history for audit and used to evaluate
    /// `variable_mapping` expressions.
    pub fn fire_webhook(
        &self,
        trigger_id: &crate::types::TriggerId,
        body: &serde_json::Value,
    ) -> Result<TickResult, SchedulerError> {
        let trigger = self.store.get_trigger(trigger_id)?;
        let now = self.clock.now();
        let now_str = now.to_rfc3339();

        if !trigger.enabled {
            return Err(SchedulerError::TriggerDisabled(trigger_id.to_string()));
        }
        if !matches!(trigger.kind, TriggerKind::Webhook { .. }) {
            return Err(SchedulerError::Sensor(format!(
                "trigger {trigger_id} is not a webhook trigger"
            )));
        }

        info!(trigger = %trigger.id, "webhook sensor firing");

        // Evaluate variable_mapping if configured.
        let mapped = if let TriggerKind::Webhook {
            variable_mapping: Some(ref mapping),
            ..
        } = trigger.kind
        {
            let ctx = WebhookContext { body: body.clone() };
            Some(
                variable_mapping::evaluate_webhook(mapping, &ctx)
                    .map_err(SchedulerError::Sensor)?,
            )
        } else {
            None
        };

        let result = self.fire_trigger(&trigger, &now_str, mapped.as_ref())?;

        // Persist state and record the body for audit.
        let new_state = TriggerState {
            trigger_id: trigger.id.clone(),
            last_evaluated_at: Some(now_str.clone()),
            last_fired_at: Some(now_str.clone()),
            next_fire_at: None,
            sensor_state: None,
            consecutive_errors: 0,
        };
        self.store.upsert_state(&new_state)?;

        // Record webhook body in trigger history details (separate from the
        // firing already recorded by fire_trigger).
        let audit_entry = TriggerHistoryEntry {
            id: Uuid::new_v4().to_string(),
            trigger_id: trigger.id.clone(),
            fired_at: now_str,
            outcome: result.outcome.clone(),
            run_id: result.run_id.clone(),
            details: Some(serde_json::json!({ "webhook_body": body })),
            error: None,
        };
        let _ = self.store.record_history(&audit_entry);

        Ok(result)
    }

    // -----------------------------------------------------------------
    // Core fire / run-policy logic
    // -----------------------------------------------------------------

    /// Fire a trigger: enforce run policy, dispatch the run, record history.
    ///
    /// `mapped_variables` are values extracted from the trigger event context
    /// (file paths, webhook body, completion metadata) via `variable_mapping`.
    /// They are merged with `trigger.variable_overrides` before dispatch, with
    /// mapped values taking precedence.
    fn fire_trigger(
        &self,
        trigger: &Trigger,
        now_str: &str,
        mapped_variables: Option<&HashMap<String, Value>>,
    ) -> Result<TickResult, SchedulerError> {
        let merged = mapped_variables
            .map(|m| variable_mapping::merge_variables(trigger.variable_overrides.as_ref(), m));
        let vars_ref = merged.as_ref().or(trigger.variable_overrides.as_ref());

        let is_running = self
            .dispatcher
            .is_pipeline_running(&trigger.pipeline_id, &trigger.environment);

        if is_running {
            return self.apply_run_policy(trigger, now_str, vars_ref);
        }

        // Dispatch the run.
        match self.dispatcher.dispatch_run(
            &trigger.pipeline_id,
            &trigger.environment,
            vars_ref,
            &trigger.id.to_string(),
        ) {
            Ok(run_id) => {
                info!(
                    trigger = %trigger.id,
                    pipeline = %trigger.pipeline_id,
                    run_id = %run_id,
                    "trigger fired — run started"
                );
                let kind_str = match &trigger.kind {
                    TriggerKind::Cron { expression, .. } => format!("cron:{expression}"),
                    TriggerKind::Interval { every, .. } => format!("interval:{every}"),
                    TriggerKind::FileArrival { .. } => "file_arrival".into(),
                    TriggerKind::Webhook { .. } => "webhook".into(),
                    TriggerKind::PipelineCompletion { .. } => "pipeline_completion".into(),
                };
                emit_event!(obs::FluxEvent::TriggerFired(obs::TriggerFired {
                    trigger_id: trigger.id.to_string(),
                    kind: kind_str.clone(),
                    pipeline_id: Some(trigger.pipeline_id.clone()),
                    next_fire_at: None,
                }));
                prom::record_trigger_firing(&trigger.id.to_string(), &kind_str, "run_started");
                prom::record_trigger_last_fired(
                    &trigger.id.to_string(),
                    self.clock.now().timestamp() as f64,
                );
                self.record_firing(
                    trigger,
                    now_str,
                    TriggerOutcome::RunStarted,
                    Some(&run_id),
                    None,
                );
                Ok(TickResult {
                    trigger_id: trigger.id.to_string(),
                    outcome: TriggerOutcome::RunStarted,
                    run_id: Some(run_id),
                    error: None,
                })
            }
            Err(e) => {
                error!(trigger = %trigger.id, "dispatch failed: {e}");
                let kind_str = match &trigger.kind {
                    TriggerKind::Cron { expression, .. } => format!("cron:{expression}"),
                    TriggerKind::Interval { every, .. } => format!("interval:{every}"),
                    TriggerKind::FileArrival { .. } => "file_arrival".into(),
                    TriggerKind::Webhook { .. } => "webhook".into(),
                    TriggerKind::PipelineCompletion { .. } => "pipeline_completion".into(),
                };
                prom::record_trigger_firing(&trigger.id.to_string(), &kind_str, "error");
                self.record_firing(trigger, now_str, TriggerOutcome::Error, None, Some(&e));
                Ok(TickResult {
                    trigger_id: trigger.id.to_string(),
                    outcome: TriggerOutcome::Error,
                    run_id: None,
                    error: Some(e),
                })
            }
        }
    }

    /// Apply the run policy when the pipeline is already running.
    fn apply_run_policy(
        &self,
        trigger: &Trigger,
        now_str: &str,
        variables: Option<&HashMap<String, Value>>,
    ) -> Result<TickResult, SchedulerError> {
        match trigger.run_policy {
            RunPolicy::Queue => {
                let queued = self.store.count_pending_runs(&trigger.id)?;
                if queued >= trigger.max_queue_depth {
                    warn!(
                        trigger = %trigger.id,
                        queued,
                        max = trigger.max_queue_depth,
                        "queue full — skipping"
                    );
                    self.record_firing(trigger, now_str, TriggerOutcome::Skipped, None, None);
                    return Err(SchedulerError::QueueFull(
                        trigger.id.to_string(),
                        trigger.max_queue_depth,
                    ));
                }

                // Dispatch with queuing semantics (the dispatcher handles enqueuing).
                match self.dispatcher.dispatch_run(
                    &trigger.pipeline_id,
                    &trigger.environment,
                    variables,
                    &trigger.id.to_string(),
                ) {
                    Ok(run_id) => {
                        info!(trigger = %trigger.id, run_id = %run_id, "trigger fired — run queued");
                        self.record_firing(
                            trigger,
                            now_str,
                            TriggerOutcome::Queued,
                            Some(&run_id),
                            None,
                        );
                        Ok(TickResult {
                            trigger_id: trigger.id.to_string(),
                            outcome: TriggerOutcome::Queued,
                            run_id: Some(run_id),
                            error: None,
                        })
                    }
                    Err(e) => {
                        self.record_firing(trigger, now_str, TriggerOutcome::Error, None, Some(&e));
                        Ok(TickResult {
                            trigger_id: trigger.id.to_string(),
                            outcome: TriggerOutcome::Error,
                            run_id: None,
                            error: Some(e),
                        })
                    }
                }
            }
            RunPolicy::Skip => {
                debug!(trigger = %trigger.id, "pipeline already running — skipping");
                self.record_firing(trigger, now_str, TriggerOutcome::Skipped, None, None);
                Ok(TickResult {
                    trigger_id: trigger.id.to_string(),
                    outcome: TriggerOutcome::Skipped,
                    run_id: None,
                    error: None,
                })
            }
            RunPolicy::Reject => {
                warn!(trigger = %trigger.id, "pipeline already running — rejecting");
                let msg = format!(
                    "pipeline '{}' already running in environment '{}'",
                    trigger.pipeline_id, trigger.environment
                );
                self.record_firing(trigger, now_str, TriggerOutcome::Rejected, None, Some(&msg));
                Err(SchedulerError::RunPolicyRejected(msg))
            }
        }
    }

    /// Record a trigger firing in the history table.
    fn record_firing(
        &self,
        trigger: &Trigger,
        now_str: &str,
        outcome: TriggerOutcome,
        run_id: Option<&str>,
        error: Option<&str>,
    ) {
        let entry = TriggerHistoryEntry {
            id: Uuid::new_v4().to_string(),
            trigger_id: trigger.id.clone(),
            fired_at: now_str.to_string(),
            outcome,
            run_id: run_id.map(|s| s.to_string()),
            details: None,
            error: error.map(|s| s.to_string()),
        };
        if let Err(e) = self.store.record_history(&entry) {
            error!(trigger = %trigger.id, "failed to record trigger history: {e}");
        }
    }

    /// Record an error during trigger evaluation.
    fn record_error(&self, trigger: &Trigger, now_str: &str, error: &str) {
        self.record_firing(trigger, now_str, TriggerOutcome::Error, None, Some(error));

        // Increment consecutive_errors in state.
        if let Ok(state) = self.store.get_state(&trigger.id) {
            let prev_errors = state.as_ref().map_or(0, |s| s.consecutive_errors);
            let new_state = TriggerState {
                trigger_id: trigger.id.clone(),
                last_evaluated_at: Some(now_str.to_string()),
                last_fired_at: state.as_ref().and_then(|s| s.last_fired_at.clone()),
                next_fire_at: state.as_ref().and_then(|s| s.next_fire_at.clone()),
                sensor_state: state.and_then(|s| s.sensor_state),
                consecutive_errors: prev_errors + 1,
            };
            let _ = self.store.upsert_state(&new_state);
        }
    }
}

/// Result of evaluating a single trigger during a tick.
#[derive(Debug, Clone)]
pub struct TickResult {
    pub trigger_id: String,
    pub outcome: TriggerOutcome,
    pub run_id: Option<String>,
    pub error: Option<String>,
}

/// Run the scheduler as an async loop. Call this from the server startup.
///
/// `tick_interval` controls how often time-based triggers are evaluated
/// (default: 15 seconds as specified in the design doc).
pub async fn run_scheduler_loop(
    scheduler: Arc<Scheduler>,
    tick_interval: std::time::Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    info!(
        interval_secs = tick_interval.as_secs(),
        "scheduler loop started"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep(tick_interval) => {
                match scheduler.tick() {
                    Ok(results) => {
                        if !results.is_empty() {
                            debug!(count = results.len(), "scheduler tick produced results");
                        }
                    }
                    Err(e) => {
                        error!("scheduler tick error: {e}");
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("scheduler loop shutting down");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::SqliteTriggerStore;
    use crate::types::*;
    use chrono::TimeZone;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Fake clock for deterministic tests.
    struct FakeClock {
        now: Mutex<DateTime<Utc>>,
    }

    impl FakeClock {
        fn new(now: DateTime<Utc>) -> Self {
            Self {
                now: Mutex::new(now),
            }
        }

        fn advance(&self, duration: chrono::Duration) {
            let mut now = self.now.lock().unwrap();
            *now += duration;
        }

        fn set(&self, time: DateTime<Utc>) {
            *self.now.lock().unwrap() = time;
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> DateTime<Utc> {
            *self.now.lock().unwrap()
        }
    }

    /// Fake dispatcher that records dispatched runs.
    struct FakeDispatcher {
        runs: Mutex<Vec<(String, String)>>,
        running: Mutex<bool>,
    }

    impl FakeDispatcher {
        fn new() -> Self {
            Self {
                runs: Mutex::new(Vec::new()),
                running: Mutex::new(false),
            }
        }

        fn set_running(&self, running: bool) {
            *self.running.lock().unwrap() = running;
        }

        fn dispatched_runs(&self) -> Vec<(String, String)> {
            self.runs.lock().unwrap().clone()
        }
    }

    impl RunDispatcher for FakeDispatcher {
        fn dispatch_run(
            &self,
            pipeline_id: &str,
            environment: &str,
            _variables: Option<&HashMap<String, serde_json::Value>>,
            _trigger_id: &str,
        ) -> Result<String, String> {
            let run_id = Uuid::new_v4().to_string();
            self.runs
                .lock()
                .unwrap()
                .push((pipeline_id.to_string(), environment.to_string()));
            Ok(run_id)
        }

        fn is_pipeline_running(&self, _pipeline_id: &str, _environment: &str) -> bool {
            *self.running.lock().unwrap()
        }
    }

    fn setup() -> (
        Arc<SqliteTriggerStore>,
        Arc<FakeClock>,
        Arc<FakeDispatcher>,
        Scheduler,
    ) {
        let store = Arc::new(SqliteTriggerStore::open_in_memory().unwrap());
        let clock = Arc::new(FakeClock::new(
            Utc.with_ymd_and_hms(2026, 4, 9, 10, 0, 0).unwrap(),
        ));
        let dispatcher = Arc::new(FakeDispatcher::new());
        let scheduler = Scheduler::new(
            store.clone() as Arc<dyn TriggerStorage>,
            clock.clone() as Arc<dyn Clock>,
            dispatcher.clone() as Arc<dyn RunDispatcher>,
        );
        (store, clock, dispatcher, scheduler)
    }

    fn cron_trigger(name: &str, expression: &str) -> Trigger {
        Trigger {
            id: TriggerId::new(),
            name: name.to_string(),
            pipeline_id: "pipeline-1".to_string(),
            environment: "dev".to_string(),
            enabled: true,
            kind: TriggerKind::Cron {
                expression: expression.to_string(),
                timezone: "UTC".to_string(),
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn cron_trigger_fires_on_schedule() {
        let (store, clock, dispatcher, scheduler) = setup();

        // "At minute 0 of every hour" — next fire after 10:00 is 11:00.
        let trigger = cron_trigger("hourly", "0 * * * *");
        store.create_trigger(&trigger).unwrap();

        // At 10:00 — first tick seeds the state, doesn't fire (next is 11:00).
        // But since there's no prior state, the reference is now-1s, and the
        // next occurrence after 09:59:59 is 10:00:00 which equals now → fires.
        let results = scheduler.tick().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, TriggerOutcome::RunStarted);

        // At 10:30 — not time yet.
        clock.set(Utc.with_ymd_and_hms(2026, 4, 9, 10, 30, 0).unwrap());
        let results = scheduler.tick().unwrap();
        assert!(results.is_empty());

        // At 11:00 — fires.
        clock.set(Utc.with_ymd_and_hms(2026, 4, 9, 11, 0, 0).unwrap());
        let results = scheduler.tick().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, TriggerOutcome::RunStarted);

        assert_eq!(dispatcher.dispatched_runs().len(), 2);
    }

    #[test]
    fn disabled_trigger_does_not_fire() {
        let (store, _clock, dispatcher, scheduler) = setup();

        let mut trigger = cron_trigger("hourly", "0 * * * *");
        trigger.enabled = false;
        store.create_trigger(&trigger).unwrap();

        let results = scheduler.tick().unwrap();
        assert!(results.is_empty());
        assert!(dispatcher.dispatched_runs().is_empty());
    }

    #[test]
    fn kill_switch_stops_firing() {
        let (store, clock, dispatcher, scheduler) = setup();

        let trigger = cron_trigger("hourly", "0 * * * *");
        store.create_trigger(&trigger).unwrap();

        // First tick fires.
        let results = scheduler.tick().unwrap();
        assert_eq!(results.len(), 1);

        // Disable (kill switch).
        store.set_enabled(&trigger.id, false).unwrap();

        // Advance to next hour.
        clock.set(Utc.with_ymd_and_hms(2026, 4, 9, 11, 0, 0).unwrap());
        let results = scheduler.tick().unwrap();
        assert!(results.is_empty());

        // Only one run dispatched total.
        assert_eq!(dispatcher.dispatched_runs().len(), 1);
    }

    #[test]
    fn run_policy_skip() {
        let (store, _clock, dispatcher, scheduler) = setup();

        let mut trigger = cron_trigger("hourly", "0 * * * *");
        trigger.run_policy = RunPolicy::Skip;
        store.create_trigger(&trigger).unwrap();

        // Mark pipeline as running.
        dispatcher.set_running(true);

        let results = scheduler.tick().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, TriggerOutcome::Skipped);
        assert!(dispatcher.dispatched_runs().is_empty());
    }

    #[test]
    fn run_policy_reject() {
        let (store, _clock, dispatcher, scheduler) = setup();

        let mut trigger = cron_trigger("hourly", "0 * * * *");
        trigger.run_policy = RunPolicy::Reject;
        store.create_trigger(&trigger).unwrap();

        dispatcher.set_running(true);

        // tick() collects the error as an Error outcome.
        let results = scheduler.tick().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, TriggerOutcome::Error);
    }

    #[test]
    fn interval_trigger_fires() {
        let (store, clock, dispatcher, scheduler) = setup();

        let trigger = Trigger {
            id: TriggerId::new(),
            name: "every-30m".to_string(),
            pipeline_id: "pipeline-1".to_string(),
            environment: "dev".to_string(),
            enabled: true,
            kind: TriggerKind::Interval {
                every: "PT30M".to_string(),
                start_at: Some("2026-04-09T00:00:00+00:00".to_string()),
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        };
        store.create_trigger(&trigger).unwrap();

        // At 10:00 — on an interval boundary, should fire.
        let results = scheduler.tick().unwrap();
        assert_eq!(results.len(), 1);

        // At 10:15 — not on boundary.
        clock.set(Utc.with_ymd_and_hms(2026, 4, 9, 10, 15, 0).unwrap());
        let results = scheduler.tick().unwrap();
        assert!(results.is_empty());

        // At 10:30 — next boundary, should fire.
        clock.set(Utc.with_ymd_and_hms(2026, 4, 9, 10, 30, 0).unwrap());
        let results = scheduler.tick().unwrap();
        assert_eq!(results.len(), 1);

        assert_eq!(dispatcher.dispatched_runs().len(), 2);
    }

    #[test]
    fn manual_fire_respects_run_policy() {
        let (store, _clock, dispatcher, scheduler) = setup();

        let mut trigger = cron_trigger("hourly", "0 * * * *");
        trigger.run_policy = RunPolicy::Reject;
        store.create_trigger(&trigger).unwrap();

        // Not running → fires.
        let result = scheduler.manual_fire(&trigger.id).unwrap();
        assert_eq!(result.outcome, TriggerOutcome::RunStarted);

        // Running → rejected.
        dispatcher.set_running(true);
        let result = scheduler.manual_fire(&trigger.id);
        assert!(result.is_err());
    }

    #[test]
    fn trigger_history_recorded() {
        let (store, _clock, _dispatcher, scheduler) = setup();

        let trigger = cron_trigger("hourly", "0 * * * *");
        store.create_trigger(&trigger).unwrap();

        scheduler.tick().unwrap();

        let history = store.get_history(&trigger.id, 10).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].outcome, TriggerOutcome::RunStarted);
        assert!(history[0].run_id.is_some());
    }

    #[test]
    fn consecutive_errors_tracked() {
        let (store, _clock, _dispatcher, scheduler) = setup();

        // Create a trigger with an invalid cron expression to force errors.
        let trigger = cron_trigger("bad", "invalid cron");
        store.create_trigger(&trigger).unwrap();

        // Tick should record the error.
        let results = scheduler.tick().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, TriggerOutcome::Error);

        let state = store.get_state(&trigger.id).unwrap().unwrap();
        assert_eq!(state.consecutive_errors, 1);
    }

    // ---------------------------------------------------------------
    // File arrival sensor tests
    // ---------------------------------------------------------------

    #[test]
    fn file_arrival_fires_on_new_files() {
        let (store, _clock, dispatcher, scheduler) = setup();

        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.csv"), "data").unwrap();

        let trigger = Trigger {
            id: TriggerId::new(),
            name: "csv-watch".to_string(),
            pipeline_id: "pipeline-1".to_string(),
            environment: "dev".to_string(),
            enabled: true,
            kind: TriggerKind::FileArrival {
                path: format!("{}/*.csv", dir.path().display()),
                poll_interval: "PT1S".to_string(),
                variable_mapping: None,
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        };
        store.create_trigger(&trigger).unwrap();

        // First tick: detects a.csv, fires.
        let results = scheduler.tick().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, TriggerOutcome::RunStarted);
        assert_eq!(dispatcher.dispatched_runs().len(), 1);

        // Verify seen files are persisted in sensor state.
        let state = store.get_state(&trigger.id).unwrap().unwrap();
        assert!(state.sensor_state.is_some());
    }

    #[test]
    fn file_arrival_suppresses_duplicates() {
        let (store, clock, dispatcher, scheduler) = setup();

        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.csv"), "data").unwrap();

        let trigger = Trigger {
            id: TriggerId::new(),
            name: "csv-watch".to_string(),
            pipeline_id: "pipeline-1".to_string(),
            environment: "dev".to_string(),
            enabled: true,
            kind: TriggerKind::FileArrival {
                path: format!("{}/*.csv", dir.path().display()),
                poll_interval: "PT1S".to_string(),
                variable_mapping: None,
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        };
        store.create_trigger(&trigger).unwrap();

        // First tick fires.
        scheduler.tick().unwrap();
        assert_eq!(dispatcher.dispatched_runs().len(), 1);

        // Second tick: same file, no fire.
        clock.advance(chrono::Duration::seconds(2));
        let results = scheduler.tick().unwrap();
        assert!(results.is_empty());
        assert_eq!(dispatcher.dispatched_runs().len(), 1);

        // Add a new file — should fire again.
        std::fs::write(dir.path().join("b.csv"), "more data").unwrap();
        clock.advance(chrono::Duration::seconds(2));
        let results = scheduler.tick().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(dispatcher.dispatched_runs().len(), 2);
    }

    #[test]
    fn file_arrival_respects_poll_interval() {
        let (store, clock, dispatcher, scheduler) = setup();

        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.csv"), "data").unwrap();

        let trigger = Trigger {
            id: TriggerId::new(),
            name: "csv-watch".to_string(),
            pipeline_id: "pipeline-1".to_string(),
            environment: "dev".to_string(),
            enabled: true,
            kind: TriggerKind::FileArrival {
                path: format!("{}/*.csv", dir.path().display()),
                poll_interval: "PT5M".to_string(), // 5-minute poll interval
                variable_mapping: None,
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        };
        store.create_trigger(&trigger).unwrap();

        // First tick fires (no prior state, so poll happens immediately).
        scheduler.tick().unwrap();
        assert_eq!(dispatcher.dispatched_runs().len(), 1);

        // Add a new file but only advance 1 minute — below poll_interval.
        std::fs::write(dir.path().join("b.csv"), "more").unwrap();
        clock.advance(chrono::Duration::minutes(1));
        let results = scheduler.tick().unwrap();
        assert!(results.is_empty()); // Throttled.

        // Advance past poll interval — now it polls and detects b.csv.
        clock.advance(chrono::Duration::minutes(5));
        let results = scheduler.tick().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(dispatcher.dispatched_runs().len(), 2);
    }

    // ---------------------------------------------------------------
    // Pipeline completion sensor tests
    // ---------------------------------------------------------------

    #[test]
    fn completion_sensor_fires_on_upstream_success() {
        let (store, _clock, dispatcher, scheduler) = setup();

        let trigger = Trigger {
            id: TriggerId::new(),
            name: "after-ingest".to_string(),
            pipeline_id: "pipeline-downstream".to_string(),
            environment: "prod".to_string(),
            enabled: true,
            kind: TriggerKind::PipelineCompletion {
                upstream_pipeline: "ingest_orders".to_string(),
                environment: Some("prod".to_string()),
                on_status: CompletionStatus::Success,
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        };
        store.create_trigger(&trigger).unwrap();

        // Notify that the upstream pipeline completed successfully.
        let results = scheduler.notify_run_completed("ingest_orders", "prod", "success");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, TriggerOutcome::RunStarted);
        assert_eq!(dispatcher.dispatched_runs().len(), 1);
        assert_eq!(dispatcher.dispatched_runs()[0].0, "pipeline-downstream");
    }

    #[test]
    fn completion_sensor_ignores_wrong_pipeline() {
        let (store, _clock, dispatcher, scheduler) = setup();

        let trigger = Trigger {
            id: TriggerId::new(),
            name: "after-ingest".to_string(),
            pipeline_id: "pipeline-downstream".to_string(),
            environment: "prod".to_string(),
            enabled: true,
            kind: TriggerKind::PipelineCompletion {
                upstream_pipeline: "ingest_orders".to_string(),
                environment: Some("prod".to_string()),
                on_status: CompletionStatus::Success,
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        };
        store.create_trigger(&trigger).unwrap();

        // Different pipeline — should not fire.
        let results = scheduler.notify_run_completed("other_pipeline", "prod", "success");
        assert!(results.is_empty());
        assert!(dispatcher.dispatched_runs().is_empty());
    }

    #[test]
    fn completion_sensor_filters_by_environment() {
        let (store, _clock, dispatcher, scheduler) = setup();

        let trigger = Trigger {
            id: TriggerId::new(),
            name: "after-ingest".to_string(),
            pipeline_id: "pipeline-downstream".to_string(),
            environment: "prod".to_string(),
            enabled: true,
            kind: TriggerKind::PipelineCompletion {
                upstream_pipeline: "ingest_orders".to_string(),
                environment: Some("prod".to_string()),
                on_status: CompletionStatus::Success,
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        };
        store.create_trigger(&trigger).unwrap();

        // Right pipeline, wrong environment — should not fire.
        let results = scheduler.notify_run_completed("ingest_orders", "dev", "success");
        assert!(results.is_empty());
        assert!(dispatcher.dispatched_runs().is_empty());
    }

    #[test]
    fn completion_sensor_status_filtering() {
        let (store, _clock, dispatcher, scheduler) = setup();

        // Trigger only fires on failure.
        let trigger = Trigger {
            id: TriggerId::new(),
            name: "on-failure".to_string(),
            pipeline_id: "alert-pipeline".to_string(),
            environment: "prod".to_string(),
            enabled: true,
            kind: TriggerKind::PipelineCompletion {
                upstream_pipeline: "ingest_orders".to_string(),
                environment: None, // any env
                on_status: CompletionStatus::Failure,
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        };
        store.create_trigger(&trigger).unwrap();

        // Success — should NOT fire.
        let results = scheduler.notify_run_completed("ingest_orders", "prod", "success");
        assert!(results.is_empty());

        // Failure — SHOULD fire.
        let results = scheduler.notify_run_completed("ingest_orders", "prod", "failed");
        assert_eq!(results.len(), 1);
        assert_eq!(dispatcher.dispatched_runs().len(), 1);
    }

    // ---------------------------------------------------------------
    // Webhook sensor tests
    // ---------------------------------------------------------------

    #[test]
    fn webhook_fires_trigger() {
        let (store, _clock, dispatcher, scheduler) = setup();

        let trigger = Trigger {
            id: TriggerId::new(),
            name: "orders-reload".to_string(),
            pipeline_id: "pipeline-1".to_string(),
            environment: "dev".to_string(),
            enabled: true,
            kind: TriggerKind::Webhook {
                path: "/triggers/orders-reload".to_string(),
                auth: "token".to_string(),
                variable_mapping: None,
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        };
        store.create_trigger(&trigger).unwrap();

        let body = serde_json::json!({ "order_id": "123" });
        let result = scheduler.fire_webhook(&trigger.id, &body).unwrap();
        assert_eq!(result.outcome, TriggerOutcome::RunStarted);
        assert_eq!(dispatcher.dispatched_runs().len(), 1);

        // Verify audit entry with webhook body.
        let history = store.get_history(&trigger.id, 10).unwrap();
        assert!(history.len() >= 2); // fire_trigger records + audit entry
        let audit = history.iter().find(|h| h.details.is_some()).unwrap();
        let details = audit.details.as_ref().unwrap();
        assert_eq!(details["webhook_body"]["order_id"], "123");
    }

    #[test]
    fn webhook_rejects_non_webhook_trigger() {
        let (store, _clock, _dispatcher, scheduler) = setup();

        let trigger = cron_trigger("hourly", "0 * * * *");
        store.create_trigger(&trigger).unwrap();

        let result = scheduler.fire_webhook(&trigger.id, &serde_json::Value::Null);
        assert!(result.is_err());
    }

    #[test]
    fn webhook_rejects_disabled_trigger() {
        let (store, _clock, _dispatcher, scheduler) = setup();

        let trigger = Trigger {
            id: TriggerId::new(),
            name: "disabled-hook".to_string(),
            pipeline_id: "pipeline-1".to_string(),
            environment: "dev".to_string(),
            enabled: false,
            kind: TriggerKind::Webhook {
                path: "/triggers/test".to_string(),
                auth: "token".to_string(),
                variable_mapping: None,
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        };
        store.create_trigger(&trigger).unwrap();

        let result = scheduler.fire_webhook(&trigger.id, &serde_json::Value::Null);
        assert!(matches!(result, Err(SchedulerError::TriggerDisabled(_))));
    }
}
