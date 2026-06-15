// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI commands for managing triggers.

use crate::OutputFormat;
use anyhow::{Context, Result};
use armillary_scheduler::{
    CompletionStatus, RunPolicy, Trigger, TriggerHistoryEntry, TriggerId, TriggerKind,
    TriggerOutcome, TriggerStorage,
};
use clap::Subcommand;
use std::sync::Arc;

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum TriggerAction {
    /// List all triggers, optionally filtered by pipeline or environment.
    List {
        /// Filter by pipeline name or UUID.
        #[arg(long)]
        pipeline: Option<String>,
        /// Filter by environment.
        #[arg(long)]
        env: Option<String>,
    },
    /// Show a trigger's configuration and current state.
    Show {
        /// Trigger ID (UUID).
        trigger_id: String,
    },
    /// Enable a trigger.
    Enable {
        /// Trigger ID (UUID).
        trigger_id: String,
    },
    /// Disable a trigger.
    Disable {
        /// Trigger ID (UUID).
        trigger_id: String,
    },
    /// Create a new trigger.
    Create {
        /// Pipeline name or UUID to trigger.
        #[arg(long)]
        pipeline: String,
        /// Trigger kind: cron, interval, file_arrival, webhook, pipeline_completion.
        #[arg(long)]
        kind: String,
        /// Cron expression (for kind=cron).
        #[arg(long)]
        expression: Option<String>,
        /// Timezone for cron (default: UTC).
        #[arg(long, default_value = "UTC")]
        timezone: String,
        /// ISO 8601 duration (for kind=interval, e.g. PT30M).
        #[arg(long)]
        every: Option<String>,
        /// Start time for interval triggers (RFC 3339).
        #[arg(long)]
        start_at: Option<String>,
        /// File path or glob pattern (for kind=file_arrival).
        #[arg(long)]
        path: Option<String>,
        /// Poll interval ISO 8601 duration (for kind=file_arrival, default PT1M).
        #[arg(long, default_value = "PT1M")]
        poll_interval: String,
        /// Upstream pipeline (for kind=pipeline_completion).
        #[arg(long)]
        upstream: Option<String>,
        /// Completion status filter: success, failure, any (default: success).
        #[arg(long, default_value = "success")]
        on_status: String,
        /// Environment for the trigger (default: dev).
        #[arg(long, short, default_value = "dev")]
        env: String,
        /// Trigger name (defaults to pipeline-kind).
        #[arg(long)]
        name: Option<String>,
        /// Run policy: queue, skip, reject (default: queue).
        #[arg(long, default_value = "queue")]
        run_policy: String,
        /// Maximum queue depth (default: 3).
        #[arg(long, default_value_t = 3)]
        max_queue_depth: u32,
    },
    /// Delete a trigger.
    Delete {
        /// Trigger ID (UUID).
        trigger_id: String,
    },
    /// Show recent trigger firing history.
    History {
        /// Trigger ID (UUID).
        trigger_id: String,
        /// Maximum number of entries to show.
        #[arg(long, default_value_t = 50)]
        limit: u32,
    },
    /// Manually fire a trigger (ignores schedule, respects run policy).
    Fire {
        /// Trigger ID (UUID).
        trigger_id: String,
    },
}

pub fn handle(
    action: TriggerAction,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    match action {
        TriggerAction::List { pipeline, env } => {
            list(pipeline.as_deref(), env.as_deref(), format, metadata_url)
        }
        TriggerAction::Show { trigger_id } => show(&trigger_id, format, metadata_url),
        TriggerAction::Enable { trigger_id } => enable(&trigger_id, format, metadata_url),
        TriggerAction::Disable { trigger_id } => disable(&trigger_id, format, metadata_url),
        TriggerAction::Create {
            pipeline,
            kind,
            expression,
            timezone,
            every,
            start_at,
            path,
            poll_interval,
            upstream,
            on_status,
            env,
            name,
            run_policy,
            max_queue_depth,
        } => {
            let trigger_kind = build_kind(
                &kind,
                expression.as_deref(),
                &timezone,
                every.as_deref(),
                start_at.as_deref(),
                path.as_deref(),
                &poll_interval,
                upstream.as_deref(),
                &on_status,
                &env,
            )?;
            let run_policy: RunPolicy =
                run_policy.parse().map_err(|e: String| anyhow::anyhow!(e))?;
            let trigger_name = name.unwrap_or_else(|| format!("{pipeline}-{kind}"));
            let now = chrono::Utc::now().to_rfc3339();
            let trigger = Trigger {
                id: TriggerId::new(),
                name: trigger_name,
                pipeline_id: pipeline,
                environment: env,
                enabled: true,
                kind: trigger_kind,
                run_policy,
                variable_overrides: None,
                max_queue_depth,
                created_at: now.clone(),
                updated_at: now,
            };
            create(&trigger, format, metadata_url)
        }
        TriggerAction::Delete { trigger_id } => delete(&trigger_id, format, metadata_url),
        TriggerAction::History { trigger_id, limit } => {
            history(&trigger_id, limit, format, metadata_url)
        }
        TriggerAction::Fire { trigger_id } => fire(&trigger_id, format, metadata_url),
    }
}

fn open_store(metadata_url: Option<&str>) -> Result<Arc<dyn TriggerStorage>> {
    let data_dir = crate::config::data_dir()?;
    let backend = crate::config::MetadataBackend::resolve(metadata_url, &data_dir)?;
    let stores = crate::config::open_stores(&backend, &data_dir)?;
    Ok(stores.trigger_store)
}

fn parse_trigger_id(s: &str) -> Result<TriggerId> {
    s.parse()
        .map_err(|_| anyhow::anyhow!("invalid trigger ID (expected UUID): {s}"))
}

fn kind_label(kind: &TriggerKind) -> &'static str {
    match kind {
        TriggerKind::Cron { .. } => "cron",
        TriggerKind::Interval { .. } => "interval",
        TriggerKind::FileArrival { .. } => "file_arrival",
        TriggerKind::Webhook { .. } => "webhook",
        TriggerKind::PipelineCompletion { .. } => "pipeline_completion",
    }
}

fn kind_summary(kind: &TriggerKind) -> String {
    match kind {
        TriggerKind::Cron {
            expression,
            timezone,
        } => format!("{expression} ({timezone})"),
        TriggerKind::Interval { every, .. } => every.clone(),
        TriggerKind::FileArrival { path, .. } => path.clone(),
        TriggerKind::Webhook { path, .. } => path.clone(),
        TriggerKind::PipelineCompletion {
            upstream_pipeline,
            on_status,
            ..
        } => format!("{upstream_pipeline} on {on_status:?}"),
    }
}

fn list(
    pipeline: Option<&str>,
    env: Option<&str>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let store = open_store(metadata_url)?;
    let triggers = store
        .list_triggers(pipeline, env)
        .context("failed to list triggers")?;

    match format {
        OutputFormat::Human => {
            if triggers.is_empty() {
                println!("No triggers found.");
                return Ok(());
            }
            println!(
                "{}",
                crate::color::bold(&format!(
                    "{:<38} {:<20} {:<16} {:<8} {:<8} {}",
                    "ID", "NAME", "KIND", "ENV", "STATUS", "SUMMARY"
                ))
            );
            println!("{}", crate::color::dim(&"-".repeat(110)));
            for t in &triggers {
                let status = if t.enabled { "active" } else { "paused" };
                println!(
                    "{:<38} {:<20} {:<16} {:<8} {:<8} {}",
                    t.id,
                    truncate(&t.name, 19),
                    kind_label(&t.kind),
                    truncate(&t.environment, 7),
                    status,
                    kind_summary(&t.kind),
                );
            }
        }
        OutputFormat::Json => {
            let items: Vec<_> = triggers
                .iter()
                .map(|t| serde_json::to_value(t).unwrap())
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "triggers": items }))?
            );
        }
    }
    Ok(())
}

fn show(trigger_id: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let store = open_store(metadata_url)?;
    let id = parse_trigger_id(trigger_id)?;
    let trigger = store.get_trigger(&id).context("failed to get trigger")?;
    let state = store.get_state(&id).ok().flatten();

    match format {
        OutputFormat::Human => {
            println!("Trigger:     {}", trigger.name);
            println!("ID:          {}", trigger.id);
            println!("Pipeline:    {}", trigger.pipeline_id);
            println!("Environment: {}", trigger.environment);
            println!(
                "Status:      {}",
                if trigger.enabled { "active" } else { "paused" }
            );
            println!("Kind:        {}", kind_label(&trigger.kind));
            println!("Config:      {}", kind_summary(&trigger.kind));
            println!("Run policy:  {}", trigger.run_policy);
            println!("Max queue:   {}", trigger.max_queue_depth);
            println!("Created:     {}", trigger.created_at);
            println!("Updated:     {}", trigger.updated_at);
            if let Some(ref st) = state {
                println!();
                println!(
                    "Last eval:   {}",
                    st.last_evaluated_at.as_deref().unwrap_or("never")
                );
                println!(
                    "Last fired:  {}",
                    st.last_fired_at.as_deref().unwrap_or("never")
                );
                println!(
                    "Next fire:   {}",
                    st.next_fire_at.as_deref().unwrap_or("N/A")
                );
                println!("Errors:      {}", st.consecutive_errors);
            }
        }
        OutputFormat::Json => {
            let mut val = serde_json::to_value(&trigger)?;
            if let Some(ref st) = state {
                val.as_object_mut()
                    .unwrap()
                    .insert("state".to_string(), serde_json::to_value(st)?);
            }
            println!("{}", serde_json::to_string_pretty(&val)?);
        }
    }
    Ok(())
}

fn enable(trigger_id: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let store = open_store(metadata_url)?;
    let id = parse_trigger_id(trigger_id)?;
    store
        .set_enabled(&id, true)
        .context("failed to enable trigger")?;

    match format {
        OutputFormat::Human => println!("Enabled trigger {trigger_id}"),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "id": trigger_id, "enabled": true
                }))?
            );
        }
    }
    Ok(())
}

fn disable(trigger_id: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let store = open_store(metadata_url)?;
    let id = parse_trigger_id(trigger_id)?;
    store
        .set_enabled(&id, false)
        .context("failed to disable trigger")?;

    match format {
        OutputFormat::Human => println!("Disabled trigger {trigger_id}"),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "id": trigger_id, "enabled": false
                }))?
            );
        }
    }
    Ok(())
}

fn create(trigger: &Trigger, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let store = open_store(metadata_url)?;
    store
        .create_trigger(trigger)
        .context("failed to create trigger")?;

    match format {
        OutputFormat::Human => {
            println!("Created trigger '{}' (id: {})", trigger.name, trigger.id);
            println!(
                "  Kind: {} — {}",
                kind_label(&trigger.kind),
                kind_summary(&trigger.kind)
            );
            println!(
                "  Pipeline: {} (env: {})",
                trigger.pipeline_id, trigger.environment
            );
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::to_value(trigger)?)?
            );
        }
    }
    Ok(())
}

fn delete(trigger_id: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let store = open_store(metadata_url)?;
    let id = parse_trigger_id(trigger_id)?;
    store
        .delete_trigger(&id)
        .context("failed to delete trigger")?;

    match format {
        OutputFormat::Human => println!("Deleted trigger {trigger_id}"),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "deleted": trigger_id }))?
            );
        }
    }
    Ok(())
}

fn history(
    trigger_id: &str,
    limit: u32,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let store = open_store(metadata_url)?;
    let id = parse_trigger_id(trigger_id)?;
    let entries = store
        .get_history(&id, limit)
        .context("failed to get trigger history")?;

    match format {
        OutputFormat::Human => {
            if entries.is_empty() {
                println!("No history for trigger {trigger_id}.");
                return Ok(());
            }
            println!(
                "{}",
                crate::color::bold(&format!(
                    "{:<26} {:<14} {:<38} {}",
                    "FIRED AT", "OUTCOME", "RUN ID", "ERROR"
                ))
            );
            println!("{}", crate::color::dim(&"-".repeat(100)));
            for e in &entries {
                println!(
                    "{:<26} {:<14} {:<38} {}",
                    e.fired_at,
                    e.outcome,
                    e.run_id.as_deref().unwrap_or("-"),
                    e.error.as_deref().unwrap_or(""),
                );
            }
        }
        OutputFormat::Json => {
            let items: Vec<_> = entries
                .iter()
                .map(|e| serde_json::to_value(e).unwrap())
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "history": items }))?
            );
        }
    }
    Ok(())
}

fn fire(trigger_id: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let store = open_store(metadata_url)?;
    let id = parse_trigger_id(trigger_id)?;

    // Verify the trigger exists and is enabled.
    let trigger = store.get_trigger(&id).context("failed to get trigger")?;

    if !trigger.enabled {
        anyhow::bail!("trigger {trigger_id} is disabled — enable it first or use the API");
    }

    // Record a manual fire in the history. Actual pipeline execution would
    // require the scheduler runtime; the CLI records the intent and the
    // scheduler picks it up on its next tick via the queued entry.
    let now = chrono::Utc::now().to_rfc3339();
    let entry = TriggerHistoryEntry {
        id: uuid::Uuid::new_v4().to_string(),
        trigger_id: id,
        fired_at: now,
        outcome: TriggerOutcome::Queued,
        run_id: None,
        details: Some(serde_json::json!({ "source": "cli_manual_fire" })),
        error: None,
    };
    store
        .record_history(&entry)
        .context("failed to record trigger fire")?;

    match format {
        OutputFormat::Human => {
            println!(
                "Fired trigger '{}' (id: {trigger_id}) — queued for execution",
                trigger.name
            );
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "id": trigger_id,
                    "outcome": "queued",
                    "history_id": entry.id,
                }))?
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_kind(
    kind: &str,
    expression: Option<&str>,
    timezone: &str,
    every: Option<&str>,
    start_at: Option<&str>,
    path: Option<&str>,
    poll_interval: &str,
    upstream: Option<&str>,
    on_status: &str,
    environment: &str,
) -> Result<TriggerKind> {
    match kind {
        "cron" => {
            let expression = expression
                .ok_or_else(|| anyhow::anyhow!("--expression is required for cron triggers"))?;
            Ok(TriggerKind::Cron {
                expression: expression.to_string(),
                timezone: timezone.to_string(),
            })
        }
        "interval" => {
            let every = every
                .ok_or_else(|| anyhow::anyhow!("--every is required for interval triggers"))?;
            Ok(TriggerKind::Interval {
                every: every.to_string(),
                start_at: start_at.map(|s| s.to_string()),
            })
        }
        "file_arrival" => {
            let path = path
                .ok_or_else(|| anyhow::anyhow!("--path is required for file_arrival triggers"))?;
            Ok(TriggerKind::FileArrival {
                path: path.to_string(),
                poll_interval: poll_interval.to_string(),
                variable_mapping: None,
            })
        }
        "webhook" => {
            let path = path
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("/triggers/webhook/{}", uuid::Uuid::new_v4()));
            Ok(TriggerKind::Webhook {
                path,
                auth: "token".to_string(),
                variable_mapping: None,
            })
        }
        "pipeline_completion" => {
            let upstream = upstream.ok_or_else(|| {
                anyhow::anyhow!("--upstream is required for pipeline_completion triggers")
            })?;
            let status = match on_status {
                "success" => CompletionStatus::Success,
                "failure" => CompletionStatus::Failure,
                "any" => CompletionStatus::Any,
                other => anyhow::bail!(
                    "unknown completion status: {other} (use success, failure, or any)"
                ),
            };
            Ok(TriggerKind::PipelineCompletion {
                upstream_pipeline: upstream.to_string(),
                environment: Some(environment.to_string()),
                on_status: status,
            })
        }
        other => anyhow::bail!(
            "unknown trigger kind: {other} (use cron, interval, file_arrival, webhook, or pipeline_completion)"
        ),
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max - 1])
    }
}
