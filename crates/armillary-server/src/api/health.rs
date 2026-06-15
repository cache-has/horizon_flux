// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cross-pipeline health dashboard API (planning doc 37, sub-feature 4).
//!
//! Provides a single `GET /api/health/overview` endpoint that aggregates
//! run history, trigger health, and SLA status into a project-wide
//! at-a-glance view.

use crate::api::ApiError;
use crate::state::AppState;
use armillary_datafusion::run::RunStatus;
use armillary_engine::SlaStatus;
use axum::Json;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

/// Build the `/health` sub-router.
pub fn router() -> Router<AppState> {
    Router::new().route("/overview", get(overview))
}

// ---------------------------------------------------------------------------
// Query / response types
// ---------------------------------------------------------------------------

/// Maximum number of runs to scan when building the overview.
const MAX_RUNS_SCAN: u32 = 10_000;

/// Maximum number of entries in top-failing / slowest pipeline lists.
const TOP_N: usize = 10;

/// How long a cached overview response is considered fresh.
const CACHE_TTL: Duration = Duration::from_secs(30);

/// In-memory TTL cache: window → (computed_at, response).
fn overview_cache() -> &'static Mutex<HashMap<String, (Instant, OverviewResponse)>> {
    static CACHE: OnceLock<Mutex<HashMap<String, (Instant, OverviewResponse)>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Debug, Deserialize)]
struct OverviewQuery {
    /// Time window: "24h", "7d", or "30d". Defaults to "24h".
    #[serde(default = "default_window")]
    window: String,
}

fn default_window() -> String {
    "24h".into()
}

fn parse_window(s: &str) -> Result<Duration, (StatusCode, Json<ApiError>)> {
    match s {
        "24h" => Ok(Duration::from_secs(24 * 3600)),
        "7d" => Ok(Duration::from_secs(7 * 24 * 3600)),
        "30d" => Ok(Duration::from_secs(30 * 24 * 3600)),
        other => Err(ApiError::bad_request(format!(
            "invalid window `{other}`: expected 24h, 7d, or 30d"
        ))),
    }
}

// --- Response types ---

#[derive(Debug, Clone, Serialize)]
struct OverviewResponse {
    window: String,
    generated_at: String,
    cached: bool,
    run_summary: RunSummary,
    top_failing_pipelines: Vec<FailingPipeline>,
    slowest_pipelines: Vec<SlowestPipeline>,
    trigger_health: TriggerHealth,
    sla_summary: SlaSummary,
    notable_events: Vec<NotableEvent>,
}

#[derive(Debug, Default, Clone, Serialize)]
struct StatusCounts {
    total: u32,
    success: u32,
    failed: u32,
    running: u32,
    pending: u32,
    cancelled: u32,
}

impl StatusCounts {
    fn count(&mut self, status: RunStatus) {
        self.total += 1;
        match status {
            RunStatus::Success => self.success += 1,
            RunStatus::Failed => self.failed += 1,
            RunStatus::Running => self.running += 1,
            RunStatus::Pending => self.pending += 1,
            RunStatus::Cancelled => self.cancelled += 1,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct RunSummary {
    #[serde(flatten)]
    counts: StatusCounts,
    by_environment: HashMap<String, StatusCounts>,
}

#[derive(Debug, Clone, Serialize)]
struct FailingPipeline {
    pipeline_name: String,
    failure_count: u32,
    last_failure_at: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SlowestPipeline {
    pipeline_name: String,
    avg_duration_ms: u64,
    max_duration_ms: u64,
    run_count: u32,
}

#[derive(Debug, Clone, Serialize)]
struct TriggerHealth {
    total: u32,
    healthy: u32,
    consecutive_failures: Vec<TriggerFailure>,
}

#[derive(Debug, Clone, Serialize)]
struct TriggerFailure {
    trigger_id: String,
    trigger_name: String,
    pipeline_id: String,
    consecutive_errors: u32,
}

#[derive(Debug, Clone, Serialize)]
struct SlaSummary {
    total: u32,
    ok: u32,
    warning: u32,
    breach: u32,
    unknown: u32,
    breaches: Vec<SlaBreach>,
}

#[derive(Debug, Clone, Serialize)]
struct SlaBreach {
    fingerprint: String,
    age: Option<String>,
    max_age: String,
    producer_pipeline: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct NotableEvent {
    kind: String,
    pipeline_name: Option<String>,
    description: String,
    at: Option<String>,
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/health/overview` — project-wide health summary.
///
/// Responses are cached in-memory for [`CACHE_TTL`] (30 s) keyed on the
/// window parameter. The `cached` field in the response indicates whether
/// the result was served from cache.
async fn overview(
    State(state): State<AppState>,
    Query(q): Query<OverviewQuery>,
) -> Result<Json<OverviewResponse>, (StatusCode, Json<ApiError>)> {
    // Check cache first.
    {
        let cache = overview_cache().lock().unwrap();
        if let Some((computed_at, cached_resp)) = cache.get(&q.window) {
            if computed_at.elapsed() < CACHE_TTL {
                let mut resp = cached_resp.clone();
                resp.cached = true;
                return Ok(Json(resp));
            }
        }
    }

    let window_dur = parse_window(&q.window)?;
    let now = SystemTime::now();
    let since = now - window_dur;

    // --- Runs ---
    let runs = state
        .run_store
        .list_runs_since(since, MAX_RUNS_SCAN)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let mut overall = StatusCounts::default();
    let mut by_env: HashMap<String, StatusCounts> = HashMap::new();

    // Per-pipeline failure tracking.
    struct PipelineFailInfo {
        count: u32,
        last_at: Option<SystemTime>,
        last_error: Option<String>,
    }
    let mut failures: HashMap<String, PipelineFailInfo> = HashMap::new();

    // Per-pipeline duration tracking (successful runs only).
    struct PipelineDurInfo {
        total_ms: u64,
        max_ms: u64,
        count: u32,
    }
    let mut durations: HashMap<String, PipelineDurInfo> = HashMap::new();

    // Track pipelines that had at least one success before their first failure
    // in this window (for "things to look at").
    let mut pipeline_first_failure: HashMap<String, (SystemTime, Option<String>)> = HashMap::new();
    let mut pipeline_had_success: HashMap<String, bool> = HashMap::new();

    // Runs are ordered most-recent-first. Process in reverse for chronological
    // ordering when building notable events.
    for run in runs.iter().rev() {
        overall.count(run.status);
        by_env
            .entry(run.environment.clone())
            .or_default()
            .count(run.status);

        if run.status == RunStatus::Failed {
            let entry = failures
                .entry(run.pipeline_name.clone())
                .or_insert(PipelineFailInfo {
                    count: 0,
                    last_at: None,
                    last_error: None,
                });
            entry.count += 1;
            if run.end_time > entry.last_at {
                entry.last_at = run.end_time;
                entry.last_error = run.error.clone();
            }
            // Record first failure for pipelines that previously succeeded.
            if !pipeline_first_failure.contains_key(&run.pipeline_name)
                && pipeline_had_success
                    .get(&run.pipeline_name)
                    .copied()
                    .unwrap_or(false)
            {
                if let Some(t) = run.end_time {
                    pipeline_first_failure
                        .insert(run.pipeline_name.clone(), (t, run.error.clone()));
                }
            }
        }

        if run.status == RunStatus::Success {
            pipeline_had_success.insert(run.pipeline_name.clone(), true);
            if let Some(dur) = run.duration_ms() {
                let entry = durations
                    .entry(run.pipeline_name.clone())
                    .or_insert(PipelineDurInfo {
                        total_ms: 0,
                        max_ms: 0,
                        count: 0,
                    });
                entry.total_ms += dur;
                entry.max_ms = entry.max_ms.max(dur);
                entry.count += 1;
            }
        }
    }

    // Build top-failing list.
    let mut top_failing: Vec<FailingPipeline> = failures
        .into_iter()
        .map(|(name, info)| FailingPipeline {
            pipeline_name: name,
            failure_count: info.count,
            last_failure_at: info.last_at.map(format_time),
            last_error: info.last_error,
        })
        .collect();
    top_failing.sort_by_key(|b| std::cmp::Reverse(b.failure_count));
    top_failing.truncate(TOP_N);

    // Build slowest list.
    let mut slowest: Vec<SlowestPipeline> = durations
        .into_iter()
        .map(|(name, info)| SlowestPipeline {
            pipeline_name: name,
            avg_duration_ms: info.total_ms / info.count as u64,
            max_duration_ms: info.max_ms,
            run_count: info.count,
        })
        .collect();
    slowest.sort_by_key(|b| std::cmp::Reverse(b.avg_duration_ms));
    slowest.truncate(TOP_N);

    // --- Trigger health ---
    let trigger_health = build_trigger_health(&state);

    // --- SLA summary ---
    let sla_summary = build_sla_summary(&state);

    // --- Notable events ("things to look at") ---
    let mut notable: Vec<NotableEvent> = Vec::new();

    // First failures of previously-healthy pipelines.
    for (name, (at, error)) in &pipeline_first_failure {
        let desc = match error {
            Some(e) => format!("First failure of previously-healthy pipeline: {e}"),
            None => "First failure of previously-healthy pipeline".to_string(),
        };
        notable.push(NotableEvent {
            kind: "first_failure".into(),
            pipeline_name: Some(name.clone()),
            description: desc,
            at: Some(format_time(*at)),
        });
    }

    // Triggers with consecutive failures.
    for tf in &trigger_health.consecutive_failures {
        notable.push(NotableEvent {
            kind: "consecutive_trigger_failure".into(),
            pipeline_name: Some(tf.pipeline_id.clone()),
            description: format!(
                "Trigger `{}` has {} consecutive errors",
                tf.trigger_name, tf.consecutive_errors
            ),
            at: None,
        });
    }

    // SLA breaches.
    for b in &sla_summary.breaches {
        notable.push(NotableEvent {
            kind: "sla_breach".into(),
            pipeline_name: b.producer_pipeline.clone(),
            description: format!(
                "Resource `{}` has breached its freshness SLA (age {} > max {})",
                b.fingerprint,
                b.age.as_deref().unwrap_or("unknown"),
                b.max_age,
            ),
            at: None,
        });
    }

    // Sort notable events: first_failure events by time (most recent first),
    // then triggers, then SLA.
    notable.sort_by(|a, b| {
        let order = |kind: &str| -> u8 {
            match kind {
                "first_failure" => 0,
                "consecutive_trigger_failure" => 1,
                "sla_breach" => 2,
                _ => 3,
            }
        };
        order(&a.kind)
            .cmp(&order(&b.kind))
            .then_with(|| b.at.cmp(&a.at))
    });

    let response = OverviewResponse {
        window: q.window.clone(),
        generated_at: format_time(now),
        cached: false,
        run_summary: RunSummary {
            counts: overall,
            by_environment: by_env,
        },
        top_failing_pipelines: top_failing,
        slowest_pipelines: slowest,
        trigger_health,
        sla_summary,
        notable_events: notable,
    };

    // Store in cache.
    {
        let mut cache = overview_cache().lock().unwrap();
        cache.insert(q.window, (Instant::now(), response.clone()));
    }

    Ok(Json(response))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_trigger_health(state: &AppState) -> TriggerHealth {
    let triggers = state
        .trigger_store
        .list_triggers(None, None)
        .unwrap_or_default();

    let total = triggers.len() as u32;
    let mut consecutive_failures = Vec::new();

    for trigger in &triggers {
        if let Ok(Some(ts)) = state.trigger_store.get_state(&trigger.id) {
            if ts.consecutive_errors > 0 {
                consecutive_failures.push(TriggerFailure {
                    trigger_id: trigger.id.0.to_string(),
                    trigger_name: trigger.name.clone(),
                    pipeline_id: trigger.pipeline_id.clone(),
                    consecutive_errors: ts.consecutive_errors,
                });
            }
        }
    }

    consecutive_failures.sort_by_key(|b| std::cmp::Reverse(b.consecutive_errors));

    let healthy = total - consecutive_failures.len() as u32;

    TriggerHealth {
        total,
        healthy,
        consecutive_failures,
    }
}

fn build_sla_summary(state: &AppState) -> SlaSummary {
    let Some(sla_store) = state.sla_store.as_ref() else {
        return SlaSummary {
            total: 0,
            ok: 0,
            warning: 0,
            breach: 0,
            unknown: 0,
            breaches: Vec::new(),
        };
    };

    let evaluations = sla_store.latest_evaluations().unwrap_or_default();
    let total = evaluations.len() as u32;
    let mut ok = 0u32;
    let mut warning = 0u32;
    let mut breach = 0u32;
    let mut unknown = 0u32;
    let mut breaches = Vec::new();

    for eval in &evaluations {
        match eval.status {
            SlaStatus::Ok => ok += 1,
            SlaStatus::Warning => warning += 1,
            SlaStatus::Breach => {
                breach += 1;
                breaches.push(SlaBreach {
                    fingerprint: eval.fingerprint.clone(),
                    age: eval.age.clone(),
                    max_age: eval.max_age.clone(),
                    producer_pipeline: eval.producer_pipeline.clone(),
                });
            }
            SlaStatus::Unknown => unknown += 1,
        }
    }

    SlaSummary {
        total,
        ok,
        warning,
        breach,
        unknown,
        breaches,
    }
}

fn format_time(t: SystemTime) -> String {
    let dur = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    // ISO 8601 UTC timestamp.
    let secs = dur.as_secs();
    let (days_since_epoch, time_of_day) = (secs / 86400, secs % 86400);
    let (hours, rem) = (time_of_day / 3600, time_of_day % 3600);
    let (minutes, seconds) = (rem / 60, rem % 60);

    // Simplified date from days since epoch.
    let (year, month, day) = days_to_ymd(days_since_epoch);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Civil calendar algorithm from Howard Hinnant.
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as u64, m, d)
}
