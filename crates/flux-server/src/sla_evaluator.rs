// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Background SLA evaluator (planning doc 37, sub-feature 3).
//!
//! Periodically walks all resources with freshness SLAs, computes their current
//! status, persists the result, and emits observability events for breaches and
//! warnings.

use crate::state::AppState;
use flux_datafusion::RunStatus;
use flux_engine::catalog::{self, Catalog};
use flux_engine::lineage::{LineageGraph, ResourceBinding};
use flux_engine::pipeline_store::PipelineId;
use flux_engine::sla::{SlaEvaluation, SlaStatus, format_iso_duration, parse_iso_duration};
use flux_observability::emit_event;
use flux_observability::events::{FluxEvent, SlaBreach, SlaWarning};
use tracing::{info, warn};

/// Run one SLA evaluation tick. Called periodically by the background task.
///
/// Returns the number of resources evaluated.
pub fn evaluate_slas(state: &AppState) -> usize {
    let sla_store = match state.sla_store.as_ref() {
        Some(s) => s,
        None => return 0,
    };

    // Build the catalog to discover which resources have SLA configs.
    let catalog = match build_catalog_for_sla(state) {
        Ok(c) => c,
        Err(e) => {
            warn!("SLA evaluator: failed to build catalog: {e}");
            return 0;
        }
    };

    let now = chrono::Utc::now();
    let now_iso = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // Collect all resources with SLA configs from annotation metadata.
    let metadata_dir = state
        .metadata_dir
        .as_deref()
        .unwrap_or_else(|| std::path::Path::new("metadata"));
    let (annotations, _) = catalog::load_annotations(metadata_dir);

    let mut evaluations = Vec::new();

    for (fp, ann_file) in &annotations {
        let sla = match &ann_file.annotation.sla {
            Some(s) => s,
            None => continue,
        };

        let max_age = match parse_iso_duration(&sla.freshness.max_age) {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "SLA evaluator: invalid max_age '{}' for {}: {e}",
                    sla.freshness.max_age, fp.0
                );
                continue;
            }
        };

        let warn_at = sla
            .freshness
            .warn_at
            .as_deref()
            .and_then(|s| parse_iso_duration(s).ok());

        // Find the latest successful producing run for this resource.
        let catalog_entry = catalog.get(fp);
        let producer_pipeline = catalog_entry
            .and_then(|e| e.derived.producers.first())
            .and_then(|p| {
                state
                    .pipeline_store
                    .get(&p.pipeline_id)
                    .ok()
                    .flatten()
                    .map(|r| r.pipeline.name.clone())
            });

        let last_success = producer_pipeline.as_ref().and_then(|name| {
            state
                .run_store
                .list_runs(Some(name), 10, 0)
                .ok()?
                .into_iter()
                .find(|r| r.status == RunStatus::Success)
        });

        let (status, age_duration) = match &last_success {
            Some(run) => {
                if let Some(end_time) = run.end_time {
                    let end_chrono = chrono::DateTime::from_timestamp(
                        end_time
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64,
                        0,
                    )
                    .unwrap_or(chrono::DateTime::UNIX_EPOCH);

                    let age = now.signed_duration_since(end_chrono);

                    if age > max_age {
                        (SlaStatus::Breach, Some(age))
                    } else if warn_at.is_some_and(|w| age > w) {
                        (SlaStatus::Warning, Some(age))
                    } else {
                        (SlaStatus::Ok, Some(age))
                    }
                } else {
                    (SlaStatus::Unknown, None)
                }
            }
            None => (SlaStatus::Unknown, None),
        };

        let last_success_at = last_success.as_ref().and_then(|r| {
            r.end_time.map(|t| {
                let secs = t
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                chrono::DateTime::from_timestamp(secs, 0)
                    .unwrap_or(chrono::DateTime::UNIX_EPOCH)
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string()
            })
        });

        let eval = SlaEvaluation {
            fingerprint: fp.0.clone(),
            evaluated_at: now_iso.clone(),
            status,
            age: age_duration.as_ref().map(format_iso_duration),
            max_age: sla.freshness.max_age.clone(),
            warn_at: sla.freshness.warn_at.clone(),
            producer_pipeline: producer_pipeline.clone(),
            last_success_at,
        };

        // Emit observability events for breach/warning.
        match status {
            SlaStatus::Breach => {
                emit_event!(FluxEvent::SlaBreach(SlaBreach {
                    resource_fingerprint: fp.0.clone(),
                    age: eval.age.clone().unwrap_or_default(),
                    max_age: sla.freshness.max_age.clone(),
                    producer_pipeline: producer_pipeline.clone(),
                    last_success_at: eval.last_success_at.clone(),
                }));
                flux_observability::metrics::record_sla_status(&fp.0, "breach");
            }
            SlaStatus::Warning => {
                emit_event!(FluxEvent::SlaWarning(SlaWarning {
                    resource_fingerprint: fp.0.clone(),
                    age: eval.age.clone().unwrap_or_default(),
                    warn_at: sla.freshness.warn_at.clone().unwrap_or_default(),
                    max_age: sla.freshness.max_age.clone(),
                    producer_pipeline: producer_pipeline.clone(),
                    last_success_at: eval.last_success_at.clone(),
                }));
                flux_observability::metrics::record_sla_status(&fp.0, "warning");
            }
            SlaStatus::Ok => {
                flux_observability::metrics::record_sla_status(&fp.0, "ok");
            }
            SlaStatus::Unknown => {
                flux_observability::metrics::record_sla_status(&fp.0, "unknown");
            }
        }

        evaluations.push(eval);
    }

    let count = evaluations.len();

    // Persist evaluations.
    if !evaluations.is_empty() {
        if let Err(e) = sla_store.save_evaluations(&evaluations) {
            warn!("SLA evaluator: failed to persist evaluations: {e}");
        }
    }

    if count > 0 {
        info!("SLA evaluator: evaluated {count} resources");
    }

    count
}

/// Build a catalog for SLA evaluation using the same approach as the catalog API.
fn build_catalog_for_sla(state: &AppState) -> Result<Catalog, String> {
    let environment = "default";

    let stored = state
        .lineage_store
        .all_bindings(environment)
        .map_err(|e| e.to_string())?;

    let bindings: Vec<ResourceBinding> = stored
        .into_iter()
        .filter_map(|sb| {
            let pipeline_id = sb.pipeline_id.parse::<PipelineId>().ok()?;
            Some(ResourceBinding {
                pipeline_id,
                node_id: sb.node_id,
                direction: sb.direction,
                fingerprint: sb.resource_fingerprint,
            })
        })
        .collect();

    let graph = LineageGraph {
        edges: Vec::new(),
        bindings,
    };

    let metadata_dir = state
        .metadata_dir
        .as_deref()
        .unwrap_or_else(|| std::path::Path::new("metadata"));

    Ok(Catalog::build(&graph, metadata_dir))
}

/// Run the SLA evaluator as a periodic background task.
///
/// Evaluates SLAs every `interval` until the shutdown signal is received.
pub async fn run_sla_evaluator_loop(
    state: AppState,
    interval: std::time::Duration,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    info!("SLA evaluator started (interval: {}s)", interval.as_secs());
    let mut ticker = tokio::time::interval(interval);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                // Run evaluation on a blocking thread since storage calls are sync.
                let state_clone = state.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    evaluate_slas(&state_clone)
                })
                .await;
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("SLA evaluator shutting down");
                    break;
                }
            }
        }
    }
}
