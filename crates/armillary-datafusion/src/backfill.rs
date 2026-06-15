// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Backfill coordinator (planning doc 33).
//!
//! Drives a sequence of pipeline runs derived from a range definition, with
//! configurable concurrency, fail-fast, resume, and cancellation support.

use crate::error::ExecutorError;
use crate::executor::{ExecutionOptions, PipelineExecutor};
use crate::provider::ProviderRegistry;
use crate::storage::BackfillStorage;
use armillary_engine::Pipeline;
use armillary_engine::backfill::{
    Backfill, BackfillId, BackfillIteration, BackfillProgress, BackfillStatus, ExpandedIteration,
    IterationStatus, expand_range,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Semaphore, mpsc};
use tracing::info;

/// Real-time backfill progress events sent to the caller.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackfillEvent {
    BackfillStarted {
        backfill_id: BackfillId,
        total_iterations: u32,
    },
    IterationStarted {
        backfill_id: BackfillId,
        iteration_index: u32,
        iteration_key: String,
    },
    IterationCompleted {
        backfill_id: BackfillId,
        iteration_index: u32,
        iteration_key: String,
        run_id: String,
    },
    IterationFailed {
        backfill_id: BackfillId,
        iteration_index: u32,
        iteration_key: String,
        error: String,
    },
    IterationSkipped {
        backfill_id: BackfillId,
        iteration_index: u32,
        iteration_key: String,
    },
    BackfillCompleted {
        backfill_id: BackfillId,
        progress: BackfillProgress,
    },
    BackfillCancelled {
        backfill_id: BackfillId,
    },
}

/// Options for starting or resuming a backfill.
pub struct BackfillRunOptions {
    /// The pipeline to execute.
    pub pipeline: Pipeline,
    /// Provider registry for connectors.
    pub registry: Arc<ProviderRegistry>,
    /// Base execution options (environment, stores, etc). The coordinator
    /// clones these per-iteration and injects iteration-specific variable
    /// overrides and the `full_refresh` flag.
    pub base_options: ExecutionOptions,
    /// Backfill store for persisting progress.
    pub backfill_store: Arc<dyn BackfillStorage>,
    /// Optional channel for real-time backfill progress events.
    pub progress: Option<mpsc::UnboundedSender<BackfillEvent>>,
    /// Cooperative cancellation flag. Set to `true` to stop after in-flight
    /// iterations complete.
    pub cancel: Arc<AtomicBool>,
}

/// Errors from the backfill coordinator.
#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    #[error("backfill not found: {0}")]
    NotFound(String),

    #[error("range expansion failed: {0}")]
    RangeExpansion(#[from] armillary_engine::backfill::RangeError),

    #[error("store error: {0}")]
    Store(#[from] crate::error::BackfillStoreError),

    #[error("execution error on iteration {iteration_index}: {source}")]
    Execution {
        iteration_index: u32,
        source: ExecutorError,
    },
}

/// Start a new backfill: persist the record, expand iterations, and run them.
///
/// Returns the backfill ID and final progress.
pub async fn start_backfill(
    backfill: Backfill,
    opts: BackfillRunOptions,
) -> Result<(BackfillId, BackfillProgress), BackfillError> {
    let backfill_id = backfill.id.clone();

    // Expand the range.
    let expanded = expand_range(&backfill.range_definition)?;

    // Persist the backfill record.
    opts.backfill_store.create_backfill(&backfill)?;

    // Persist iteration records.
    let iterations: Vec<BackfillIteration> = expanded
        .iter()
        .map(|e| BackfillIteration {
            backfill_id: backfill_id.clone(),
            iteration_index: e.index,
            iteration_key: e.key.clone(),
            variables: e.variables.clone(),
            status: IterationStatus::Pending,
            run_id: None,
            error: None,
            started_at: None,
            completed_at: None,
        })
        .collect();
    opts.backfill_store.create_iterations(&iterations)?;

    // Run the coordinator loop.
    run_coordinator(
        &backfill_id,
        &expanded,
        backfill.concurrency,
        backfill.fail_fast,
        backfill.full_refresh,
        opts,
    )
    .await
}

/// Resume a previously started backfill, skipping already-succeeded iterations.
pub async fn resume_backfill(
    backfill_id: &BackfillId,
    opts: BackfillRunOptions,
) -> Result<(BackfillId, BackfillProgress), BackfillError> {
    let backfill = opts
        .backfill_store
        .get_backfill(backfill_id)?
        .ok_or_else(|| BackfillError::NotFound(backfill_id.0.clone()))?;

    let iterations = opts.backfill_store.list_iterations(backfill_id)?;

    // Build expanded iterations from persisted records.
    let expanded: Vec<ExpandedIteration> = iterations
        .iter()
        .map(|i| ExpandedIteration {
            index: i.iteration_index,
            key: i.iteration_key.clone(),
            variables: i.variables.clone(),
        })
        .collect();

    // Collect already-succeeded keys for skip logic.
    let succeeded_keys: std::collections::HashSet<String> = iterations
        .iter()
        .filter(|i| i.status == IterationStatus::Succeeded)
        .map(|i| i.iteration_key.clone())
        .collect();

    // Reset failed/pending iterations to pending for re-execution.
    for iter in &iterations {
        if iter.status == IterationStatus::Failed {
            opts.backfill_store.update_iteration(
                backfill_id,
                iter.iteration_index,
                IterationStatus::Pending,
                None,
                None,
                None,
                None,
            )?;
        }
    }

    // Filter to non-succeeded iterations.
    let to_run: Vec<ExpandedIteration> = expanded
        .into_iter()
        .filter(|e| !succeeded_keys.contains(&e.key))
        .collect();

    run_coordinator_with_skip(
        backfill_id,
        &to_run,
        &succeeded_keys,
        backfill.concurrency,
        backfill.fail_fast,
        backfill.full_refresh,
        opts,
    )
    .await
}

/// Cancel a running backfill by setting its status. In-flight iterations are
/// not forcefully stopped — the caller should also set the cancel flag on
/// `BackfillRunOptions` to prevent new iterations from starting.
pub fn cancel_backfill(
    backfill_id: &BackfillId,
    store: &dyn BackfillStorage,
) -> Result<(), BackfillError> {
    let now = chrono::Utc::now().to_rfc3339();
    store.update_backfill_status(backfill_id, BackfillStatus::Cancelled, None, Some(&now))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal coordinator loop
// ---------------------------------------------------------------------------

async fn run_coordinator(
    backfill_id: &BackfillId,
    iterations: &[ExpandedIteration],
    concurrency: u32,
    fail_fast: bool,
    full_refresh: bool,
    opts: BackfillRunOptions,
) -> Result<(BackfillId, BackfillProgress), BackfillError> {
    run_coordinator_with_skip(
        backfill_id,
        iterations,
        &std::collections::HashSet::new(),
        concurrency,
        fail_fast,
        full_refresh,
        opts,
    )
    .await
}

async fn run_coordinator_with_skip(
    backfill_id: &BackfillId,
    iterations: &[ExpandedIteration],
    skipped_keys: &std::collections::HashSet<String>,
    concurrency: u32,
    fail_fast: bool,
    full_refresh: bool,
    opts: BackfillRunOptions,
) -> Result<(BackfillId, BackfillProgress), BackfillError> {
    let now = chrono::Utc::now().to_rfc3339();
    opts.backfill_store.update_backfill_status(
        backfill_id,
        BackfillStatus::Running,
        Some(&now),
        None,
    )?;

    if let Some(tx) = &opts.progress {
        let total = iterations.len() as u32 + skipped_keys.len() as u32;
        let _ = tx.send(BackfillEvent::BackfillStarted {
            backfill_id: backfill_id.clone(),
            total_iterations: total,
        });
    }

    let semaphore = Arc::new(Semaphore::new(concurrency as usize));
    let had_failure = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    for expanded in iterations {
        // Check cancellation before starting new iterations.
        if opts.cancel.load(Ordering::Relaxed) {
            info!(backfill_id = %backfill_id, "backfill cancelled, stopping new iterations");
            break;
        }

        // Check fail-fast.
        if fail_fast && had_failure.load(Ordering::Relaxed) {
            info!(backfill_id = %backfill_id, "fail_fast: stopping after failure");
            break;
        }

        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let pipeline = opts.pipeline.clone();
        let registry = opts.registry.clone();
        let store = opts.backfill_store.clone();
        let bf_id = backfill_id.clone();
        let cancel = opts.cancel.clone();
        let progress_tx = opts.progress.clone();
        let had_failure = had_failure.clone();
        let iter = expanded.clone();

        // Build per-iteration execution options.
        let mut iter_options = ExecutionOptions {
            environment: opts.base_options.environment.clone(),
            run_store: opts.base_options.run_store.clone(),
            cancel: cancel.clone(),
            environment_resolver: opts.base_options.environment_resolver.clone(),
            progress: opts.base_options.progress.clone(),
            variable_overrides: opts.base_options.variable_overrides.clone(),
            secret_resolver: opts.base_options.secret_resolver.clone(),
            session_factory: opts.base_options.session_factory.clone(),
            incremental_state_store: opts.base_options.incremental_state_store.clone(),
            full_refresh,
            bootstrap_incremental: opts.base_options.bootstrap_incremental,
            dry_run_no_sinks: opts.base_options.dry_run_no_sinks,
            lineage_store: opts.base_options.lineage_store.clone(),
            fingerprint_fn: opts.base_options.fingerprint_fn,
            pipeline_id: opts.base_options.pipeline_id.clone(),
            column_lineage_store: opts.base_options.column_lineage_store.clone(),
            on_column_lineage_updated: opts.base_options.on_column_lineage_updated.clone(),
            triggered_by: opts.base_options.triggered_by.clone(),
            openlineage_client: opts.base_options.openlineage_client.clone(),
        };

        // Merge iteration variables into the overrides.
        for (k, v) in &iter.variables {
            iter_options.variable_overrides.insert(k.clone(), v.clone());
        }

        let handle = tokio::spawn(async move {
            let _permit = permit;

            // Mark running.
            let start_ts = chrono::Utc::now().to_rfc3339();
            let _ = store.update_iteration(
                &bf_id,
                iter.index,
                IterationStatus::Running,
                None,
                None,
                Some(&start_ts),
                None,
            );

            if let Some(tx) = &progress_tx {
                let _ = tx.send(BackfillEvent::IterationStarted {
                    backfill_id: bf_id.clone(),
                    iteration_index: iter.index,
                    iteration_key: iter.key.clone(),
                });
            }

            // Execute the pipeline.
            let result = PipelineExecutor::execute(&pipeline, &registry, &iter_options).await;

            let end_ts = chrono::Utc::now().to_rfc3339();
            match result {
                Ok((_result, run)) => {
                    let _ = store.update_iteration(
                        &bf_id,
                        iter.index,
                        IterationStatus::Succeeded,
                        Some(&run.id.to_string()),
                        None,
                        None,
                        Some(&end_ts),
                    );
                    if let Some(tx) = &progress_tx {
                        let _ = tx.send(BackfillEvent::IterationCompleted {
                            backfill_id: bf_id.clone(),
                            iteration_index: iter.index,
                            iteration_key: iter.key.clone(),
                            run_id: run.id.to_string(),
                        });
                    }
                }
                Err(e) => {
                    had_failure.store(true, Ordering::Relaxed);
                    let error_msg = e.to_string();
                    let _ = store.update_iteration(
                        &bf_id,
                        iter.index,
                        IterationStatus::Failed,
                        None,
                        Some(&error_msg),
                        None,
                        Some(&end_ts),
                    );
                    if let Some(tx) = &progress_tx {
                        let _ = tx.send(BackfillEvent::IterationFailed {
                            backfill_id: bf_id.clone(),
                            iteration_index: iter.index,
                            iteration_key: iter.key.clone(),
                            error: error_msg,
                        });
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all spawned iterations.
    for handle in handles {
        let _ = handle.await;
    }

    // Determine final status.
    let progress = opts.backfill_store.get_progress(backfill_id)?;
    let final_status = if opts.cancel.load(Ordering::Relaxed) {
        if let Some(tx) = &opts.progress {
            let _ = tx.send(BackfillEvent::BackfillCancelled {
                backfill_id: backfill_id.clone(),
            });
        }
        BackfillStatus::Cancelled
    } else if progress.failed > 0 {
        BackfillStatus::Failed
    } else {
        BackfillStatus::Completed
    };

    let end_ts = chrono::Utc::now().to_rfc3339();
    opts.backfill_store
        .update_backfill_status(backfill_id, final_status, None, Some(&end_ts))?;

    if let Some(tx) = &opts.progress {
        let _ = tx.send(BackfillEvent::BackfillCompleted {
            backfill_id: backfill_id.clone(),
            progress: progress.clone(),
        });
    }

    info!(
        backfill_id = %backfill_id,
        status = ?final_status,
        succeeded = progress.succeeded,
        failed = progress.failed,
        "backfill finished"
    );

    Ok((backfill_id.clone(), progress))
}
