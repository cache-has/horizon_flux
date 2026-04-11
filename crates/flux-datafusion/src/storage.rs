// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Storage trait abstractions for run history and environment metadata.
//!
//! [`RunStorage`] and [`EnvironmentStorage`] define the public APIs that any
//! metadata backend must implement. The default implementations are
//! [`SqliteRunStore`](crate::run_store::SqliteRunStore) and
//! [`SqliteEnvironmentStore`](crate::environment::SqliteEnvironmentStore).

use crate::environment::{Environment, TableOverride};
use crate::error::{
    BackfillStoreError, ColumnLineageStoreError, EnvironmentError, IncrementalStateError,
    LineageStoreError, RunStoreError,
};
use crate::failure_report::FailureReport;
use crate::incremental_state::{IncrementalSchemaRecord, IncrementalState};
use crate::run::{NodeRunStats, PipelineRun, RunId, RunStatus, TestResultSummary};
use flux_engine::backfill::{
    Backfill, BackfillId, BackfillIteration, BackfillProgress, BackfillStatus, IterationStatus,
};
use flux_engine::column_lineage::ColumnEdge;
use flux_engine::lineage::{BindingDirection, ResourceFingerprint};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Backend-agnostic storage interface for pipeline execution history.
///
/// Implementations must be safe to share across threads (`Send + Sync`).
pub trait RunStorage: Send + Sync {
    /// Create a new run in `Pending` status and persist it.
    fn create_run(
        &self,
        pipeline_name: &str,
        environment: &str,
    ) -> Result<PipelineRun, RunStoreError>;

    /// Transition a run to `Running` and record the start time and trigger
    /// attribution.
    fn set_running(
        &self,
        run_id: &RunId,
        start_time: SystemTime,
        triggered_by: Option<&str>,
    ) -> Result<(), RunStoreError>;

    /// Mark a run as finished (success, failed, or cancelled).
    fn finish_run(
        &self,
        run_id: &RunId,
        status: RunStatus,
        end_time: SystemTime,
        error: Option<&str>,
    ) -> Result<(), RunStoreError>;

    /// Persist statistics for a single node within a run.
    fn save_node_stats(&self, run_id: &RunId, stats: &NodeRunStats) -> Result<(), RunStoreError>;

    /// Persist test results for a run.
    fn save_test_results(
        &self,
        run_id: &RunId,
        results: &[TestResultSummary],
    ) -> Result<(), RunStoreError>;

    /// Load a run by ID, including its node stats.
    fn get_run(&self, run_id: &RunId) -> Result<Option<PipelineRun>, RunStoreError>;

    /// List runs, optionally filtered by pipeline name, ordered by most recent first.
    fn list_runs(
        &self,
        pipeline_name: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<PipelineRun>, RunStoreError>;

    /// Count runs, optionally filtered by pipeline name.
    fn count_runs(&self, pipeline_name: Option<&str>) -> Result<u32, RunStoreError>;

    /// List runs started after `since`, ordered by most recent first.
    ///
    /// Returns lightweight run metadata **without** per-node stats (the
    /// `node_stats` vec is empty). Designed for aggregation queries like the
    /// health dashboard where per-node detail is not needed.
    fn list_runs_since(
        &self,
        since: SystemTime,
        limit: u32,
    ) -> Result<Vec<PipelineRun>, RunStoreError>;

    /// Persist a failure report for a node within a run (planning doc 37).
    fn save_failure_report(&self, report: &FailureReport) -> Result<(), RunStoreError>;

    /// Load the failure report for a specific node within a run, if one exists.
    fn get_failure_report(
        &self,
        run_id: &RunId,
        node_id: &str,
    ) -> Result<Option<FailureReport>, RunStoreError>;
}

/// Backend-agnostic storage interface for environment metadata and table
/// override tracking.
///
/// Implementations must be safe to share across threads (`Send + Sync`).
pub trait EnvironmentStorage: Send + Sync {
    /// Create a new environment. Returns an error if it already exists or the
    /// fallback environment does not exist.
    fn create(&self, name: &str, fallback: Option<&str>) -> Result<Environment, EnvironmentError>;

    /// Delete an environment. Cannot delete `prod`.
    fn delete(&self, name: &str) -> Result<(), EnvironmentError>;

    /// Get a single environment by name.
    fn get(&self, name: &str) -> Result<Option<Environment>, EnvironmentError>;

    /// List all environments.
    fn list(&self) -> Result<Vec<Environment>, EnvironmentError>;

    /// Update the fallback chain for an environment.
    fn update_fallback(&self, name: &str, fallback: Option<&str>) -> Result<(), EnvironmentError>;

    /// Compute the full fallback chain starting from the given environment.
    fn fallback_chain(&self, start: &str) -> Result<Vec<String>, EnvironmentError>;

    /// Record that a table override exists in the given environment.
    fn register_table_override(
        &self,
        environment: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), EnvironmentError>;

    /// Remove a table override from an environment.
    fn deregister_table_override(
        &self,
        environment: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<bool, EnvironmentError>;

    /// List all table overrides in an environment.
    fn list_table_overrides(
        &self,
        environment: &str,
    ) -> Result<Vec<TableOverride>, EnvironmentError>;
}

/// Backend-agnostic storage interface for incremental sink materialization
/// state (planning doc 27).
///
/// One row of [`IncrementalState`] is kept per
/// `(pipeline_id, node_id, environment)`. Schema observations are appended to
/// a separate history table and queried via [`latest_schema`].
///
/// Implementations must be safe to share across threads (`Send + Sync`).
pub trait IncrementalStateStorage: Send + Sync {
    /// Load the latest state for a node, or `None` if no run has been recorded.
    fn load_state(
        &self,
        pipeline_id: &str,
        node_id: &str,
        environment: &str,
    ) -> Result<Option<IncrementalState>, IncrementalStateError>;

    /// Upsert the latest state for a node.
    ///
    /// This is intended to be called immediately after a successful sink
    /// commit. Full transactional coupling with the sink write is tracked
    /// separately in planning doc 27 alongside the executor coordinator task.
    fn save_state(&self, state: &IncrementalState) -> Result<(), IncrementalStateError>;

    /// Delete the state for a node, forcing the next run to be a first run.
    /// Returns `true` if a row was actually removed.
    fn reset_state(
        &self,
        pipeline_id: &str,
        node_id: &str,
        environment: &str,
    ) -> Result<bool, IncrementalStateError>;

    /// List all incremental state, optionally filtered by environment.
    fn list_states(
        &self,
        environment: Option<&str>,
    ) -> Result<Vec<IncrementalState>, IncrementalStateError>;

    /// Append a schema observation to the history table.
    fn record_schema(&self, record: &IncrementalSchemaRecord) -> Result<(), IncrementalStateError>;

    /// Fetch the most recent schema observation for a node, if any.
    fn latest_schema(
        &self,
        pipeline_id: &str,
        node_id: &str,
        environment: &str,
    ) -> Result<Option<IncrementalSchemaRecord>, IncrementalStateError>;

    /// Backfill a state row from a remote metadata store, preserving its
    /// original values. Skips the row if one already exists for the same key.
    fn import_state(&self, state: &IncrementalState) -> Result<(), IncrementalStateError>;

    /// Backfill a schema observation from a remote metadata store. Skips the
    /// row if one with the same `(pipeline_id, node_id, environment, run_id)`
    /// already exists.
    fn import_schema_record(
        &self,
        record: &IncrementalSchemaRecord,
    ) -> Result<(), IncrementalStateError>;
}

// ---------------------------------------------------------------------------
// LineageStorage (planning doc 31)
// ---------------------------------------------------------------------------

/// A persisted resource binding from a pipeline node to an external resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredResourceBinding {
    pub pipeline_id: String,
    pub node_id: String,
    pub direction: BindingDirection,
    pub resource_fingerprint: ResourceFingerprint,
    pub environment: String,
    pub updated_at_ms: i64,
}

/// A runtime-observed lineage event: a source read or sink write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageObservation {
    pub pipeline_id: String,
    pub node_id: String,
    pub run_id: String,
    pub direction: BindingDirection,
    pub resource_fingerprint: ResourceFingerprint,
    pub environment: String,
    pub observed_at_ms: i64,
}

/// Backend-agnostic storage interface for cross-pipeline lineage metadata
/// (planning doc 31).
///
/// Stores two kinds of data:
/// - **Resource bindings** — static mappings from pipeline nodes to external
///   resources, updated on every pipeline save.
/// - **Lineage observations** — runtime-observed resource accesses, recorded
///   on every successful source read and sink write.
///
/// Implementations must be safe to share across threads (`Send + Sync`).
pub trait LineageStorage: Send + Sync {
    /// Replace all resource bindings for a pipeline in the given environment.
    ///
    /// Deletes any existing bindings for `(pipeline_id, environment)` and
    /// inserts the new set. This is an atomic replace: a pipeline save
    /// recomputes all fingerprints and stores them in one shot.
    fn save_bindings(
        &self,
        pipeline_id: &str,
        environment: &str,
        bindings: &[StoredResourceBinding],
    ) -> Result<(), LineageStoreError>;

    /// Load all resource bindings for a specific pipeline and environment.
    fn load_bindings(
        &self,
        pipeline_id: &str,
        environment: &str,
    ) -> Result<Vec<StoredResourceBinding>, LineageStoreError>;

    /// Load all resource bindings across all pipelines in an environment.
    fn all_bindings(
        &self,
        environment: &str,
    ) -> Result<Vec<StoredResourceBinding>, LineageStoreError>;

    /// Delete all bindings for a pipeline (all environments).
    fn delete_bindings(&self, pipeline_id: &str) -> Result<(), LineageStoreError>;

    /// Record an observed lineage event (source read or sink write).
    fn record_observation(&self, observation: &LineageObservation)
    -> Result<(), LineageStoreError>;

    /// Query observations within a time window for an environment.
    fn query_observations(
        &self,
        environment: &str,
        since_ms: i64,
    ) -> Result<Vec<LineageObservation>, LineageStoreError>;

    /// Delete observations older than the given timestamp.
    /// Returns the number of rows deleted.
    fn enforce_retention(&self, older_than_ms: i64) -> Result<u64, LineageStoreError>;
}

// ---------------------------------------------------------------------------
// ColumnLineageStorage (planning doc 35)
// ---------------------------------------------------------------------------

/// A persisted column-level lineage edge, enriched with pipeline/environment
/// context for storage and cross-pipeline queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredColumnEdge {
    /// Auto-generated row ID (populated on load, ignored on save).
    pub id: Option<i64>,
    pub pipeline_id: String,
    pub environment: String,
    /// The edge payload (upstream/downstream columns, relationship, etc.).
    pub edge: ColumnEdge,
    /// ISO-8601 timestamp of when this edge was derived.
    pub derived_at: String,
    /// Optional run ID that triggered the derivation.
    pub source_run_id: Option<String>,
}

/// Backend-agnostic storage interface for column-level lineage edges
/// (planning doc 35).
///
/// Edges are derived on pipeline save or execution. The primary write
/// operation is an atomic replacement of all edges for a
/// `(pipeline_id, environment)` pair.
///
/// Implementations must be safe to share across threads (`Send + Sync`).
pub trait ColumnLineageStorage: Send + Sync {
    /// Replace all column lineage edges for a pipeline in the given environment.
    ///
    /// Deletes any existing edges for `(pipeline_id, environment)` and inserts
    /// the new set atomically.
    fn save_column_edges(
        &self,
        pipeline_id: &str,
        environment: &str,
        edges: &[StoredColumnEdge],
    ) -> Result<(), ColumnLineageStoreError>;

    /// Load all column lineage edges for a pipeline and environment.
    fn load_column_edges(
        &self,
        pipeline_id: &str,
        environment: &str,
    ) -> Result<Vec<StoredColumnEdge>, ColumnLineageStoreError>;

    /// Load column lineage edges for a specific node within a pipeline.
    fn load_column_edges_for_node(
        &self,
        pipeline_id: &str,
        environment: &str,
        node_id: &str,
    ) -> Result<Vec<StoredColumnEdge>, ColumnLineageStoreError>;

    /// Load all column lineage edges across all pipelines in an environment.
    fn all_column_edges(
        &self,
        environment: &str,
    ) -> Result<Vec<StoredColumnEdge>, ColumnLineageStoreError>;

    /// Delete all column lineage edges for a pipeline (all environments).
    fn delete_column_edges(&self, pipeline_id: &str) -> Result<(), ColumnLineageStoreError>;

    /// Delete edges older than the given ISO-8601 timestamp.
    /// Returns the number of rows deleted.
    fn enforce_column_lineage_retention(
        &self,
        older_than: &str,
    ) -> Result<u64, ColumnLineageStoreError>;
}

// ---------------------------------------------------------------------------
// BackfillStorage (planning doc 33)
// ---------------------------------------------------------------------------

/// Backend-agnostic storage interface for backfill metadata.
///
/// Implementations must be safe to share across threads (`Send + Sync`).
pub trait BackfillStorage: Send + Sync {
    /// Insert a new backfill record.
    fn create_backfill(&self, backfill: &Backfill) -> Result<(), BackfillStoreError>;

    /// Insert a batch of iteration records for a backfill.
    fn create_iterations(&self, iterations: &[BackfillIteration])
    -> Result<(), BackfillStoreError>;

    /// Load a backfill by ID.
    fn get_backfill(&self, id: &BackfillId) -> Result<Option<Backfill>, BackfillStoreError>;

    /// List backfills, optionally filtered by pipeline and/or status.
    fn list_backfills(
        &self,
        pipeline_id: Option<&str>,
        status: Option<BackfillStatus>,
        limit: u32,
    ) -> Result<Vec<Backfill>, BackfillStoreError>;

    /// Load all iterations for a backfill.
    fn list_iterations(
        &self,
        backfill_id: &BackfillId,
    ) -> Result<Vec<BackfillIteration>, BackfillStoreError>;

    /// Update the top-level backfill status and optional timestamps.
    fn update_backfill_status(
        &self,
        id: &BackfillId,
        status: BackfillStatus,
        started_at: Option<&str>,
        completed_at: Option<&str>,
    ) -> Result<(), BackfillStoreError>;

    /// Update a single iteration's status, run_id, error, and timestamps.
    #[allow(clippy::too_many_arguments)]
    fn update_iteration(
        &self,
        backfill_id: &BackfillId,
        iteration_index: u32,
        status: IterationStatus,
        run_id: Option<&str>,
        error: Option<&str>,
        started_at: Option<&str>,
        completed_at: Option<&str>,
    ) -> Result<(), BackfillStoreError>;

    /// Compute aggregated progress for a backfill.
    fn get_progress(
        &self,
        backfill_id: &BackfillId,
    ) -> Result<BackfillProgress, BackfillStoreError>;

    /// Delete a backfill and its iterations.
    fn delete_backfill(&self, id: &BackfillId) -> Result<bool, BackfillStoreError>;
}

// ---------------------------------------------------------------------------
// SlaStorage (planning doc 37, sub-feature 3)
// ---------------------------------------------------------------------------

/// Backend-agnostic storage interface for SLA evaluations.
///
/// Stores the result of periodic freshness checks so that the API can serve
/// current and historical SLA status without re-evaluating on every request.
///
/// Implementations must be safe to share across threads (`Send + Sync`).
pub trait SlaStorage: Send + Sync {
    /// Persist a batch of SLA evaluations (one per resource evaluated).
    fn save_evaluations(
        &self,
        evaluations: &[flux_engine::SlaEvaluation],
    ) -> Result<(), RunStoreError>;

    /// Load the most recent evaluation for each resource with an SLA.
    /// Used by the compliance dashboard.
    fn latest_evaluations(&self) -> Result<Vec<flux_engine::SlaEvaluation>, RunStoreError>;

    /// Load the most recent evaluation for a specific resource.
    fn latest_evaluation(
        &self,
        fingerprint: &str,
    ) -> Result<Option<flux_engine::SlaEvaluation>, RunStoreError>;

    /// Load historical evaluations for a resource, ordered most-recent first.
    fn evaluation_history(
        &self,
        fingerprint: &str,
        limit: u32,
    ) -> Result<Vec<flux_engine::SlaEvaluation>, RunStoreError>;
}
