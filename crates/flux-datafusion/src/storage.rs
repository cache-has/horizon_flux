// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Storage trait abstractions for run history and environment metadata.
//!
//! [`RunStorage`] and [`EnvironmentStorage`] define the public APIs that any
//! metadata backend must implement. The default implementations are
//! [`SqliteRunStore`](crate::run_store::SqliteRunStore) and
//! [`SqliteEnvironmentStore`](crate::environment::SqliteEnvironmentStore).

use crate::environment::{Environment, TableOverride};
use crate::error::{EnvironmentError, IncrementalStateError, RunStoreError};
use crate::incremental_state::{IncrementalSchemaRecord, IncrementalState};
use crate::run::{NodeRunStats, PipelineRun, RunId, RunStatus};
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

    /// Transition a run to `Running` and record the start time.
    fn set_running(&self, run_id: &RunId, start_time: SystemTime) -> Result<(), RunStoreError>;

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

    /// Load a run by ID, including its node stats.
    fn get_run(&self, run_id: &RunId) -> Result<Option<PipelineRun>, RunStoreError>;

    /// List runs, optionally filtered by pipeline name, ordered by most recent first.
    fn list_runs(
        &self,
        pipeline_name: Option<&str>,
        limit: u32,
    ) -> Result<Vec<PipelineRun>, RunStoreError>;
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
