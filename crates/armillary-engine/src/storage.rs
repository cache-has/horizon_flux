// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Storage trait abstraction for pipeline metadata.
//!
//! [`PipelineStorage`] defines the public API that any pipeline metadata
//! backend must implement. The default implementation is
//! [`SqlitePipelineStore`](crate::pipeline_store::SqlitePipelineStore).

use crate::pipeline::Pipeline;
use crate::pipeline_store::{PipelineId, PipelineRecord, PipelineStoreError, PipelineVersion};

/// Backend-agnostic storage interface for pipeline definitions, metadata,
/// and version history.
///
/// Implementations must be safe to share across threads (`Send + Sync`).
/// The default backend is SQLite; a PostgreSQL backend is planned.
pub trait PipelineStorage: Send + Sync {
    /// Create a new pipeline. Returns the created record.
    ///
    /// The pipeline's version is set to 1 and an initial version snapshot is
    /// stored in the history.
    fn create(&self, pipeline: Pipeline) -> Result<PipelineRecord, PipelineStoreError>;

    /// Get a pipeline by ID.
    fn get(&self, id: &PipelineId) -> Result<Option<PipelineRecord>, PipelineStoreError>;

    /// Get a pipeline by name.
    fn get_by_name(&self, name: &str) -> Result<Option<PipelineRecord>, PipelineStoreError>;

    /// List all pipelines, ordered by name.
    fn list(&self, limit: u32, offset: u32) -> Result<Vec<PipelineRecord>, PipelineStoreError>;

    /// Get the total count of pipelines.
    fn count(&self) -> Result<u32, PipelineStoreError>;

    /// Update an existing pipeline. Returns the updated record.
    ///
    /// The pipeline's version is auto-incremented and a version snapshot is
    /// stored in the history.
    fn update(
        &self,
        id: &PipelineId,
        pipeline: Pipeline,
    ) -> Result<PipelineRecord, PipelineStoreError>;

    /// Delete a pipeline by ID.
    fn delete(&self, id: &PipelineId) -> Result<(), PipelineStoreError>;

    /// List version history for a pipeline, newest first.
    fn list_versions(
        &self,
        id: &PipelineId,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<PipelineVersion>, PipelineStoreError>;

    /// Get a specific version snapshot.
    fn get_version(
        &self,
        id: &PipelineId,
        version: u32,
    ) -> Result<Option<PipelineVersion>, PipelineStoreError>;

    /// Count versions for a pipeline.
    fn count_versions(&self, id: &PipelineId) -> Result<u32, PipelineStoreError>;

    /// Record that a pipeline was executed. Updates `last_run_at` and
    /// increments `run_count`.
    fn record_run(&self, id: &PipelineId) -> Result<(), PipelineStoreError>;
}
