// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod dag;
pub mod edge;
pub mod error;
pub mod materialization;
pub mod node;
pub mod pipeline;
pub mod pipeline_store;
pub mod sample;
pub mod snapshot;
pub mod storage;
pub mod validate;
pub mod variables;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

// Re-export primary types at crate root for ergonomic imports.
pub use edge::Edge;
pub use error::{DagError, EngineError, ImportError, ImportWarnings, ValidationError};
pub use materialization::{
    ChangeDetection, FirstRun, HardDeletes, MaterializationError, MaterializationPolicy,
    OnSchemaChange, ReadMode, SnapshotPolicy, Watermark, WatermarkType, WriteStrategy,
};
pub use node::{Node, NodeId, NodeKind};
pub use pipeline::Pipeline;
pub use pipeline_store::{
    PipelineId, PipelineRecord, PipelineStoreError, PipelineVersion, SqlitePipelineStore,
};
pub use sample::SampleConfig;
pub use snapshot::{
    ClassifiedRow, CurrentTargetRow, FLUX_IS_CURRENT, FLUX_SCD_ID, FLUX_VALID_FROM, FLUX_VALID_TO,
    RowClassification, ScdColumnType, ScdMetadataColumn, SnapshotMergeStats, SnapshotPlan,
    StagedRow, check_hash, plan_snapshot_merge, scd_metadata_columns, surrogate_key,
};
pub use storage::PipelineStorage;
pub use variables::{BuiltinContext, ResolvedVariables, VariableWarning};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_set() {
        assert!(!version().is_empty());
    }
}
