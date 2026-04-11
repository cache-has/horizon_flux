// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod backfill;
pub mod catalog;
pub mod column_lineage;
pub mod dag;
pub mod edge;
pub mod error;
pub mod lineage;
pub mod materialization;
pub mod node;
pub mod pipeline;
pub mod pipeline_store;
pub mod sample;
pub mod sla;
pub mod snapshot;
pub mod snippet;
pub mod storage;
pub mod validate;
pub mod variables;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

// Re-export primary types at crate root for ergonomic imports.
pub use backfill::{
    Backfill, BackfillId, BackfillIteration, BackfillProgress, BackfillStatus, DateGranularity,
    ExpandedIteration, IterationStatus, RangeDefinition, RangeError,
};
pub use catalog::{
    AnnotationFile, AnnotationOwner, AnnotationResource, AutoDerivedFacts, Catalog, CatalogEntry,
    CatalogError, CatalogWarning, ColumnAnnotation, MergedColumn, ResourceAnnotation, SchemaColumn,
    SearchIndex,
};
pub use column_lineage::{
    BoundaryColumn, ColumnEdge, ColumnKey, ColumnLineageGraph, ColumnRef, Confidence,
    CrossPipelineColumnEdge, CrossPipelineColumnLineage, NodeColumnLineage, OneSidedColumn,
    OneSidedKind, RelationshipKind, TraceEdge, TraceOptions, TraceResult, canonicalize_column,
    derive_cross_pipeline_column_lineage,
};
pub use edge::Edge;
pub use error::{DagError, EngineError, ImportError, ImportWarnings, ValidationError};
pub use lineage::{
    BindingDirection, EdgeSource, FingerprintFn, LineageEdge, LineageGraph, ResourceBinding,
    ResourceFingerprint,
};
pub use materialization::{
    ChangeDetection, FirstRun, HardDeletes, MaterializationError, MaterializationPolicy,
    OnSchemaChange, ReadMode, SnapshotPolicy, Watermark, WatermarkType, WriteStrategy,
};
pub use node::{Assertion, Node, NodeId, NodeKind, TestConfig, TestSeverity};
pub use pipeline::Pipeline;
pub use pipeline_store::{
    PipelineId, PipelineRecord, PipelineStoreError, PipelineVersion, SqlitePipelineStore,
};
pub use sample::SampleConfig;
pub use sla::{
    FreshnessConfig, SlaConfig, SlaEvaluation, SlaScope, SlaStatus, format_iso_duration,
    parse_iso_duration,
};
pub use snapshot::{
    ClassifiedRow, CurrentTargetRow, FLUX_IS_CURRENT, FLUX_SCD_ID, FLUX_VALID_FROM, FLUX_VALID_TO,
    RowClassification, ScdColumnType, ScdMetadataColumn, SnapshotMergeStats, SnapshotPlan,
    StagedRow, check_hash, plan_snapshot_merge, scd_metadata_columns, surrogate_key,
};
pub use snippet::{SnippetError, SnippetRegistry, expand_snippets};
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
