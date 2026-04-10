// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod backfill;
pub mod backfill_store;
pub mod column_lineage;
pub mod column_stats;
pub mod environment;
pub mod error;
pub mod executor;
pub mod friendly_sql;
pub mod incremental_coordinator;
pub mod incremental_state;
pub mod output_cache;
pub mod preview;
pub mod provider;
pub mod python_env;
pub mod python_runtime;
pub mod resolver;
pub mod result;
pub mod run;
pub mod run_store;
pub mod schema_diff;
pub mod session;
pub mod stats;
pub mod storage;
pub mod test_assertion;
pub mod udfs;
pub mod watermark;

pub use backfill::{BackfillError, BackfillEvent, BackfillRunOptions};
pub use backfill_store::SqliteBackfillStore;
pub use column_lineage::{
    derive_column_lineage, derive_opaque_lineage, derive_sink_boundary_lineage,
    derive_source_boundary_lineage,
};
pub use column_stats::{ColumnStats, compute_column_stats};
pub use environment::{Environment, SqliteEnvironmentStore, TableOverride};
pub use error::{
    BackfillStoreError, ColumnLineageStoreError, IncrementalStateError, LineageStoreError,
};
pub use executor::{ExecutionOptions, PipelineExecutor, SecretResolver};
pub use friendly_sql::{PreprocessError, preprocess_sql};
pub use incremental_state::{IncrementalSchemaRecord, IncrementalState};
pub use output_cache::OutputCache;
pub use preview::{PreviewNodeResult, PreviewOptions, PreviewResult, PreviewStatus};
pub use provider::{
    OnConflict, PipelineSink, ProviderError, ProviderRegistry, SourceConnector, WriteOptions,
    WriteStats,
};
pub use python_runtime::PythonConfig;
pub use resolver::{EnvironmentCatalog, EnvironmentResolver, EnvironmentSchema};
pub use result::PipelineResult;
pub use run::{
    AssertionResultSummary, ExecutionEvent, NodeRunStats, PipelineRun, RunId, RunStatus,
    TestResultSummary,
};
pub use run_store::SqliteRunStore;
pub use session::{SessionFactory, SessionFactoryConfig};
pub use stats::NodeStats;
pub use storage::{
    BackfillStorage, ColumnLineageStorage, EnvironmentStorage, IncrementalStateStorage,
    LineageObservation, LineageStorage, RunStorage, StoredColumnEdge, StoredResourceBinding,
};
pub use test_assertion::{AssertionResult, TestNodeResult};
pub use udfs::{UdfDefinition, UdfError, UdfRegistry};

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
