// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

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
pub mod watermark;

pub use column_stats::{ColumnStats, compute_column_stats};
pub use environment::{Environment, SqliteEnvironmentStore, TableOverride};
pub use error::IncrementalStateError;
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
pub use run::{ExecutionEvent, NodeRunStats, PipelineRun, RunId, RunStatus};
pub use run_store::SqliteRunStore;
pub use session::{SessionFactory, SessionFactoryConfig};
pub use stats::NodeStats;
pub use storage::{EnvironmentStorage, IncrementalStateStorage, RunStorage};

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
