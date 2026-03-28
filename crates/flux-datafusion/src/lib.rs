// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod environment;
pub mod error;
pub mod executor;
pub mod friendly_sql;
pub mod preview;
pub mod provider;
pub mod python_env;
pub mod python_runtime;
pub mod resolver;
pub mod result;
pub mod run;
pub mod run_store;
pub mod stats;

pub use environment::{Environment, EnvironmentStore, TableOverride};
pub use friendly_sql::{preprocess_sql, PreprocessError};
pub use executor::{ExecutionOptions, PipelineExecutor};
pub use preview::{PreviewNodeResult, PreviewOptions, PreviewResult};
pub use provider::{
    OnConflict, PipelineSink, ProviderError, ProviderRegistry, SourceConnector, WriteOptions,
    WriteStats,
};
pub use resolver::{EnvironmentCatalog, EnvironmentResolver, EnvironmentSchema};
pub use result::PipelineResult;
pub use run::{NodeRunStats, PipelineRun, RunId, RunStatus};
pub use run_store::RunStore;
pub use stats::NodeStats;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
