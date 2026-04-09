// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Error types for pipeline execution.

use flux_engine::NodeId;

/// What went wrong when executing a single node.
#[derive(Debug, thiserror::Error)]
pub enum NodeErrorKind {
    #[error("source provider `{0}` not registered")]
    SourceProviderNotFound(String),

    #[error("sink connector `{0}` not registered")]
    SinkNotFound(String),

    #[error("no output found for upstream node `{0}`")]
    MissingUpstreamOutput(NodeId),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("source provider error: {0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),

    #[error("sink error: {0}")]
    Sink(Box<dyn std::error::Error + Send + Sync>),

    #[error("SQL preprocessing error: {0}")]
    Preprocess(#[from] crate::friendly_sql::PreprocessError),

    #[error("UDF error: {0}")]
    Udf(#[from] crate::udfs::UdfError),

    #[error("Python transform error: {0}")]
    Python(String),

    #[error("Python transform timed out after {0:.0?}")]
    PythonTimeout(std::time::Duration),

    #[error(
        "Python transform exceeded memory limit ({used_mb:.1} MiB used, {limit_mb:.1} MiB limit)"
    )]
    PythonMemoryExceeded { used_mb: f64, limit_mb: f64 },

    #[error("Python interpreter not found (`{0}`). Ensure Python 3 is installed and on your PATH.")]
    PythonNotFound(String),

    #[error("failed to read code file `{path}`: {source}")]
    CodeFileRead {
        path: String,
        source: std::io::Error,
    },

    #[error("test assertion failed: {summary}")]
    TestAssertionFailed {
        summary: String,
        result: crate::test_assertion::TestNodeResult,
    },

    #[error("test execution error: {0}")]
    TestExecution(String),
}

/// Top-level executor error.
#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("DAG validation failed: {0:?}")]
    Validation(Vec<flux_engine::DagError>),

    #[error("node `{node_id}` failed: {kind}")]
    Node {
        node_id: NodeId,
        kind: NodeErrorKind,
    },

    #[error("pipeline execution was cancelled")]
    Cancelled,

    #[error("run store error: {0}")]
    RunStore(#[from] RunStoreError),
}

/// Errors from the run store.
#[derive(Debug, thiserror::Error)]
pub enum RunStoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("run not found: {0}")]
    NotFound(String),

    #[error("invalid run status: {0}")]
    InvalidStatus(String),

    #[error("database error: {0}")]
    Database(String),
}

/// Errors from the incremental-state store (planning doc 27).
#[derive(Debug, thiserror::Error)]
pub enum IncrementalStateError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("database error: {0}")]
    Database(String),

    #[error(
        "incremental state not found for pipeline `{pipeline_id}`, node `{node_id}`, env `{environment}`"
    )]
    NotFound {
        pipeline_id: String,
        node_id: String,
        environment: String,
    },
}

/// Errors from the lineage store (planning doc 31).
#[derive(Debug, thiserror::Error)]
pub enum LineageStoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("database error: {0}")]
    Database(String),
}

/// Errors from the backfill store (planning doc 33).
#[derive(Debug, thiserror::Error)]
pub enum BackfillStoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("backfill not found: {0}")]
    NotFound(String),

    #[error("invalid status: {0}")]
    InvalidStatus(String),

    #[error("database error: {0}")]
    Database(String),
}

/// Errors from the environment system.
#[derive(Debug, thiserror::Error)]
pub enum EnvironmentError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("environment `{0}` not found")]
    NotFound(String),

    #[error("environment `{0}` already exists")]
    AlreadyExists(String),

    #[error("fallback environment `{0}` not found")]
    FallbackNotFound(String),

    #[error("cannot delete the `prod` environment")]
    CannotDeleteProd,

    #[error("`prod` cannot have a fallback environment")]
    ProdCannotHaveFallback,

    #[error("cyclic fallback chain detected")]
    CyclicFallback,

    #[error("database error: {0}")]
    Database(String),
}
