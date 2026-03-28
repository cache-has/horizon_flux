// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Error types for pipeline execution.

use flux_engine::NodeId;

/// What went wrong when executing a single node.
#[derive(Debug, thiserror::Error)]
pub enum NodeErrorKind {
    #[error("source provider `{0}` not registered")]
    SourceProviderNotFound(String),

    #[error("sink writer `{0}` not registered")]
    SinkWriterNotFound(String),

    #[error("no output found for upstream node `{0}`")]
    MissingUpstreamOutput(NodeId),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("source provider error: {0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),

    #[error("sink writer error: {0}")]
    Sink(Box<dyn std::error::Error + Send + Sync>),

    #[error("Python transforms are not yet supported")]
    PythonNotSupported,
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
}
