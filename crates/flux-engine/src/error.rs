// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::node::NodeId;

/// Errors that can occur during DAG construction and validation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DagError {
    #[error("cycle detected involving node `{0}`")]
    CycleDetected(NodeId),

    #[error("orphan node `{0}` has no edges connecting it to the pipeline")]
    OrphanNode(NodeId),

    #[error("edge references unknown node `{0}`")]
    UnknownNode(NodeId),

    #[error("source node `{0}` must not have upstream edges")]
    SourceHasUpstream(NodeId),

    #[error("sink node `{0}` must not have downstream edges")]
    SinkHasDownstream(NodeId),

    #[error("transform node `{0}` must have at least one upstream edge")]
    TransformMissingUpstream(NodeId),

    #[error("sink node `{0}` must have at least one upstream edge")]
    SinkMissingUpstream(NodeId),

    #[error("duplicate node id `{0}`")]
    DuplicateNodeId(NodeId),

    #[error("duplicate edge from `{from}` to `{to}`")]
    DuplicateEdge { from: NodeId, to: NodeId },

    #[error("pipeline has no nodes")]
    EmptyPipeline,
}

/// Top-level engine error type.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    Dag(#[from] DagError),

    #[error("pipeline `{0}` not found")]
    PipelineNotFound(String),

    #[error("{0}")]
    Other(String),
}

/// Error returned when importing a pipeline from JSON.
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("pipeline validation failed:\n{}", format_validation_errors(.0))]
    Validation(Vec<ValidationError>),
}

/// Result of a successful pipeline import, which may include non-fatal warnings.
#[derive(Debug, Clone, Default)]
pub struct ImportWarnings {
    pub undefined_variables: Vec<crate::variables::VariableWarning>,
}

impl ImportWarnings {
    pub fn is_empty(&self) -> bool {
        self.undefined_variables.is_empty()
    }
}

/// A single validation error found during pipeline import.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ValidationError {
    #[error("pipeline name must not be empty")]
    EmptyName,

    #[error("node at index {index} has an empty id")]
    EmptyNodeId { index: usize },

    #[error("node at index {index} has an empty name")]
    EmptyNodeName { index: usize },

    #[error("variable `{name}`: default value `{value}` is not compatible with type `{expected}`")]
    VariableDefaultTypeMismatch {
        name: String,
        expected: String,
        value: String,
    },

    #[error("environment override `{environment}` references unknown node `{node_id}`")]
    OverrideUnknownNode {
        environment: String,
        node_id: String,
    },

    #[error(transparent)]
    Dag(#[from] DagError),
}

fn format_validation_errors(errors: &[ValidationError]) -> String {
    errors
        .iter()
        .enumerate()
        .map(|(i, e)| format!("  {}. {e}", i + 1))
        .collect::<Vec<_>>()
        .join("\n")
}
