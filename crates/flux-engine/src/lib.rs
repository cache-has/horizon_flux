// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod dag;
pub mod edge;
pub mod error;
pub mod node;
pub mod pipeline;
pub mod pipeline_store;
pub mod sample;
pub mod validate;
pub mod variables;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

// Re-export primary types at crate root for ergonomic imports.
pub use edge::Edge;
pub use error::{DagError, EngineError, ImportError, ImportWarnings, ValidationError};
pub use node::{Node, NodeId, NodeKind};
pub use pipeline::Pipeline;
pub use pipeline_store::{
    PipelineId, PipelineRecord, PipelineStore, PipelineStoreError, PipelineVersion,
};
pub use sample::SampleConfig;
pub use variables::{BuiltinContext, ResolvedVariables, VariableWarning};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_set() {
        assert!(!version().is_empty());
    }
}
