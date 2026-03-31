// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::edge::Edge;
use crate::node::{Node, NodeId, TransformConfig};
use crate::sample::SampleConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// A complete pipeline definition: a DAG of source, transform, and sink nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub name: String,
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default = "default_environment")]
    pub default_environment: String,
    #[serde(default)]
    pub variables: BTreeMap<String, Variable>,
    /// Per-environment, per-node config overrides.
    /// Outer key = environment name, inner key = node id, value = override config.
    #[serde(default)]
    pub environment_overrides: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Default sample configuration for preview execution.
    /// When `None`, previews use `SampleConfig::default()` (first 100 rows).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_config: Option<SampleConfig>,
    /// Maximum number of rows to cache per node for preview. Individual nodes
    /// can override this with their own `cache_row_limit`. Default: 10 000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_row_limit: Option<usize>,
    /// Base directory for resolving `code_path` references on transform nodes.
    /// Paths in `code_path` are joined to this directory. When `None`, paths
    /// are resolved relative to the current working directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_dir: Option<String>,
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

fn default_version() -> u32 {
    1
}

fn default_environment() -> String {
    "dev".to_string()
}

impl Pipeline {
    /// Serialize to deterministic, pretty-printed JSON with sorted keys.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON with schema validation.
    ///
    /// Parses the JSON string into a `Pipeline` and runs import validation
    /// (name, node IDs, variable defaults, environment override references,
    /// and DAG structure). Returns descriptive errors on failure.
    pub fn from_json(json: &str) -> Result<Self, crate::error::ImportError> {
        let pipeline: Pipeline =
            serde_json::from_str(json).map_err(crate::error::ImportError::Json)?;
        crate::validate::validate_import(&pipeline)?;
        Ok(pipeline)
    }

    /// Deserialize from JSON with schema validation and variable reference warnings.
    ///
    /// Like [`from_json`](Self::from_json), but also returns non-fatal warnings
    /// for undefined `{{ variable }}` references in node code and configs.
    pub fn from_json_with_warnings(
        json: &str,
    ) -> Result<(Self, crate::error::ImportWarnings), crate::error::ImportError> {
        let pipeline: Pipeline =
            serde_json::from_str(json).map_err(crate::error::ImportError::Json)?;
        crate::validate::validate_import(&pipeline)?;
        let warnings = crate::error::ImportWarnings {
            undefined_variables: crate::variables::validate_variable_references(&pipeline),
        };
        Ok((pipeline, warnings))
    }

    /// Look up a node by its ID.
    pub fn node(&self, id: &NodeId) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id == *id)
    }

    /// Return an iterator of node IDs.
    pub fn node_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.nodes.iter().map(|n| &n.id)
    }

    /// Resolve the code for a transform node.
    ///
    /// If `code_path` is set, reads the file relative to `code_dir` (or the
    /// current working directory). Otherwise returns the inline `code` string.
    pub fn resolve_code(&self, xform: &TransformConfig) -> Result<String, std::io::Error> {
        match &xform.code_path {
            Some(rel_path) => {
                let base = self.code_dir.as_deref().unwrap_or(".");
                let full_path = Path::new(base).join(rel_path);
                std::fs::read_to_string(&full_path)
            }
            None => Ok(xform.code.clone()),
        }
    }

    /// Return a copy with `code_path` references resolved to inline `code`
    /// while keeping the `code_path` field intact. Used when serving the
    /// pipeline to the frontend so the editor has the code content and still
    /// knows which file to write back to.
    pub fn with_code_populated(&self) -> Result<Self, std::io::Error> {
        use crate::node::NodeKind;

        let mut resolved = self.clone();
        for node in &mut resolved.nodes {
            if let NodeKind::Transform(ref mut xform) = node.kind {
                if xform.code_path.is_some() && xform.code.is_empty() {
                    xform.code = self.resolve_code(xform)?;
                }
            }
        }
        Ok(resolved)
    }

    /// Return a copy of this pipeline with all `code_path` references resolved
    /// to inline `code` and `code_path`/`code_dir` cleared. Used for export so
    /// the resulting JSON is self-contained and importable without external files.
    pub fn with_resolved_code(&self) -> Result<Self, std::io::Error> {
        use crate::node::NodeKind;

        let mut resolved = self.clone();
        for node in &mut resolved.nodes {
            if let NodeKind::Transform(ref mut xform) = node.kind {
                if xform.code_path.is_some() {
                    xform.code = self.resolve_code(xform)?;
                    xform.code_path = None;
                }
            }
        }
        resolved.code_dir = None;
        Ok(resolved)
    }

    /// Return the upstream node IDs for a given node.
    pub fn upstream_of(&self, id: &NodeId) -> Vec<&NodeId> {
        self.edges
            .iter()
            .filter(|e| e.to == *id)
            .map(|e| &e.from)
            .collect()
    }

    /// Default cache row limit when neither node nor pipeline specifies one.
    pub const DEFAULT_CACHE_ROW_LIMIT: usize = 10_000;

    /// Resolve the effective cache row limit for a node.
    ///
    /// Precedence: node-level > pipeline-level > global default (10 000).
    pub fn effective_cache_row_limit(&self, node: &Node) -> usize {
        let node_limit = match &node.kind {
            crate::node::NodeKind::Source(cfg) => cfg.cache_row_limit,
            crate::node::NodeKind::Transform(cfg) => cfg.cache_row_limit,
            crate::node::NodeKind::Sink(_) => None,
        };
        node_limit
            .or(self.cache_row_limit)
            .unwrap_or(Self::DEFAULT_CACHE_ROW_LIMIT)
    }

    /// Return the immediate downstream node IDs for a given node.
    pub fn downstream_of(&self, id: &NodeId) -> Vec<&NodeId> {
        self.edges
            .iter()
            .filter(|e| e.from == *id)
            .map(|e| &e.to)
            .collect()
    }

    /// Return all transitive downstream node IDs (the full forward closure).
    pub fn all_downstream_of(&self, id: &NodeId) -> Vec<NodeId> {
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(id.clone());
        while let Some(current) = queue.pop_front() {
            for downstream in self.downstream_of(&current) {
                if visited.insert(downstream.clone()) {
                    queue.push_back(downstream.clone());
                }
            }
        }
        visited.into_iter().collect()
    }
}

/// A user-defined pipeline variable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Variable {
    #[serde(rename = "type")]
    pub var_type: VariableType,
    #[serde(default)]
    pub default: Option<serde_json::Value>,
}

/// Supported variable types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VariableType {
    String,
    Integer,
    Float,
    Date,
    Boolean,
}
