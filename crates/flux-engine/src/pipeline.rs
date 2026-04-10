// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::edge::Edge;
use crate::node::{Node, NodeId, TransformConfig};
use crate::sample::SampleConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
    /// Directory containing reusable SQL UDF definitions (`*.sql` files with
    /// `CREATE FUNCTION` statements). Each UDF is parsed at pipeline load time
    /// and inlined into SQL transforms by name. Resolved relative to the
    /// pipeline file's directory when relative.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udfs_dir: Option<String>,
    /// Directory containing reusable snippet `.json` files. Mirrors `udfs_dir`.
    /// Resolved relative to the pipeline file's directory when relative (via
    /// [`Pipeline::from_json_at_path`]) or relative to the current working
    /// directory (via [`Pipeline::from_json`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippets_dir: Option<String>,
    /// When `Some`, this file is a snippet rather than a runnable pipeline.
    /// The value is the snippet's external name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
    /// Parameters this snippet accepts. Empty for non-snippet pipelines.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, SnippetParamType>,
    /// Internal node IDs exposed to the calling pipeline. Empty for non-snippet
    /// pipelines.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<String>,
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

/// A snippet parameter type. Deserializes from/serializes to a bare string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnippetParamType {
    String,
    Number,
    Bool,
    Column,
    ColumnList,
}

impl SnippetParamType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Number => "number",
            Self::Bool => "bool",
            Self::Column => "column",
            Self::ColumnList => "column_list",
        }
    }
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
        let mut pipeline: Pipeline =
            serde_json::from_str(json).map_err(crate::error::ImportError::Json)?;
        crate::validate::migrate_legacy_sinks(&mut pipeline);
        // Snippet files themselves load-through without expansion or
        // validate_import (they are not runnable DAGs on their own).
        if pipeline.snippet.is_some() {
            return Ok(pipeline);
        }
        Self::expand_snippets_if_needed(&mut pipeline, None)?;
        crate::validate::validate_import(&pipeline)?;
        Ok(pipeline)
    }

    /// Like [`from_json`](Self::from_json), but resolves a relative
    /// `snippets_dir` against `base_dir` (the pipeline file's directory).
    pub fn from_json_at_path(
        json: &str,
        base_dir: &Path,
    ) -> Result<Self, crate::error::ImportError> {
        let mut pipeline: Pipeline =
            serde_json::from_str(json).map_err(crate::error::ImportError::Json)?;
        crate::validate::migrate_legacy_sinks(&mut pipeline);
        if pipeline.snippet.is_some() {
            return Ok(pipeline);
        }
        Self::expand_snippets_if_needed(&mut pipeline, Some(base_dir))?;
        crate::validate::validate_import(&pipeline)?;
        Ok(pipeline)
    }

    fn expand_snippets_if_needed(
        pipeline: &mut Pipeline,
        base_dir: Option<&Path>,
    ) -> Result<(), crate::error::ImportError> {
        let has_snippets = pipeline.nodes.iter().any(|n| n.kind.is_snippet());
        if !has_snippets {
            return Ok(());
        }
        let dir = match pipeline.snippets_dir.as_deref() {
            Some(d) => d,
            None => {
                return Err(crate::error::ImportError::Snippet(
                    crate::snippet::SnippetError::NotConfigured,
                ));
            }
        };
        let raw_path = Path::new(dir);
        let resolved: PathBuf = if raw_path.is_absolute() {
            raw_path.to_path_buf()
        } else if let Some(base) = base_dir {
            base.join(raw_path)
        } else {
            raw_path.to_path_buf()
        };
        let registry = crate::snippet::SnippetRegistry::load_from_dir(&resolved)
            .map_err(crate::error::ImportError::Snippet)?;
        crate::snippet::expand_snippets(pipeline, &registry)
            .map_err(crate::error::ImportError::Snippet)?;
        Ok(())
    }

    /// Deserialize from JSON with schema validation and variable reference warnings.
    ///
    /// Like [`from_json`](Self::from_json), but also returns non-fatal warnings
    /// for undefined `{{ variable }}` references in node code and configs.
    pub fn from_json_with_warnings(
        json: &str,
    ) -> Result<(Self, crate::error::ImportWarnings), crate::error::ImportError> {
        let mut pipeline: Pipeline =
            serde_json::from_str(json).map_err(crate::error::ImportError::Json)?;
        crate::validate::migrate_legacy_sinks(&mut pipeline);
        if pipeline.snippet.is_none() {
            Self::expand_snippets_if_needed(&mut pipeline, None)?;
        }
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
            crate::node::NodeKind::Test(_) => None,
            crate::node::NodeKind::Snippet(_) => None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::Edge;
    use crate::node::*;
    use std::collections::BTreeMap;

    fn source_node(id: &str) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.to_string(),
            kind: NodeKind::Source(SourceConfig {
                connector: "csv".into(),
                config: serde_json::Value::Null,
                cache_row_limit: None,
            }),
            position: Position::default(),
            pinned_position: false,
            snippet_parent: None,
            snippet_name: None,
        }
    }

    fn transform_node(id: &str) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.to_string(),
            kind: NodeKind::Transform(TransformConfig {
                mode: TransformMode::Sql,
                code: "SELECT * FROM upstream".into(),
                code_path: None,
                materialized: false,
                cache_row_limit: None, lineage_annotations: None,
            }),
            position: Position::default(),
            pinned_position: false,
            snippet_parent: None,
            snippet_name: None,
        }
    }

    fn sink_node(id: &str) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.to_string(),
            kind: NodeKind::Sink(SinkConfig {
                connector: "stdout".into(),
                config: serde_json::Value::Null,
                materialization: None,
            }),
            position: Position::default(),
            pinned_position: false,
            snippet_parent: None,
            snippet_name: None,
        }
    }

    fn diamond_pipeline() -> Pipeline {
        // src -> a, src -> b, a -> join, b -> join, join -> sink
        Pipeline {
            name: "diamond".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: BTreeMap::new(),
            environment_overrides: BTreeMap::new(),
            sample_config: None,
            cache_row_limit: None,
            code_dir: None,
            udfs_dir: None,
            snippets_dir: None,
            snippet: None,
            params: BTreeMap::new(),
            outputs: Vec::new(),
            nodes: vec![
                source_node("src"),
                transform_node("a"),
                transform_node("b"),
                transform_node("join"),
                sink_node("sink"),
            ],
            edges: vec![
                Edge::new("src", "a"),
                Edge::new("src", "b"),
                Edge::new("a", "join"),
                Edge::new("b", "join"),
                Edge::new("join", "sink"),
            ],
        }
    }

    #[test]
    fn node_lookup_found() {
        let p = diamond_pipeline();
        let node = p.node(&NodeId::new("a")).unwrap();
        assert_eq!(node.name, "a");
    }

    #[test]
    fn node_lookup_not_found() {
        let p = diamond_pipeline();
        assert!(p.node(&NodeId::new("ghost")).is_none());
    }

    #[test]
    fn node_ids_returns_all() {
        let p = diamond_pipeline();
        let ids: Vec<&str> = p.node_ids().map(|id| id.0.as_str()).collect();
        assert_eq!(ids.len(), 5);
        assert!(ids.contains(&"src"));
        assert!(ids.contains(&"sink"));
    }

    #[test]
    fn upstream_of_transform() {
        let p = diamond_pipeline();
        let mut upstream: Vec<&str> = p
            .upstream_of(&NodeId::new("join"))
            .iter()
            .map(|id| id.0.as_str())
            .collect();
        upstream.sort();
        assert_eq!(upstream, vec!["a", "b"]);
    }

    #[test]
    fn upstream_of_source_is_empty() {
        let p = diamond_pipeline();
        assert!(p.upstream_of(&NodeId::new("src")).is_empty());
    }

    #[test]
    fn downstream_of_source() {
        let p = diamond_pipeline();
        let mut downstream: Vec<&str> = p
            .downstream_of(&NodeId::new("src"))
            .iter()
            .map(|id| id.0.as_str())
            .collect();
        downstream.sort();
        assert_eq!(downstream, vec!["a", "b"]);
    }

    #[test]
    fn downstream_of_sink_is_empty() {
        let p = diamond_pipeline();
        assert!(p.downstream_of(&NodeId::new("sink")).is_empty());
    }

    #[test]
    fn all_downstream_of_source() {
        let p = diamond_pipeline();
        let mut all: Vec<String> = p
            .all_downstream_of(&NodeId::new("src"))
            .into_iter()
            .map(|id| id.0)
            .collect();
        all.sort();
        assert_eq!(all, vec!["a", "b", "join", "sink"]);
    }

    #[test]
    fn all_downstream_of_leaf_is_empty() {
        let p = diamond_pipeline();
        assert!(p.all_downstream_of(&NodeId::new("sink")).is_empty());
    }

    #[test]
    fn effective_cache_row_limit_default() {
        let p = diamond_pipeline();
        let node = p.node(&NodeId::new("src")).unwrap();
        assert_eq!(
            p.effective_cache_row_limit(node),
            Pipeline::DEFAULT_CACHE_ROW_LIMIT
        );
    }

    #[test]
    fn effective_cache_row_limit_pipeline_level() {
        let mut p = diamond_pipeline();
        p.cache_row_limit = Some(5_000);
        let node = p.node(&NodeId::new("src")).unwrap();
        assert_eq!(p.effective_cache_row_limit(node), 5_000);
    }

    #[test]
    fn effective_cache_row_limit_node_overrides_pipeline() {
        let mut p = diamond_pipeline();
        p.cache_row_limit = Some(5_000);
        // Mutate the source node to have a node-level limit.
        p.nodes[0] = Node {
            id: NodeId::new("src"),
            name: "src".into(),
            kind: NodeKind::Source(SourceConfig {
                connector: "csv".into(),
                config: serde_json::Value::Null,
                cache_row_limit: Some(500),
            }),
            position: Position::default(),
            pinned_position: false,
            snippet_parent: None,
            snippet_name: None,
        };
        let node = p.node(&NodeId::new("src")).unwrap();
        assert_eq!(p.effective_cache_row_limit(node), 500);
    }

    #[test]
    fn effective_cache_row_limit_sink_ignores_node() {
        let p = diamond_pipeline();
        let node = p.node(&NodeId::new("sink")).unwrap();
        // Sink nodes have no cache_row_limit field, so falls to pipeline/default.
        assert_eq!(
            p.effective_cache_row_limit(node),
            Pipeline::DEFAULT_CACHE_ROW_LIMIT
        );
    }

    #[test]
    fn resolve_code_inline() {
        let xform = TransformConfig {
            mode: TransformMode::Sql,
            code: "SELECT 1".into(),
            code_path: None,
            materialized: false,
            cache_row_limit: None, lineage_annotations: None,
        };
        let p = diamond_pipeline();
        assert_eq!(p.resolve_code(&xform).unwrap(), "SELECT 1");
    }

    #[test]
    fn resolve_code_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let sql_path = dir.path().join("query.sql");
        std::fs::write(&sql_path, "SELECT * FROM t").unwrap();

        let xform = TransformConfig {
            mode: TransformMode::Sql,
            code: String::new(),
            code_path: Some("query.sql".into()),
            materialized: false,
            cache_row_limit: None, lineage_annotations: None,
        };

        let mut p = diamond_pipeline();
        p.code_dir = Some(dir.path().to_string_lossy().into_owned());

        assert_eq!(p.resolve_code(&xform).unwrap(), "SELECT * FROM t");
    }

    #[test]
    fn with_code_populated_fills_empty_code() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("t.sql"), "SELECT 42").unwrap();

        let mut p = diamond_pipeline();
        p.code_dir = Some(dir.path().to_string_lossy().into_owned());
        // Replace transform "a" with one that has code_path but empty code.
        p.nodes[1] = Node {
            id: NodeId::new("a"),
            name: "a".into(),
            kind: NodeKind::Transform(TransformConfig {
                mode: TransformMode::Sql,
                code: String::new(),
                code_path: Some("t.sql".into()),
                materialized: false,
                cache_row_limit: None, lineage_annotations: None,
            }),
            position: Position::default(),
            pinned_position: false,
            snippet_parent: None,
            snippet_name: None,
        };

        let populated = p.with_code_populated().unwrap();
        if let NodeKind::Transform(ref xform) = populated.nodes[1].kind {
            assert_eq!(xform.code, "SELECT 42");
            // code_path is preserved.
            assert_eq!(xform.code_path.as_deref(), Some("t.sql"));
        } else {
            panic!("expected transform node");
        }
    }

    #[test]
    fn with_resolved_code_clears_code_path() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("t.sql"), "SELECT 99").unwrap();

        let mut p = diamond_pipeline();
        p.code_dir = Some(dir.path().to_string_lossy().into_owned());
        p.nodes[1] = Node {
            id: NodeId::new("a"),
            name: "a".into(),
            kind: NodeKind::Transform(TransformConfig {
                mode: TransformMode::Sql,
                code: String::new(),
                code_path: Some("t.sql".into()),
                materialized: false,
                cache_row_limit: None, lineage_annotations: None,
            }),
            position: Position::default(),
            pinned_position: false,
            snippet_parent: None,
            snippet_name: None,
        };

        let resolved = p.with_resolved_code().unwrap();
        assert!(resolved.code_dir.is_none());
        if let NodeKind::Transform(ref xform) = resolved.nodes[1].kind {
            assert_eq!(xform.code, "SELECT 99");
            assert!(xform.code_path.is_none());
        } else {
            panic!("expected transform node");
        }
    }

    #[test]
    fn from_json_with_warnings_reports_undefined_vars() {
        let p = Pipeline {
            name: "test".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: BTreeMap::new(),
            environment_overrides: BTreeMap::new(),
            sample_config: None,
            cache_row_limit: None,
            code_dir: None,
            udfs_dir: None,
            snippets_dir: None,
            snippet: None,
            params: BTreeMap::new(),
            outputs: Vec::new(),
            nodes: vec![
                source_node("src"),
                Node {
                    id: NodeId::new("xform"),
                    name: "xform".into(),
                    kind: NodeKind::Transform(TransformConfig {
                        mode: TransformMode::Sql,
                        code: "SELECT * WHERE x = '{{ unknown_var }}'".into(),
                        code_path: None,
                        materialized: false,
                        cache_row_limit: None, lineage_annotations: None,
                    }),
                    position: Position::default(),
                    pinned_position: false,
                    snippet_parent: None,
                    snippet_name: None,
                },
                sink_node("sink"),
            ],
            edges: vec![Edge::new("src", "xform"), Edge::new("xform", "sink")],
        };
        let json = serde_json::to_string_pretty(&p).unwrap();
        let (_, warnings) = Pipeline::from_json_with_warnings(&json).unwrap();
        assert_eq!(warnings.undefined_variables.len(), 1);
        assert_eq!(warnings.undefined_variables[0].variable, "unknown_var");
    }

    #[test]
    fn default_version_and_environment() {
        // Verify defaults applied when fields are absent from JSON.
        let json = r#"{
            "name": "minimal",
            "nodes": [{"id": "src", "name": "src", "type": "source", "connector": "csv"}],
            "edges": []
        }"#;
        let p: Pipeline = serde_json::from_str(json).unwrap();
        assert_eq!(p.version, 1);
        assert_eq!(p.default_environment, "dev");
    }
}
