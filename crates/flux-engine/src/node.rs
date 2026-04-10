// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::materialization::MaterializationPolicy;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;

/// Severity level for a test node. Controls whether a failing assertion
/// stops the pipeline or merely emits a warning.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestSeverity {
    /// A failing assertion fails the pipeline (default).
    #[default]
    Error,
    /// A failing assertion logs a warning but the pipeline continues.
    Warn,
}

/// A single data assertion applied to the upstream input of a test node.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Assertion {
    /// One or more columns must contain no nulls.
    NotNull { columns: Vec<String> },
    /// Column or composite key must be unique across all rows.
    Unique { columns: Vec<String> },
    /// Column values must be in a provided set.
    AcceptedValues {
        column: String,
        values: Vec<serde_json::Value>,
    },
    /// Total row count must fall in `[min, max]`.
    RowCountBetween { min: u64, max: u64 },
    /// Total row count must equal a fixed number.
    RowCountEqualTo { count: u64 },
    /// Entire rows must be unique (all columns considered).
    NoDuplicates,
    /// String column must match a regex pattern.
    ColumnValuesMatchRegex { column: String, pattern: String },
    /// A SQL expression evaluated per row must be true for every row.
    ExpressionTrue { expression: String },
    /// Escape hatch: user-provided query returning a `failing` count.
    /// Zero means pass.
    Sql { name: String, query: String },
}

/// Configuration for a test node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestConfig {
    /// Whether a failing assertion fails the pipeline or just warns.
    #[serde(default)]
    pub severity: TestSeverity,
    /// List of assertions to evaluate against the upstream input.
    pub assertions: Vec<Assertion>,
    /// How many violating rows to include in failure reports.
    #[serde(default = "default_max_violations")]
    pub max_violations_reported: usize,
}

fn default_max_violations() -> usize {
    25
}

/// Unique identifier for a node within a pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<S: Into<String>> From<S> for NodeId {
    fn from(s: S) -> Self {
        Self(s.into())
    }
}

/// A node in the pipeline DAG.
///
/// Serialization is custom to support the "no `type` field" wire format for
/// snippet call sites while keeping the existing flattened tagged shape for
/// source/transform/sink nodes (zero churn for pre-snippet pipelines).
#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub name: String,
    pub kind: NodeKind,
    /// Canvas position for the frontend.
    pub position: Position,
    /// Whether the user has pinned this node's position on the canvas.
    pub pinned_position: bool,
    /// If this node was produced by snippet expansion, the call-site ID of the
    /// outermost snippet call it belongs to. Used by the frontend to render
    /// snippet expansions as collapsible group nodes. Always `None` on disk;
    /// stamped during `expand_snippets` and serialized in API responses.
    pub snippet_parent: Option<NodeId>,
    /// The snippet name (matches `SnippetCall.snippet`) for the outermost
    /// snippet call this node belongs to. Sibling of `snippet_parent`.
    pub snippet_name: Option<String>,
}

/// The type-specific configuration for a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeKind {
    Source(SourceConfig),
    Transform(TransformConfig),
    Sink(SinkConfig),
    /// A data assertion node. Has exactly one upstream input and produces no
    /// output data — it validates the upstream data against its assertions.
    Test(TestConfig),
    /// A reference to a reusable pipeline snippet. Present only between
    /// deserialization and snippet expansion; the executor must never see this.
    #[serde(skip)]
    Snippet(SnippetCall),
}

impl NodeKind {
    pub fn is_source(&self) -> bool {
        matches!(self, Self::Source(_))
    }

    pub fn is_transform(&self) -> bool {
        matches!(self, Self::Transform(_))
    }

    pub fn is_sink(&self) -> bool {
        matches!(self, Self::Sink(_))
    }

    pub fn is_test(&self) -> bool {
        matches!(self, Self::Test(_))
    }

    pub fn is_snippet(&self) -> bool {
        matches!(self, Self::Snippet(_))
    }
}

/// A call-site reference to a reusable snippet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnippetCall {
    /// The snippet's name (matches the snippet file's `snippet` field).
    pub snippet: String,
    /// Parameter values supplied at the call site.
    #[serde(default)]
    pub params: BTreeMap<String, serde_json::Value>,
}

// -- Custom (de)serialization for Node ---------------------------------------
//
// The wire format for source/transform/sink nodes uses `#[serde(flatten)]` on
// the tagged `NodeKind` enum — emitting `{ id, name, type, ... }`. Snippet
// call sites intentionally have NO `type` field (the `snippet` key is the
// discriminator). We implement both traits by hand:
//
// * Deserialize: parse to a `Value`, check for a top-level `snippet` key, and
//   route to either a bespoke snippet path or the existing flattened path via
//   a private mirror struct.
// * Serialize: for non-snippet variants, emit via the same mirror struct (so
//   the byte output is identical to the old derived impl). For snippets,
//   emit `{ id, name, snippet, params, position?, pinned_position? }`.

#[derive(Serialize, Deserialize)]
struct NodeWire {
    id: NodeId,
    name: String,
    #[serde(flatten)]
    kind: NodeKind,
    #[serde(default)]
    position: Position,
    #[serde(default)]
    pinned_position: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    snippet_parent: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    snippet_name: Option<String>,
}

impl Serialize for Node {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.kind {
            NodeKind::Snippet(call) => {
                use serde::ser::SerializeMap;
                let mut extra = 0;
                if self.position.x != 0.0 || self.position.y != 0.0 {
                    extra += 1;
                }
                if self.pinned_position {
                    extra += 1;
                }
                let mut map = serializer.serialize_map(Some(4 + extra))?;
                map.serialize_entry("id", &self.id)?;
                map.serialize_entry("name", &self.name)?;
                map.serialize_entry("snippet", &call.snippet)?;
                map.serialize_entry("params", &call.params)?;
                if self.position.x != 0.0 || self.position.y != 0.0 {
                    map.serialize_entry("position", &self.position)?;
                }
                if self.pinned_position {
                    map.serialize_entry("pinned_position", &true)?;
                }
                map.end()
            }
            _ => {
                let wire = NodeWire {
                    id: self.id.clone(),
                    name: self.name.clone(),
                    kind: self.kind.clone(),
                    position: self.position,
                    pinned_position: self.pinned_position,
                    snippet_parent: self.snippet_parent.clone(),
                    snippet_name: self.snippet_name.clone(),
                };
                wire.serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let value = serde_json::Value::deserialize(deserializer)?;
        let obj = value
            .as_object()
            .ok_or_else(|| D::Error::custom("node must be an object"))?;

        if obj.contains_key("snippet") && !obj.contains_key("type") {
            // Snippet call-site path.
            let id: NodeId = obj
                .get("id")
                .cloned()
                .ok_or_else(|| D::Error::custom("node missing `id`"))
                .and_then(|v| serde_json::from_value(v).map_err(D::Error::custom))?;
            let name: String = obj
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| id.0.clone());
            let snippet: String = obj
                .get("snippet")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| D::Error::custom("snippet node `snippet` must be a string"))?;
            let params: BTreeMap<String, serde_json::Value> = match obj.get("params") {
                Some(v) => serde_json::from_value(v.clone()).map_err(D::Error::custom)?,
                None => BTreeMap::new(),
            };
            let position: Position = match obj.get("position") {
                Some(v) => serde_json::from_value(v.clone()).map_err(D::Error::custom)?,
                None => Position::default(),
            };
            let pinned_position = obj
                .get("pinned_position")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            Ok(Node {
                id,
                name,
                kind: NodeKind::Snippet(SnippetCall { snippet, params }),
                position,
                pinned_position,
                snippet_parent: None,
                snippet_name: None,
            })
        } else {
            let wire: NodeWire = serde_json::from_value(value).map_err(D::Error::custom)?;
            Ok(Node {
                id: wire.id,
                name: wire.name,
                kind: wire.kind,
                position: wire.position,
                pinned_position: wire.pinned_position,
                snippet_parent: wire.snippet_parent,
                snippet_name: wire.snippet_name,
            })
        }
    }
}

/// Configuration for a source node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Connector type name (e.g. "csv", "parquet", "postgresql", "rest_api").
    pub connector: String,
    /// Connector-specific configuration (opaque JSON).
    #[serde(default)]
    pub config: serde_json::Value,
    /// Maximum number of rows to cache for preview. Overrides the pipeline-level
    /// `cache_row_limit`. When `None`, falls back to the pipeline default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_row_limit: Option<usize>,
}

/// The execution mode for a transform node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransformMode {
    Sql,
    Python,
}

/// Configuration for a transform node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    pub mode: TransformMode,
    /// Inline SQL query or Python code. Ignored when `code_path` is set.
    #[serde(default)]
    pub code: String,
    /// Path to an external file containing the SQL or Python code.
    /// Resolved relative to the pipeline's `code_dir` (or the working directory
    /// if `code_dir` is not set). Supports nested paths like
    /// `"silver/usgs/earthquake_transform.py"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_path: Option<String>,
    /// Whether this node's output should be materialized (cached).
    #[serde(default)]
    pub materialized: bool,
    /// Maximum number of rows to cache for preview. Overrides the pipeline-level
    /// `cache_row_limit`. When `None`, falls back to the pipeline default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_row_limit: Option<usize>,
    /// User-provided column lineage annotations (planning doc 35c).
    ///
    /// When present, these annotations take precedence over automatically
    /// derived lineage (LazyFrame plan walk or opaque fallback). This is the
    /// escape hatch for eager Python code where lineage cannot be derived
    /// automatically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_annotations: Option<crate::column_lineage::LineageAnnotations>,
}

/// Configuration for a sink node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    /// Connector type name (e.g. "csv", "parquet", "postgresql", "stdout").
    pub connector: String,
    /// Connector-specific configuration (opaque JSON).
    #[serde(default)]
    pub config: serde_json::Value,
    /// Optional materialization policy. When omitted, the sink behaves as
    /// `mode: "full"` (current behavior — every run overwrites/appends blindly).
    /// See `planning/14-pipeline-format.md` and `planning/27-incremental-materializations.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization: Option<MaterializationPolicy>,
}

/// 2D position on the canvas.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_display() {
        let id = NodeId::new("my_node");
        assert_eq!(id.to_string(), "my_node");
    }

    #[test]
    fn node_id_from_string() {
        let id: NodeId = "test".into();
        assert_eq!(id.0, "test");
    }

    #[test]
    fn node_id_serde_transparent() {
        let id = NodeId::new("abc");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, r#""abc""#);
        let id2: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, id2);
    }

    #[test]
    fn node_kind_source_serde() {
        let kind = NodeKind::Source(SourceConfig {
            connector: "csv".into(),
            config: serde_json::json!({"path": "/data.csv"}),
            cache_row_limit: Some(500),
        });
        let json = serde_json::to_value(&kind).unwrap();
        assert_eq!(json["type"], "source");
        assert_eq!(json["connector"], "csv");
        assert_eq!(json["cache_row_limit"], 500);

        let kind2: NodeKind = serde_json::from_value(json).unwrap();
        assert!(kind2.is_source());
        assert!(!kind2.is_transform());
        assert!(!kind2.is_sink());
    }

    #[test]
    fn node_kind_transform_serde() {
        let kind = NodeKind::Transform(TransformConfig {
            mode: TransformMode::Python,
            code: "df.filter(col > 1)".into(),
            code_path: Some("transforms/t.py".into()),
            materialized: true,
            cache_row_limit: None, lineage_annotations: None,
        });
        let json = serde_json::to_value(&kind).unwrap();
        assert_eq!(json["type"], "transform");
        assert_eq!(json["mode"], "python");
        assert_eq!(json["materialized"], true);
        assert_eq!(json["code_path"], "transforms/t.py");

        let kind2: NodeKind = serde_json::from_value(json).unwrap();
        assert!(kind2.is_transform());
    }

    #[test]
    fn node_kind_sink_serde() {
        let kind = NodeKind::Sink(SinkConfig {
            connector: "postgresql".into(),
            config: serde_json::json!({"table": "output"}),
            materialization: None,
        });
        let json = serde_json::to_value(&kind).unwrap();
        assert_eq!(json["type"], "sink");
        assert_eq!(json["connector"], "postgresql");

        let kind2: NodeKind = serde_json::from_value(json).unwrap();
        assert!(kind2.is_sink());
    }

    #[test]
    fn transform_mode_serde() {
        assert_eq!(
            serde_json::to_string(&TransformMode::Sql).unwrap(),
            r#""sql""#
        );
        assert_eq!(
            serde_json::to_string(&TransformMode::Python).unwrap(),
            r#""python""#
        );
        let mode: TransformMode = serde_json::from_str(r#""sql""#).unwrap();
        assert_eq!(mode, TransformMode::Sql);
    }

    #[test]
    fn position_defaults_to_zero() {
        let pos = Position::default();
        assert_eq!(pos.x, 0.0);
        assert_eq!(pos.y, 0.0);
    }

    #[test]
    fn full_node_serde_roundtrip() {
        let node = Node {
            id: NodeId::new("t1"),
            name: "Transform 1".into(),
            kind: NodeKind::Transform(TransformConfig {
                mode: TransformMode::Sql,
                code: "SELECT * FROM upstream".into(),
                code_path: None,
                materialized: false,
                cache_row_limit: None, lineage_annotations: None,
            }),
            position: Position { x: 100.0, y: 200.5 },
            pinned_position: true,
            snippet_parent: None,
            snippet_name: None,
        };
        let json = serde_json::to_string(&node).unwrap();
        let node2: Node = serde_json::from_str(&json).unwrap();
        assert_eq!(node2.id, node.id);
        assert_eq!(node2.name, "Transform 1");
        assert!(node2.pinned_position);
        assert_eq!(node2.position.x, 100.0);
        assert_eq!(node2.position.y, 200.5);
    }

    #[test]
    fn source_config_optional_fields() {
        // cache_row_limit is skipped when None.
        let cfg = SourceConfig {
            connector: "csv".into(),
            config: serde_json::Value::Null,
            cache_row_limit: None,
        };
        let json = serde_json::to_value(&cfg).unwrap();
        assert!(json.get("cache_row_limit").is_none());
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = TestConfig {
            severity: TestSeverity::Warn,
            assertions: vec![
                Assertion::NotNull {
                    columns: vec!["id".into(), "name".into()],
                },
                Assertion::Unique {
                    columns: vec!["id".into()],
                },
                Assertion::AcceptedValues {
                    column: "status".into(),
                    values: vec![
                        serde_json::Value::String("active".into()),
                        serde_json::Value::String("inactive".into()),
                    ],
                },
                Assertion::RowCountBetween { min: 1, max: 1000 },
                Assertion::RowCountEqualTo { count: 42 },
                Assertion::NoDuplicates,
                Assertion::ColumnValuesMatchRegex {
                    column: "email".into(),
                    pattern: r"^.+@.+\..+$".into(),
                },
                Assertion::ExpressionTrue {
                    expression: "amount > 0".into(),
                },
                Assertion::Sql {
                    name: "custom".into(),
                    query: "SELECT COUNT(*) AS failing FROM ${input} WHERE x < 0".into(),
                },
            ],
            max_violations_reported: 10,
        };
        let json = serde_json::to_value(&cfg).unwrap();
        let cfg2: TestConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg2.assertions.len(), 9);
        assert_eq!(cfg2.severity, TestSeverity::Warn);
        assert_eq!(cfg2.max_violations_reported, 10);
    }

    #[test]
    fn test_config_defaults() {
        let json = serde_json::json!({
            "assertions": [
                { "kind": "not_null", "columns": ["id"] }
            ]
        });
        let cfg: TestConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.severity, TestSeverity::Error); // default
        assert_eq!(cfg.max_violations_reported, 25); // default
    }

    #[test]
    fn node_kind_test_serde() {
        let kind = NodeKind::Test(TestConfig {
            severity: TestSeverity::Error,
            assertions: vec![Assertion::NotNull {
                columns: vec!["id".into()],
            }],
            max_violations_reported: 25,
        });
        let json = serde_json::to_value(&kind).unwrap();
        assert_eq!(json["type"], "test");
        assert!(json["assertions"].is_array());

        let kind2: NodeKind = serde_json::from_value(json).unwrap();
        assert!(kind2.is_test());
        assert!(!kind2.is_source());
    }

    #[test]
    fn full_test_node_serde_roundtrip() {
        let node = Node {
            id: NodeId::new("validate_orders"),
            name: "Validate Orders".into(),
            kind: NodeKind::Test(TestConfig {
                severity: TestSeverity::Error,
                assertions: vec![
                    Assertion::NotNull {
                        columns: vec!["order_id".into()],
                    },
                    Assertion::Unique {
                        columns: vec!["order_id".into()],
                    },
                ],
                max_violations_reported: 25,
            }),
            position: Position { x: 300.0, y: 400.0 },
            pinned_position: false,
            snippet_parent: None,
            snippet_name: None,
        };
        let json = serde_json::to_string(&node).unwrap();
        let node2: Node = serde_json::from_str(&json).unwrap();
        assert_eq!(node2.id, node.id);
        assert_eq!(node2.name, "Validate Orders");
        assert!(node2.kind.is_test());
    }
}
