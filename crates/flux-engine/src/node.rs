// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use serde::{Deserialize, Serialize};
use std::fmt;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub name: String,
    #[serde(flatten)]
    pub kind: NodeKind,
    /// Canvas position for the frontend.
    #[serde(default)]
    pub position: Position,
    /// Whether the user has pinned this node's position on the canvas.
    #[serde(default)]
    pub pinned_position: bool,
}

/// The type-specific configuration for a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeKind {
    Source(SourceConfig),
    Transform(TransformConfig),
    Sink(SinkConfig),
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
}

/// Configuration for a sink node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    /// Connector type name (e.g. "csv", "parquet", "postgresql", "stdout").
    pub connector: String,
    /// Connector-specific configuration (opaque JSON).
    #[serde(default)]
    pub config: serde_json::Value,
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
            cache_row_limit: None,
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
                cache_row_limit: None,
            }),
            position: Position { x: 100.0, y: 200.5 },
            pinned_position: true,
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
}
