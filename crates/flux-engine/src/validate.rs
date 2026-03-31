// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Semantic validation for pipeline imports.
//!
//! Runs checks beyond what serde deserialization can enforce:
//! name/ID presence, variable default type compatibility,
//! environment override node references, and DAG structure.

use crate::dag;
use crate::error::{ImportError, ValidationError};
use crate::pipeline::{Pipeline, VariableType};
use std::collections::HashSet;

/// Validate a pipeline for import. Returns `Ok(())` if valid, or an
/// `ImportError::Validation` containing all detected problems.
pub fn validate_import(pipeline: &Pipeline) -> Result<(), ImportError> {
    let mut errors = Vec::new();

    // --- Name ---
    if pipeline.name.trim().is_empty() {
        errors.push(ValidationError::EmptyName);
    }

    // --- Node IDs and names ---
    for (i, node) in pipeline.nodes.iter().enumerate() {
        if node.id.0.trim().is_empty() {
            errors.push(ValidationError::EmptyNodeId { index: i });
        }
        if node.name.trim().is_empty() {
            errors.push(ValidationError::EmptyNodeName { index: i });
        }
    }

    // --- Variable default type checks ---
    for (name, var) in &pipeline.variables {
        if let Some(ref default) = var.default {
            if !default_matches_type(default, var.var_type) {
                errors.push(ValidationError::VariableDefaultTypeMismatch {
                    name: name.clone(),
                    expected: format!("{:?}", var.var_type).to_lowercase(),
                    value: default.to_string(),
                });
            }
        }
    }

    // --- Environment override node references ---
    let node_ids: HashSet<&str> = pipeline.nodes.iter().map(|n| n.id.0.as_str()).collect();
    for (env_name, overrides) in &pipeline.environment_overrides {
        for node_id in overrides.keys() {
            if !node_ids.contains(node_id.as_str()) {
                errors.push(ValidationError::OverrideUnknownNode {
                    environment: env_name.clone(),
                    node_id: node_id.clone(),
                });
            }
        }
    }

    // --- DAG structural validation ---
    if !pipeline.nodes.is_empty() {
        if let Err(dag_errors) = dag::validate(pipeline) {
            for e in dag_errors {
                errors.push(ValidationError::Dag(e));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ImportError::Validation(errors))
    }
}

/// Check whether a JSON default value is compatible with the declared variable type.
fn default_matches_type(value: &serde_json::Value, var_type: VariableType) -> bool {
    match var_type {
        VariableType::String => value.is_string(),
        VariableType::Integer => value.is_i64() || value.is_u64(),
        VariableType::Float => value.is_number(),
        VariableType::Boolean => value.is_boolean(),
        VariableType::Date => {
            // Accept a string that looks like a date (YYYY-MM-DD).
            value
                .as_str()
                .is_some_and(|s| s.len() == 10 && s.as_bytes().get(4) == Some(&b'-'))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::Edge;
    use crate::node::*;
    use crate::pipeline::Variable;
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
        }
    }

    fn sink_node(id: &str) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.to_string(),
            kind: NodeKind::Sink(SinkConfig {
                connector: "stdout".into(),
                config: serde_json::Value::Null,
            }),
            position: Position::default(),
            pinned_position: false,
        }
    }

    fn valid_pipeline() -> Pipeline {
        Pipeline {
            name: "test".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: BTreeMap::new(),
            environment_overrides: BTreeMap::new(),
            sample_config: None,
            cache_row_limit: None,
            code_dir: None,
            nodes: vec![source_node("src"), sink_node("sink")],
            edges: vec![Edge::new("src", "sink")],
        }
    }

    #[test]
    fn valid_pipeline_passes() {
        assert!(validate_import(&valid_pipeline()).is_ok());
    }

    #[test]
    fn empty_name_rejected() {
        let mut p = valid_pipeline();
        p.name = "  ".into();
        let err = validate_import(&p).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("name must not be empty"), "{msg}");
    }

    #[test]
    fn empty_node_id_rejected() {
        let mut p = valid_pipeline();
        p.nodes[0].id = NodeId::new("");
        let err = validate_import(&p).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("empty id"), "{msg}");
    }

    #[test]
    fn variable_type_mismatch_rejected() {
        let mut p = valid_pipeline();
        p.variables.insert(
            "count".into(),
            Variable {
                var_type: VariableType::Integer,
                default: Some(serde_json::json!("not a number")),
            },
        );
        let err = validate_import(&p).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not compatible with type"), "{msg}");
    }

    #[test]
    fn valid_variable_defaults_pass() {
        let mut p = valid_pipeline();
        p.variables.insert(
            "name".into(),
            Variable {
                var_type: VariableType::String,
                default: Some(serde_json::json!("hello")),
            },
        );
        p.variables.insert(
            "count".into(),
            Variable {
                var_type: VariableType::Integer,
                default: Some(serde_json::json!(42)),
            },
        );
        p.variables.insert(
            "ratio".into(),
            Variable {
                var_type: VariableType::Float,
                default: Some(serde_json::json!(3.14)),
            },
        );
        p.variables.insert(
            "enabled".into(),
            Variable {
                var_type: VariableType::Boolean,
                default: Some(serde_json::json!(true)),
            },
        );
        p.variables.insert(
            "start_date".into(),
            Variable {
                var_type: VariableType::Date,
                default: Some(serde_json::json!("2026-01-15")),
            },
        );
        assert!(validate_import(&p).is_ok());
    }

    #[test]
    fn override_unknown_node_rejected() {
        let mut p = valid_pipeline();
        let mut overrides = BTreeMap::new();
        overrides.insert("ghost_node".into(), serde_json::json!({"path": "/tmp"}));
        p.environment_overrides.insert("prod".into(), overrides);
        let err = validate_import(&p).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown node"), "{msg}");
    }

    #[test]
    fn override_valid_node_passes() {
        let mut p = valid_pipeline();
        let mut overrides = BTreeMap::new();
        overrides.insert("sink".into(), serde_json::json!({"path": "/tmp"}));
        p.environment_overrides.insert("prod".into(), overrides);
        assert!(validate_import(&p).is_ok());
    }

    #[test]
    fn dag_errors_surfaced() {
        let mut p = valid_pipeline();
        // Remove edges so sink has no upstream (DAG error).
        p.edges.clear();
        let err = validate_import(&p).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("must have at least one upstream"), "{msg}");
    }

    #[test]
    fn from_json_valid() {
        let p = valid_pipeline();
        let json = p.to_json().unwrap();
        let p2 = Pipeline::from_json(&json).unwrap();
        assert_eq!(p2.name, "test");
        assert_eq!(p2.nodes.len(), 2);
    }

    #[test]
    fn from_json_invalid_json() {
        let err = Pipeline::from_json("not json").unwrap_err();
        assert!(matches!(err, crate::error::ImportError::Json(_)));
    }

    #[test]
    fn from_json_validation_error() {
        // Valid JSON but empty name.
        let mut p = valid_pipeline();
        p.name = "".into();
        let json = serde_json::to_string_pretty(&p).unwrap();
        let err = Pipeline::from_json(&json).unwrap_err();
        assert!(matches!(err, crate::error::ImportError::Validation(_)));
    }

    #[test]
    fn deterministic_serialization() {
        let mut p = valid_pipeline();
        p.variables.insert(
            "z_var".into(),
            Variable {
                var_type: VariableType::String,
                default: None,
            },
        );
        p.variables.insert(
            "a_var".into(),
            Variable {
                var_type: VariableType::Integer,
                default: Some(serde_json::json!(1)),
            },
        );
        let json1 = p.to_json().unwrap();
        let json2 = p.to_json().unwrap();
        assert_eq!(json1, json2, "serialization must be deterministic");
        // BTreeMap guarantees a_var comes before z_var.
        let a_pos = json1.find("a_var").unwrap();
        let z_pos = json1.find("z_var").unwrap();
        assert!(a_pos < z_pos, "keys must be sorted");
    }
}
