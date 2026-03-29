// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline variable resolution and interpolation.
//!
//! Variables follow a precedence chain: runtime overrides > pipeline defaults.
//! Built-in variables (`run_date`, `run_id`, `pipeline_name`, `environment`)
//! are always available and can be overridden.
//!
//! The template syntax uses `{{ variable_name }}` (with optional whitespace
//! inside the braces). Variable references are resolved in SQL code, Python
//! params, connector configs, and file paths.

use crate::pipeline::{Pipeline, VariableType};
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::LazyLock;

/// Regex matching `{{ variable_name }}` with optional whitespace.
static VAR_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}").unwrap());

/// Built-in variable names that are always available.
pub const BUILTIN_VARS: &[&str] = &["run_date", "run_id", "pipeline_name", "environment"];

/// Context for built-in variable values, provided at execution time.
#[derive(Debug, Clone)]
pub struct BuiltinContext {
    pub run_date: String,
    pub run_id: String,
    pub pipeline_name: String,
    pub environment: String,
}

/// Resolved variable values ready for interpolation.
#[derive(Debug, Clone, Default)]
pub struct ResolvedVariables {
    values: HashMap<String, Value>,
}

impl ResolvedVariables {
    /// Resolve all variables for a pipeline execution.
    ///
    /// Precedence: `overrides` > pipeline variable defaults > built-in defaults.
    pub fn resolve(
        pipeline: &Pipeline,
        overrides: &HashMap<String, Value>,
        builtin: &BuiltinContext,
    ) -> Self {
        let mut values = HashMap::new();

        // 1. Built-in variables (lowest precedence, set first).
        values.insert("run_date".into(), Value::String(builtin.run_date.clone()));
        values.insert("run_id".into(), Value::String(builtin.run_id.clone()));
        values.insert(
            "pipeline_name".into(),
            Value::String(builtin.pipeline_name.clone()),
        );
        values.insert(
            "environment".into(),
            Value::String(builtin.environment.clone()),
        );

        // 2. Pipeline variable defaults (override built-ins if same name).
        for (name, var) in &pipeline.variables {
            if let Some(default) = &var.default {
                values.insert(name.clone(), default.clone());
            }
        }

        // 3. Runtime overrides (highest precedence).
        for (name, value) in overrides {
            values.insert(name.clone(), value.clone());
        }

        Self { values }
    }

    /// Get the resolved value for a variable.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.values.get(name)
    }

    /// Return all resolved values as a HashMap (for Python params).
    pub fn as_map(&self) -> &HashMap<String, Value> {
        &self.values
    }

    /// Interpolate `{{ variable }}` references in a string.
    ///
    /// Variables are replaced with their string representation:
    /// - Strings → the string value (without quotes)
    /// - Numbers/booleans → their JSON representation
    /// - Null → empty string
    pub fn interpolate(&self, input: &str) -> String {
        VAR_PATTERN
            .replace_all(input, |caps: &regex::Captures| {
                let var_name = &caps[1];
                match self.values.get(var_name) {
                    Some(Value::String(s)) => s.clone(),
                    Some(Value::Number(n)) => n.to_string(),
                    Some(Value::Bool(b)) => b.to_string(),
                    Some(Value::Null) | None => String::new(),
                    Some(other) => other.to_string(),
                }
            })
            .into_owned()
    }

    /// Interpolate variables in a JSON value recursively.
    ///
    /// Only string values are interpolated; objects and arrays are traversed.
    pub fn interpolate_json(&self, value: &Value) -> Value {
        match value {
            Value::String(s) => Value::String(self.interpolate(s)),
            Value::Object(map) => {
                let new_map: serde_json::Map<String, Value> = map
                    .iter()
                    .map(|(k, v)| (k.clone(), self.interpolate_json(v)))
                    .collect();
                Value::Object(new_map)
            }
            Value::Array(arr) => {
                Value::Array(arr.iter().map(|v| self.interpolate_json(v)).collect())
            }
            other => other.clone(),
        }
    }
}

/// Extract all `{{ variable }}` references from a string.
pub fn extract_references(input: &str) -> Vec<String> {
    VAR_PATTERN
        .captures_iter(input)
        .map(|c| c[1].to_string())
        .collect()
}

/// Extract variable references from a JSON value (recursively scans strings).
pub fn extract_references_json(value: &Value) -> Vec<String> {
    match value {
        Value::String(s) => extract_references(s),
        Value::Object(map) => map.values().flat_map(extract_references_json).collect(),
        Value::Array(arr) => arr.iter().flat_map(extract_references_json).collect(),
        _ => Vec::new(),
    }
}

/// Validate that all `{{ variable }}` references in a pipeline are defined.
///
/// Returns a list of warnings for undefined variable references. Each warning
/// includes the location (node ID + context) and the undefined variable name.
pub fn validate_variable_references(pipeline: &Pipeline) -> Vec<VariableWarning> {
    let mut defined: std::collections::HashSet<&str> = std::collections::HashSet::new();

    // Built-in variables are always defined.
    for name in BUILTIN_VARS {
        defined.insert(name);
    }

    // User-defined variables.
    for name in pipeline.variables.keys() {
        defined.insert(name.as_str());
    }

    let mut warnings = Vec::new();

    for node in &pipeline.nodes {
        let node_id = node.id.0.as_str();

        match &node.kind {
            crate::node::NodeKind::Source(src) => {
                check_json_refs(
                    &src.config,
                    node_id,
                    "source config",
                    &defined,
                    &mut warnings,
                );
            }
            crate::node::NodeKind::Transform(xform) => {
                check_string_refs(
                    &xform.code,
                    node_id,
                    "transform code",
                    &defined,
                    &mut warnings,
                );
            }
            crate::node::NodeKind::Sink(sink) => {
                check_json_refs(
                    &sink.config,
                    node_id,
                    "sink config",
                    &defined,
                    &mut warnings,
                );
            }
        }
    }

    warnings
}

/// A warning about an undefined variable reference.
#[derive(Debug, Clone)]
pub struct VariableWarning {
    /// The node where the reference was found.
    pub node_id: String,
    /// Context (e.g. "transform code", "source config").
    pub location: String,
    /// The undefined variable name.
    pub variable: String,
}

impl std::fmt::Display for VariableWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "node `{}` {} references undefined variable `{}`",
            self.node_id, self.location, self.variable
        )
    }
}

fn check_string_refs(
    input: &str,
    node_id: &str,
    location: &str,
    defined: &std::collections::HashSet<&str>,
    warnings: &mut Vec<VariableWarning>,
) {
    for var_name in extract_references(input) {
        if !defined.contains(var_name.as_str()) {
            warnings.push(VariableWarning {
                node_id: node_id.to_string(),
                location: location.to_string(),
                variable: var_name,
            });
        }
    }
}

fn check_json_refs(
    value: &Value,
    node_id: &str,
    location: &str,
    defined: &std::collections::HashSet<&str>,
    warnings: &mut Vec<VariableWarning>,
) {
    for var_name in extract_references_json(value) {
        if !defined.contains(var_name.as_str()) {
            warnings.push(VariableWarning {
                node_id: node_id.to_string(),
                location: location.to_string(),
                variable: var_name,
            });
        }
    }
}

/// Validate that runtime override values are compatible with their declared types.
///
/// Returns errors for any override whose value doesn't match the pipeline's
/// variable type declaration. Unknown overrides (not declared in the pipeline)
/// are silently allowed — they may be used by built-in variables or future extensions.
pub fn validate_overrides(
    pipeline: &Pipeline,
    overrides: &HashMap<String, Value>,
) -> Vec<String> {
    let mut errors = Vec::new();
    for (name, value) in overrides {
        if let Some(var) = pipeline.variables.get(name) {
            if !override_matches_type(value, var.var_type) {
                errors.push(format!(
                    "variable `{name}`: override value `{value}` is not compatible \
                     with declared type `{:?}`",
                    var.var_type
                ));
            }
        }
    }
    errors
}

fn override_matches_type(value: &Value, var_type: VariableType) -> bool {
    match var_type {
        VariableType::String => value.is_string(),
        VariableType::Integer => value.is_i64() || value.is_u64(),
        VariableType::Float => value.is_number(),
        VariableType::Boolean => value.is_boolean(),
        VariableType::Date => {
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
    use crate::pipeline::{Pipeline, Variable};
    use std::collections::BTreeMap;

    fn test_builtin() -> BuiltinContext {
        BuiltinContext {
            run_date: "2026-03-29".into(),
            run_id: "abc-123".into(),
            pipeline_name: "test_pipeline".into(),
            environment: "dev".into(),
        }
    }

    fn minimal_pipeline() -> Pipeline {
        Pipeline {
            name: "test".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: BTreeMap::new(),
            environment_overrides: BTreeMap::new(),
            sample_config: None,
            nodes: vec![
                Node {
                    id: NodeId::new("src"),
                    name: "src".into(),
                    kind: NodeKind::Source(SourceConfig {
                        connector: "csv".into(),
                        config: serde_json::json!({"path": "/data/{{ run_date }}.csv"}),
                    }),
                    position: Position::default(),
                    pinned_position: false,
                },
                Node {
                    id: NodeId::new("sink"),
                    name: "sink".into(),
                    kind: NodeKind::Sink(SinkConfig {
                        connector: "stdout".into(),
                        config: serde_json::Value::Null,
                    }),
                    position: Position::default(),
                    pinned_position: false,
                },
            ],
            edges: vec![Edge::new("src", "sink")],
        }
    }

    #[test]
    fn builtin_variables_resolved() {
        let pipeline = minimal_pipeline();
        let resolved = ResolvedVariables::resolve(&pipeline, &HashMap::new(), &test_builtin());
        assert_eq!(
            resolved.get("run_date"),
            Some(&Value::String("2026-03-29".into()))
        );
        assert_eq!(
            resolved.get("run_id"),
            Some(&Value::String("abc-123".into()))
        );
        assert_eq!(
            resolved.get("pipeline_name"),
            Some(&Value::String("test_pipeline".into()))
        );
        assert_eq!(
            resolved.get("environment"),
            Some(&Value::String("dev".into()))
        );
    }

    #[test]
    fn defaults_override_builtins() {
        let mut pipeline = minimal_pipeline();
        pipeline.variables.insert(
            "environment".into(),
            Variable {
                var_type: VariableType::String,
                default: Some(Value::String("staging".into())),
            },
        );
        let resolved = ResolvedVariables::resolve(&pipeline, &HashMap::new(), &test_builtin());
        assert_eq!(
            resolved.get("environment"),
            Some(&Value::String("staging".into()))
        );
    }

    #[test]
    fn overrides_take_highest_precedence() {
        let mut pipeline = minimal_pipeline();
        pipeline.variables.insert(
            "threshold".into(),
            Variable {
                var_type: VariableType::Integer,
                default: Some(serde_json::json!(10)),
            },
        );
        let mut overrides = HashMap::new();
        overrides.insert("threshold".into(), serde_json::json!(42));
        overrides.insert("environment".into(), Value::String("prod".into()));

        let resolved = ResolvedVariables::resolve(&pipeline, &overrides, &test_builtin());
        assert_eq!(resolved.get("threshold"), Some(&serde_json::json!(42)));
        assert_eq!(
            resolved.get("environment"),
            Some(&Value::String("prod".into()))
        );
    }

    #[test]
    fn interpolate_string() {
        let pipeline = minimal_pipeline();
        let resolved = ResolvedVariables::resolve(&pipeline, &HashMap::new(), &test_builtin());

        assert_eq!(
            resolved.interpolate("SELECT * WHERE date = '{{ run_date }}'"),
            "SELECT * WHERE date = '2026-03-29'"
        );
        assert_eq!(
            resolved.interpolate("/data/{{run_date}}/output.csv"),
            "/data/2026-03-29/output.csv"
        );
        assert_eq!(
            resolved.interpolate("run: {{ run_id }}, env: {{ environment }}"),
            "run: abc-123, env: dev"
        );
    }

    #[test]
    fn interpolate_undefined_becomes_empty() {
        let pipeline = minimal_pipeline();
        let resolved = ResolvedVariables::resolve(&pipeline, &HashMap::new(), &test_builtin());
        assert_eq!(resolved.interpolate("{{ undefined_var }}"), "");
    }

    #[test]
    fn interpolate_json_recursive() {
        let pipeline = minimal_pipeline();
        let resolved = ResolvedVariables::resolve(&pipeline, &HashMap::new(), &test_builtin());

        let config = serde_json::json!({
            "path": "/data/{{ run_date }}.csv",
            "nested": {
                "name": "{{ pipeline_name }}"
            },
            "list": ["{{ environment }}", "static"],
            "number": 42
        });

        let result = resolved.interpolate_json(&config);
        assert_eq!(result["path"], "/data/2026-03-29.csv");
        assert_eq!(result["nested"]["name"], "test_pipeline");
        assert_eq!(result["list"][0], "dev");
        assert_eq!(result["list"][1], "static");
        assert_eq!(result["number"], 42);
    }

    #[test]
    fn interpolate_number_and_bool() {
        let mut pipeline = minimal_pipeline();
        pipeline.variables.insert(
            "count".into(),
            Variable {
                var_type: VariableType::Integer,
                default: Some(serde_json::json!(100)),
            },
        );
        pipeline.variables.insert(
            "enabled".into(),
            Variable {
                var_type: VariableType::Boolean,
                default: Some(serde_json::json!(true)),
            },
        );
        let resolved = ResolvedVariables::resolve(&pipeline, &HashMap::new(), &test_builtin());
        assert_eq!(
            resolved.interpolate("LIMIT {{ count }}"),
            "LIMIT 100"
        );
        assert_eq!(
            resolved.interpolate("enabled={{ enabled }}"),
            "enabled=true"
        );
    }

    #[test]
    fn extract_refs_from_string() {
        let refs = extract_references("{{ foo }} and {{ bar_baz }} and {{run_date}}");
        assert_eq!(refs, vec!["foo", "bar_baz", "run_date"]);
    }

    #[test]
    fn validate_refs_warns_on_undefined() {
        let mut pipeline = minimal_pipeline();
        pipeline.nodes[0] = Node {
            id: NodeId::new("xform"),
            name: "xform".into(),
            kind: NodeKind::Transform(TransformConfig {
                mode: TransformMode::Sql,
                code: "SELECT * WHERE x = '{{ undefined_var }}' AND d = '{{ run_date }}'".into(),
                materialized: false,
            }),
            position: Position::default(),
            pinned_position: false,
        };
        // Fix edges for valid DAG.
        pipeline.edges = vec![Edge::new("sink", "xform")];
        pipeline.nodes.push(Node {
            id: NodeId::new("sink2"),
            name: "sink2".into(),
            kind: NodeKind::Sink(SinkConfig {
                connector: "stdout".into(),
                config: serde_json::Value::Null,
            }),
            position: Position::default(),
            pinned_position: false,
        });
        pipeline.edges.push(Edge::new("xform", "sink2"));

        let warnings = validate_variable_references(&pipeline);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].variable, "undefined_var");
        assert_eq!(warnings[0].node_id, "xform");
    }

    #[test]
    fn validate_refs_no_warnings_for_defined() {
        let mut pipeline = minimal_pipeline();
        pipeline.variables.insert(
            "my_var".into(),
            Variable {
                var_type: VariableType::String,
                default: None,
            },
        );
        pipeline.nodes[0] = Node {
            id: NodeId::new("src"),
            name: "src".into(),
            kind: NodeKind::Source(SourceConfig {
                connector: "csv".into(),
                config: serde_json::json!({"path": "/data/{{ my_var }}/{{ run_date }}.csv"}),
            }),
            position: Position::default(),
            pinned_position: false,
        };
        let warnings = validate_variable_references(&pipeline);
        assert!(warnings.is_empty(), "got warnings: {:?}", warnings);
    }

    #[test]
    fn validate_overrides_rejects_type_mismatch() {
        let mut pipeline = minimal_pipeline();
        pipeline.variables.insert(
            "count".into(),
            Variable {
                var_type: VariableType::Integer,
                default: Some(serde_json::json!(10)),
            },
        );
        let mut overrides = HashMap::new();
        overrides.insert("count".into(), Value::String("not a number".into()));
        let errors = validate_overrides(&pipeline, &overrides);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("not compatible"));
    }

    #[test]
    fn validate_overrides_allows_unknown_keys() {
        let pipeline = minimal_pipeline();
        let mut overrides = HashMap::new();
        overrides.insert("unknown_key".into(), Value::String("anything".into()));
        let errors = validate_overrides(&pipeline, &overrides);
        assert!(errors.is_empty());
    }
}
