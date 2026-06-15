// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Variable mapping evaluation for trigger-injected pipeline variables.
//!
//! Each trigger kind can declare a `variable_mapping` that maps pipeline variable
//! names to expressions evaluated at fire time. Expressions use a `$`-prefixed
//! syntax:
//!
//! - **File arrival:** `$file_path`, `$file_name`, `$file_dir`, `$file_ext`,
//!   `$file_count`
//! - **Webhook:** `$body` (whole body), `$body.key`, `$body.key.nested` (dot-path)
//! - **Pipeline completion:** `$upstream_pipeline`, `$upstream_environment`,
//!   `$upstream_status`, `$upstream_run_id`

use serde_json::Value;
use std::collections::HashMap;

/// Context provided when a file-arrival trigger fires.
#[derive(Debug, Clone)]
pub struct FileArrivalContext {
    /// The new files detected (absolute paths).
    pub new_files: Vec<String>,
}

/// Context provided when a webhook trigger fires.
#[derive(Debug, Clone)]
pub struct WebhookContext {
    /// The parsed JSON request body.
    pub body: Value,
}

/// Context provided when a pipeline-completion trigger fires.
#[derive(Debug, Clone)]
pub struct CompletionContext {
    pub upstream_pipeline: String,
    pub upstream_environment: String,
    pub upstream_status: String,
    pub upstream_run_id: Option<String>,
}

/// Evaluate a file-arrival variable mapping against the detected files.
///
/// Supported expressions:
/// - `$file_path`  — full path of the first new file
/// - `$file_name`  — file name (basename) of the first new file
/// - `$file_dir`   — parent directory of the first new file
/// - `$file_ext`   — file extension (without dot) of the first new file
/// - `$file_count` — number of new files detected (as integer)
/// - `$file_paths` — JSON array of all new file paths
pub fn evaluate_file_arrival(
    mapping: &HashMap<String, String>,
    ctx: &FileArrivalContext,
) -> Result<HashMap<String, Value>, String> {
    let mut result = HashMap::new();
    let first = ctx.new_files.first().map(|s| s.as_str()).unwrap_or("");

    for (var_name, expr) in mapping {
        let value = match expr.as_str() {
            "$file_path" => Value::String(first.to_string()),
            "$file_name" => {
                let name = std::path::Path::new(first)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");
                Value::String(name.to_string())
            }
            "$file_dir" => {
                let dir = std::path::Path::new(first)
                    .parent()
                    .and_then(|p| p.to_str())
                    .unwrap_or("");
                Value::String(dir.to_string())
            }
            "$file_ext" => {
                let ext = std::path::Path::new(first)
                    .extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("");
                Value::String(ext.to_string())
            }
            "$file_count" => Value::Number(serde_json::Number::from(ctx.new_files.len())),
            "$file_paths" => {
                let arr: Vec<Value> = ctx
                    .new_files
                    .iter()
                    .map(|p| Value::String(p.clone()))
                    .collect();
                Value::Array(arr)
            }
            other => {
                return Err(format!(
                    "unknown file_arrival expression `{other}` for variable `{var_name}`; \
                     expected one of: $file_path, $file_name, $file_dir, $file_ext, \
                     $file_count, $file_paths"
                ));
            }
        };
        result.insert(var_name.clone(), value);
    }

    Ok(result)
}

/// Evaluate a webhook variable mapping against the request body.
///
/// Supported expressions:
/// - `$body`           — the entire request body as a JSON value
/// - `$body.key`       — top-level key from the body object
/// - `$body.key.child` — nested dot-path traversal into the body object
pub fn evaluate_webhook(
    mapping: &HashMap<String, String>,
    ctx: &WebhookContext,
) -> Result<HashMap<String, Value>, String> {
    let mut result = HashMap::new();

    for (var_name, expr) in mapping {
        let value = if expr == "$body" {
            ctx.body.clone()
        } else if let Some(path) = expr.strip_prefix("$body.") {
            resolve_dot_path(&ctx.body, path).unwrap_or(Value::Null)
        } else {
            return Err(format!(
                "unknown webhook expression `{expr}` for variable `{var_name}`; \
                 expected `$body` or `$body.<path>`"
            ));
        };
        result.insert(var_name.clone(), value);
    }

    Ok(result)
}

/// Evaluate a pipeline-completion variable mapping.
///
/// Supported expressions:
/// - `$upstream_pipeline`    — name of the upstream pipeline that completed
/// - `$upstream_environment` — environment the upstream ran in
/// - `$upstream_status`      — completion status ("success", "failed")
/// - `$upstream_run_id`      — run ID of the upstream execution (null if unknown)
pub fn evaluate_completion(
    mapping: &HashMap<String, String>,
    ctx: &CompletionContext,
) -> Result<HashMap<String, Value>, String> {
    let mut result = HashMap::new();

    for (var_name, expr) in mapping {
        let value = match expr.as_str() {
            "$upstream_pipeline" => Value::String(ctx.upstream_pipeline.clone()),
            "$upstream_environment" => Value::String(ctx.upstream_environment.clone()),
            "$upstream_status" => Value::String(ctx.upstream_status.clone()),
            "$upstream_run_id" => match &ctx.upstream_run_id {
                Some(id) => Value::String(id.clone()),
                None => Value::Null,
            },
            other => {
                return Err(format!(
                    "unknown pipeline_completion expression `{other}` for variable `{var_name}`; \
                     expected one of: $upstream_pipeline, $upstream_environment, \
                     $upstream_status, $upstream_run_id"
                ));
            }
        };
        result.insert(var_name.clone(), value);
    }

    Ok(result)
}

/// Merge evaluated variable-mapping values into the trigger's static
/// `variable_overrides`. Mapping values take precedence over static overrides
/// (they are more specific to the firing event).
pub fn merge_variables(
    base: Option<&HashMap<String, Value>>,
    mapped: &HashMap<String, Value>,
) -> HashMap<String, Value> {
    let mut merged = base.cloned().unwrap_or_default();
    for (k, v) in mapped {
        merged.insert(k.clone(), v.clone());
    }
    merged
}

/// Validate that all variable names in a mapping exist in the pipeline's declared
/// variables. Returns a list of error strings for any that don't match.
///
/// This is intended to be called at trigger creation/update time by the server
/// API, not at fire time.
pub fn validate_mapping_keys(
    mapping: &HashMap<String, String>,
    pipeline_variables: &std::collections::BTreeMap<String, armillary_engine::pipeline::Variable>,
) -> Vec<String> {
    let mut errors = Vec::new();
    for var_name in mapping.keys() {
        if !pipeline_variables.contains_key(var_name) {
            errors.push(format!(
                "variable_mapping target `{var_name}` is not declared in the pipeline's variables"
            ));
        }
    }
    errors
}

/// Traverse a JSON value using a dot-separated path.
fn resolve_dot_path(value: &Value, path: &str) -> Option<Value> {
    let mut current = value;
    for segment in path.split('.') {
        match current {
            Value::Object(map) => {
                current = map.get(segment)?;
            }
            _ => return None,
        }
    }
    Some(current.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- File arrival tests ---

    #[test]
    fn file_arrival_basic_expressions() {
        let mut mapping = HashMap::new();
        mapping.insert("input_file".into(), "$file_path".into());
        mapping.insert("name".into(), "$file_name".into());
        mapping.insert("dir".into(), "$file_dir".into());
        mapping.insert("ext".into(), "$file_ext".into());
        mapping.insert("count".into(), "$file_count".into());

        let ctx = FileArrivalContext {
            new_files: vec!["/data/incoming/orders.parquet".into()],
        };

        let result = evaluate_file_arrival(&mapping, &ctx).unwrap();
        assert_eq!(
            result["input_file"],
            Value::String("/data/incoming/orders.parquet".into())
        );
        assert_eq!(result["name"], Value::String("orders.parquet".into()));
        assert_eq!(result["dir"], Value::String("/data/incoming".into()));
        assert_eq!(result["ext"], Value::String("parquet".into()));
        assert_eq!(result["count"], serde_json::json!(1));
    }

    #[test]
    fn file_arrival_multiple_files() {
        let mut mapping = HashMap::new();
        mapping.insert("files".into(), "$file_paths".into());
        mapping.insert("count".into(), "$file_count".into());

        let ctx = FileArrivalContext {
            new_files: vec!["/a.csv".into(), "/b.csv".into()],
        };

        let result = evaluate_file_arrival(&mapping, &ctx).unwrap();
        assert_eq!(result["count"], serde_json::json!(2));
        assert_eq!(result["files"], serde_json::json!(["/a.csv", "/b.csv"]));
    }

    #[test]
    fn file_arrival_unknown_expression() {
        let mut mapping = HashMap::new();
        mapping.insert("x".into(), "$unknown".into());
        let ctx = FileArrivalContext {
            new_files: vec!["/a.csv".into()],
        };
        assert!(evaluate_file_arrival(&mapping, &ctx).is_err());
    }

    // --- Webhook tests ---

    #[test]
    fn webhook_whole_body() {
        let mut mapping = HashMap::new();
        mapping.insert("payload".into(), "$body".into());

        let ctx = WebhookContext {
            body: serde_json::json!({"order_id": 42}),
        };

        let result = evaluate_webhook(&mapping, &ctx).unwrap();
        assert_eq!(result["payload"], serde_json::json!({"order_id": 42}));
    }

    #[test]
    fn webhook_dot_path() {
        let mut mapping = HashMap::new();
        mapping.insert("order".into(), "$body.order_id".into());
        mapping.insert("city".into(), "$body.address.city".into());

        let ctx = WebhookContext {
            body: serde_json::json!({
                "order_id": 123,
                "address": {"city": "Austin", "state": "TX"}
            }),
        };

        let result = evaluate_webhook(&mapping, &ctx).unwrap();
        assert_eq!(result["order"], serde_json::json!(123));
        assert_eq!(result["city"], Value::String("Austin".into()));
    }

    #[test]
    fn webhook_missing_path_returns_null() {
        let mut mapping = HashMap::new();
        mapping.insert("x".into(), "$body.nonexistent".into());

        let ctx = WebhookContext {
            body: serde_json::json!({}),
        };

        let result = evaluate_webhook(&mapping, &ctx).unwrap();
        assert_eq!(result["x"], Value::Null);
    }

    #[test]
    fn webhook_unknown_expression() {
        let mut mapping = HashMap::new();
        mapping.insert("x".into(), "$headers.foo".into());
        let ctx = WebhookContext { body: Value::Null };
        assert!(evaluate_webhook(&mapping, &ctx).is_err());
    }

    // --- Pipeline completion tests ---

    #[test]
    fn completion_all_expressions() {
        let mut mapping = HashMap::new();
        mapping.insert("upstream".into(), "$upstream_pipeline".into());
        mapping.insert("env".into(), "$upstream_environment".into());
        mapping.insert("status".into(), "$upstream_status".into());
        mapping.insert("run".into(), "$upstream_run_id".into());

        let ctx = CompletionContext {
            upstream_pipeline: "ingest_orders".into(),
            upstream_environment: "prod".into(),
            upstream_status: "success".into(),
            upstream_run_id: Some("run-abc".into()),
        };

        let result = evaluate_completion(&mapping, &ctx).unwrap();
        assert_eq!(result["upstream"], Value::String("ingest_orders".into()));
        assert_eq!(result["env"], Value::String("prod".into()));
        assert_eq!(result["status"], Value::String("success".into()));
        assert_eq!(result["run"], Value::String("run-abc".into()));
    }

    #[test]
    fn completion_null_run_id() {
        let mut mapping = HashMap::new();
        mapping.insert("run".into(), "$upstream_run_id".into());

        let ctx = CompletionContext {
            upstream_pipeline: "x".into(),
            upstream_environment: "dev".into(),
            upstream_status: "success".into(),
            upstream_run_id: None,
        };

        let result = evaluate_completion(&mapping, &ctx).unwrap();
        assert_eq!(result["run"], Value::Null);
    }

    // --- Merge tests ---

    #[test]
    fn merge_mapped_takes_precedence() {
        let mut base = HashMap::new();
        base.insert("a".into(), Value::String("base".into()));
        base.insert("b".into(), Value::String("keep".into()));

        let mut mapped = HashMap::new();
        mapped.insert("a".into(), Value::String("mapped".into()));
        mapped.insert("c".into(), Value::String("new".into()));

        let merged = merge_variables(Some(&base), &mapped);
        assert_eq!(merged["a"], Value::String("mapped".into()));
        assert_eq!(merged["b"], Value::String("keep".into()));
        assert_eq!(merged["c"], Value::String("new".into()));
    }

    #[test]
    fn merge_with_no_base() {
        let mut mapped = HashMap::new();
        mapped.insert("x".into(), serde_json::json!(42));

        let merged = merge_variables(None, &mapped);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged["x"], serde_json::json!(42));
    }

    // --- Validation tests ---

    #[test]
    fn validate_mapping_keys_reports_missing() {
        use armillary_engine::pipeline::{Variable, VariableType};
        use std::collections::BTreeMap;

        let mut vars = BTreeMap::new();
        vars.insert(
            "input_file".into(),
            Variable {
                var_type: VariableType::String,
                default: None,
            },
        );

        let mut mapping = HashMap::new();
        mapping.insert("input_file".into(), "$file_path".into());
        mapping.insert("missing_var".into(), "$file_name".into());

        let errors = validate_mapping_keys(&mapping, &vars);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("missing_var"));
    }

    #[test]
    fn validate_mapping_keys_all_present() {
        use armillary_engine::pipeline::{Variable, VariableType};
        use std::collections::BTreeMap;

        let mut vars = BTreeMap::new();
        vars.insert(
            "input_file".into(),
            Variable {
                var_type: VariableType::String,
                default: None,
            },
        );

        let mut mapping = HashMap::new();
        mapping.insert("input_file".into(), "$file_path".into());

        let errors = validate_mapping_keys(&mapping, &vars);
        assert!(errors.is_empty());
    }

    // --- dot-path resolution ---

    #[test]
    fn resolve_dot_path_nested() {
        let val = serde_json::json!({"a": {"b": {"c": 99}}});
        assert_eq!(resolve_dot_path(&val, "a.b.c"), Some(serde_json::json!(99)));
    }

    #[test]
    fn resolve_dot_path_missing() {
        let val = serde_json::json!({"a": 1});
        assert_eq!(resolve_dot_path(&val, "b"), None);
    }

    #[test]
    fn resolve_dot_path_non_object() {
        let val = serde_json::json!(42);
        assert_eq!(resolve_dot_path(&val, "a"), None);
    }
}
