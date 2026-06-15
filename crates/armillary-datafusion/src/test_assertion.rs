// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Test node execution — compiles assertions to SQL, runs them against
//! upstream data, and collects results.

use crate::error::NodeErrorKind;
use armillary_engine::NodeId;
use armillary_engine::node::{Assertion, TestConfig, TestSeverity};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

/// The name of the table registered in the DataFusion context for the upstream
/// input data. Assertions compile their SQL against this name.
const INPUT_TABLE: &str = "__test_input";

/// Result of evaluating a single assertion.
#[derive(Debug, Clone)]
pub struct AssertionResult {
    /// Human-readable name of the assertion.
    pub name: String,
    /// Whether the assertion passed.
    pub passed: bool,
    /// Number of violating rows found (0 if passed).
    pub violation_count: u64,
    /// Sample of violating rows (up to `max_violations_reported`).
    pub violating_rows: Vec<RecordBatch>,
    /// Error message when the assertion fails.
    pub message: Option<String>,
}

/// Aggregate result of all assertions for a test node.
#[derive(Debug, Clone)]
pub struct TestNodeResult {
    /// Node ID of the test node.
    pub node_id: NodeId,
    /// Whether the overall test passed.
    pub passed: bool,
    /// Severity level of the test node.
    pub severity: TestSeverity,
    /// Individual assertion results.
    pub assertions: Vec<AssertionResult>,
}

/// Compile an assertion to a SQL query that returns violating rows.
///
/// The query runs against a table named `__test_input`.
fn compile_assertion_sql(assertion: &Assertion) -> (String, String) {
    match assertion {
        Assertion::NotNull { columns } => {
            let conditions: Vec<String> =
                columns.iter().map(|c| format!("\"{c}\" IS NULL")).collect();
            let name = format!("not_null({})", columns.join(", "));
            let sql = format!(
                "SELECT * FROM {INPUT_TABLE} WHERE {}",
                conditions.join(" OR ")
            );
            (name, sql)
        }
        Assertion::Unique { columns } => {
            let cols = columns
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let name = format!("unique({})", columns.join(", "));
            let sql = format!(
                "SELECT {cols}, COUNT(*) AS __dup_count \
                 FROM {INPUT_TABLE} \
                 GROUP BY {cols} \
                 HAVING COUNT(*) > 1"
            );
            (name, sql)
        }
        Assertion::AcceptedValues { column, values } => {
            let value_list: Vec<String> = values
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    other => other.to_string(),
                })
                .collect();
            let name = format!("accepted_values({column})");
            let sql = format!(
                "SELECT \"{column}\" FROM {INPUT_TABLE} \
                 WHERE \"{column}\" NOT IN ({})",
                value_list.join(", ")
            );
            (name, sql)
        }
        Assertion::RowCountBetween { min, max } => {
            let name = format!("row_count_between({min}, {max})");
            // Returns one row if count is out of range, zero rows if OK.
            let sql = format!(
                "SELECT cnt AS row_count, {min} AS expected_min, {max} AS expected_max \
                 FROM (SELECT COUNT(*) AS cnt FROM {INPUT_TABLE}) \
                 WHERE cnt < {min} OR cnt > {max}"
            );
            (name, sql)
        }
        Assertion::RowCountEqualTo { count } => {
            let name = format!("row_count_equal_to({count})");
            let sql = format!(
                "SELECT cnt AS row_count, {count} AS expected \
                 FROM (SELECT COUNT(*) AS cnt FROM {INPUT_TABLE}) \
                 WHERE cnt != {count}"
            );
            (name, sql)
        }
        Assertion::NoDuplicates => {
            let name = "no_duplicates".to_string();
            let sql = format!(
                "SELECT *, COUNT(*) AS __dup_count \
                 FROM {INPUT_TABLE} \
                 GROUP BY ALL \
                 HAVING COUNT(*) > 1"
            );
            (name, sql)
        }
        Assertion::ColumnValuesMatchRegex { column, pattern } => {
            let escaped_pattern = pattern.replace('\'', "''");
            let name = format!("column_values_match_regex({column})");
            let sql = format!(
                "SELECT \"{column}\" FROM {INPUT_TABLE} \
                 WHERE \"{column}\" IS NOT NULL \
                 AND \"{column}\" !~ '{escaped_pattern}'"
            );
            (name, sql)
        }
        Assertion::ExpressionTrue { expression } => {
            let name = format!("expression_true({expression})");
            let sql = format!("SELECT * FROM {INPUT_TABLE} WHERE NOT ({expression})");
            (name, sql)
        }
        Assertion::Sql { name, query } => {
            // The user query returns a `failing` count. We wrap it so
            // that a non-zero failing count produces a row.
            let resolved_query = query.replace("${input}", INPUT_TABLE);
            let wrapped = format!("SELECT failing FROM ({resolved_query}) WHERE failing > 0");
            (name.clone(), wrapped)
        }
    }
}

/// Execute all assertions in a test node against the upstream data.
///
/// Returns `Ok(batches)` where batches is empty (test nodes produce no output).
/// On assertion failure with `severity: error`, returns `Err(NodeErrorKind)`.
/// On assertion failure with `severity: warn`, logs warnings and returns `Ok`.
pub async fn execute_test(
    node_id: &NodeId,
    config: &TestConfig,
    upstream_data: HashMap<NodeId, &Vec<RecordBatch>>,
) -> Result<(Vec<RecordBatch>, TestNodeResult), NodeErrorKind> {
    // Register all upstream tables as a single merged input table.
    let ctx = SessionContext::new();

    // Merge all upstream batches into a single input.
    let all_batches: Vec<RecordBatch> = upstream_data
        .into_values()
        .flat_map(|batches| batches.iter().cloned())
        .collect();

    if all_batches.is_empty() {
        return Err(NodeErrorKind::TestExecution(
            "test node has no upstream data".into(),
        ));
    }

    let schema = all_batches[0].schema();
    let mem_table = MemTable::try_new(schema, vec![all_batches])?;
    ctx.register_table(INPUT_TABLE, Arc::new(mem_table))?;

    let mut assertion_results = Vec::new();
    let mut all_passed = true;

    for assertion in &config.assertions {
        let (name, sql) = compile_assertion_sql(assertion);

        let result = match ctx.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let violation_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                    let passed = violation_count == 0;

                    // Trim violating rows to the configured max.
                    let violating_rows = if violation_count > 0 {
                        limit_batches(&batches, config.max_violations_reported)
                    } else {
                        Vec::new()
                    };

                    let message = if passed {
                        None
                    } else {
                        Some(format!(
                            "assertion `{name}` failed: {violation_count} violation(s) found"
                        ))
                    };

                    AssertionResult {
                        name,
                        passed,
                        violation_count,
                        violating_rows,
                        message,
                    }
                }
                Err(e) => AssertionResult {
                    name,
                    passed: false,
                    violation_count: 0,
                    violating_rows: Vec::new(),
                    message: Some(format!("assertion query execution failed: {e}")),
                },
            },
            Err(e) => AssertionResult {
                name,
                passed: false,
                violation_count: 0,
                violating_rows: Vec::new(),
                message: Some(format!("assertion query compilation failed: {e}")),
            },
        };

        if !result.passed {
            all_passed = false;
        }
        assertion_results.push(result);
    }

    let test_result = TestNodeResult {
        node_id: node_id.clone(),
        passed: all_passed,
        severity: config.severity,
        assertions: assertion_results,
    };

    if !all_passed {
        match config.severity {
            TestSeverity::Error => {
                // Build a summary error message from all failing assertions.
                let failures: Vec<String> = test_result
                    .assertions
                    .iter()
                    .filter(|a| !a.passed)
                    .filter_map(|a| a.message.clone())
                    .collect();
                return Err(NodeErrorKind::TestAssertionFailed {
                    summary: failures.join("; "),
                    result: test_result,
                });
            }
            TestSeverity::Warn => {
                for a in &test_result.assertions {
                    if let Some(msg) = &a.message {
                        warn!(node = %node_id, "{msg}");
                    }
                }
            }
        }
    }

    // Test nodes produce no output data.
    Ok((Vec::new(), test_result))
}

/// Limit batches to at most `max_rows` total rows.
fn limit_batches(batches: &[RecordBatch], max_rows: usize) -> Vec<RecordBatch> {
    let mut remaining = max_rows;
    let mut result = Vec::new();
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let take = batch.num_rows().min(remaining);
        result.push(batch.slice(0, take));
        remaining -= take;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
        ]));
        vec![
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                    Arc::new(StringArray::from(vec![
                        Some("Alice"),
                        Some("Bob"),
                        None,
                        Some("Diana"),
                        Some("Eve"),
                    ])),
                    Arc::new(StringArray::from(vec![
                        Some("active"),
                        Some("active"),
                        Some("inactive"),
                        Some("banned"),
                        Some("active"),
                    ])),
                ],
            )
            .unwrap(),
        ]
    }

    fn upstream(batches: Vec<RecordBatch>) -> HashMap<NodeId, &'static Vec<RecordBatch>> {
        // We need to leak the batches for the test (they're small).
        let leaked: &'static Vec<RecordBatch> = Box::leak(Box::new(batches));
        let mut map = HashMap::new();
        map.insert(NodeId::new("upstream"), leaked);
        map
    }

    #[tokio::test]
    async fn not_null_detects_nulls() {
        let data = test_batches();
        let upstream = upstream(data);
        let config = TestConfig {
            severity: TestSeverity::Error,
            assertions: vec![Assertion::NotNull {
                columns: vec!["name".into()],
            }],
            max_violations_reported: 25,
        };
        let result = execute_test(&NodeId::new("t"), &config, upstream).await;
        let err = result.unwrap_err();
        match err {
            NodeErrorKind::TestAssertionFailed { result, .. } => {
                assert!(!result.passed);
                assert_eq!(result.assertions[0].violation_count, 1);
            }
            _ => panic!("expected TestAssertionFailed"),
        }
    }

    #[tokio::test]
    async fn not_null_passes_clean_data() {
        let data = test_batches();
        let upstream = upstream(data);
        let config = TestConfig {
            severity: TestSeverity::Error,
            assertions: vec![Assertion::NotNull {
                columns: vec!["id".into()],
            }],
            max_violations_reported: 25,
        };
        let (batches, result) = execute_test(&NodeId::new("t"), &config, upstream)
            .await
            .unwrap();
        assert!(result.passed);
        assert!(batches.is_empty()); // test nodes produce no output
    }

    #[tokio::test]
    async fn unique_detects_duplicates() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let data = vec![
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 2, 3]))])
                .unwrap(),
        ];
        let upstream = upstream(data);
        let config = TestConfig {
            severity: TestSeverity::Error,
            assertions: vec![Assertion::Unique {
                columns: vec!["id".into()],
            }],
            max_violations_reported: 25,
        };
        let result = execute_test(&NodeId::new("t"), &config, upstream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn accepted_values_detects_invalid() {
        let data = test_batches();
        let upstream = upstream(data);
        let config = TestConfig {
            severity: TestSeverity::Error,
            assertions: vec![Assertion::AcceptedValues {
                column: "status".into(),
                values: vec![
                    serde_json::Value::String("active".into()),
                    serde_json::Value::String("inactive".into()),
                ],
            }],
            max_violations_reported: 25,
        };
        let result = execute_test(&NodeId::new("t"), &config, upstream).await;
        assert!(result.is_err()); // "banned" is not in accepted values
    }

    #[tokio::test]
    async fn row_count_between_passes() {
        let data = test_batches();
        let upstream = upstream(data);
        let config = TestConfig {
            severity: TestSeverity::Error,
            assertions: vec![Assertion::RowCountBetween { min: 1, max: 100 }],
            max_violations_reported: 25,
        };
        let (_, result) = execute_test(&NodeId::new("t"), &config, upstream)
            .await
            .unwrap();
        assert!(result.passed);
    }

    #[tokio::test]
    async fn row_count_between_fails() {
        let data = test_batches();
        let upstream = upstream(data);
        let config = TestConfig {
            severity: TestSeverity::Error,
            assertions: vec![Assertion::RowCountBetween { min: 10, max: 100 }],
            max_violations_reported: 25,
        };
        let result = execute_test(&NodeId::new("t"), &config, upstream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn warn_severity_does_not_fail() {
        let data = test_batches();
        let upstream = upstream(data);
        let config = TestConfig {
            severity: TestSeverity::Warn,
            assertions: vec![Assertion::NotNull {
                columns: vec!["name".into()],
            }],
            max_violations_reported: 25,
        };
        let (_, result) = execute_test(&NodeId::new("t"), &config, upstream)
            .await
            .unwrap();
        assert!(!result.passed);
        assert_eq!(result.assertions[0].violation_count, 1);
    }

    #[tokio::test]
    async fn expression_true_catches_violations() {
        let data = test_batches();
        let upstream = upstream(data);
        let config = TestConfig {
            severity: TestSeverity::Error,
            assertions: vec![Assertion::ExpressionTrue {
                expression: "\"id\" > 2".into(),
            }],
            max_violations_reported: 25,
        };
        let result = execute_test(&NodeId::new("t"), &config, upstream).await;
        assert!(result.is_err()); // ids 1 and 2 violate the expression
    }

    #[tokio::test]
    async fn sql_escape_hatch() {
        let data = test_batches();
        let upstream = upstream(data);
        let config = TestConfig {
            severity: TestSeverity::Error,
            assertions: vec![Assertion::Sql {
                name: "custom_check".into(),
                query: format!("SELECT COUNT(*) AS failing FROM {INPUT_TABLE} WHERE \"id\" < 0"),
            }],
            max_violations_reported: 25,
        };
        let (_, result) = execute_test(&NodeId::new("t"), &config, upstream)
            .await
            .unwrap();
        assert!(result.passed); // no negative ids
    }

    #[tokio::test]
    async fn max_violations_limits_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, true),
        ]));
        // 50 rows, all with null val.
        let ids: Vec<i64> = (1..=50).collect();
        let vals: Vec<Option<&str>> = vec![None; 50];
        let data = vec![
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(vals)),
                ],
            )
            .unwrap(),
        ];
        let upstream = upstream(data);
        let config = TestConfig {
            severity: TestSeverity::Warn,
            assertions: vec![Assertion::NotNull {
                columns: vec!["val".into()],
            }],
            max_violations_reported: 5,
        };
        let (_, result) = execute_test(&NodeId::new("t"), &config, upstream)
            .await
            .unwrap();
        assert_eq!(result.assertions[0].violation_count, 50);
        let reported_rows: usize = result.assertions[0]
            .violating_rows
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(reported_rows, 5);
    }
}
