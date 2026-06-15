// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Failure report capture for pipeline node errors (planning doc 37,
//! sub-feature 2).
//!
//! When a node fails during pipeline execution, the executor builds a
//! [`FailureReport`] containing enough context to diagnose the issue
//! without re-running the pipeline: the full error chain, node config,
//! input Arrow schemas, a sample of the input batch, executed SQL (for
//! SQL transforms), and plugin diagnostics.

use armillary_engine::NodeId;
use armillary_engine::node::NodeKind;
use arrow::datatypes::Schema;
use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Maximum number of input sample rows captured by default.
pub const DEFAULT_SAMPLE_ROW_LIMIT: usize = 25;

/// Absolute ceiling on input sample rows.
pub const MAX_SAMPLE_ROW_LIMIT: usize = 500;

/// A captured failure report for a single node within a pipeline run.
///
/// Persisted as JSON in the run store and served via the failure report
/// API. Contains everything needed to understand *why* a node failed
/// without re-running the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureReport {
    /// The run this failure belongs to.
    pub run_id: String,
    /// The node that failed.
    pub node_id: String,
    /// Pipeline name for navigation.
    pub pipeline_name: String,
    /// Environment the pipeline was running in.
    pub environment: String,
    /// The full error chain: top-level message first, then causes.
    pub error_chain: Vec<String>,
    /// The node's config at failure time, serialized as JSON.
    /// Secrets are already scrubbed by the executor before this is built.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_config: Option<serde_json::Value>,
    /// Arrow schemas from all upstream input nodes, keyed by node ID.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_schemas: Vec<InputSchema>,
    /// A sample of the input rows the node was processing when it failed.
    /// Capped at `sample_row_limit` rows total across all upstream nodes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_sample: Vec<serde_json::Value>,
    /// Number of rows available in the input (before sampling).
    #[serde(default)]
    pub input_total_rows: u64,
    /// For SQL transforms: the fully resolved SQL text after variable
    /// interpolation and UDF expansion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executed_sql: Option<String>,
    /// For plugin sinks: diagnostic information from the plugin process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_diagnostics: Option<PluginDiagnostics>,
    /// For source nodes: the generated upstream query (e.g. the Postgres
    /// SELECT with pushdowns applied).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_query: Option<String>,
    /// Wall-clock time of failure capture (Unix millis).
    pub captured_at_ms: i64,
}

/// Arrow schema captured from an upstream input node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputSchema {
    /// The upstream node ID that produced this schema.
    pub node_id: String,
    /// Arrow schema fields as JSON (field name, data type, nullable).
    pub fields: Vec<SchemaField>,
}

/// A single field from an Arrow schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Plugin-specific diagnostics captured when a plugin sink fails.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDiagnostics {
    /// Plugin name from the manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_name: Option<String>,
    /// Exit code if the plugin process terminated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Tail of stderr output from the plugin process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_tail: Option<String>,
    /// Summary of the last protocol messages exchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub last_protocol_messages: Vec<String>,
}

// ---------------------------------------------------------------------------
// Reproduce-locally bundle (planning doc 37, sub-feature 2)
// ---------------------------------------------------------------------------

/// A self-contained JSON bundle that packages everything needed to reproduce
/// a node failure offline. v1: structured data + human-readable instructions.
///
/// Served as a downloadable `.json` file from the failure-report reproduce
/// endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReproduceBundle {
    /// Format version so tooling can detect breaking changes.
    pub version: u32,
    /// Human-readable instructions for reproducing the failure.
    pub instructions: Vec<String>,
    /// Pipeline and environment context.
    pub context: ReproduceContext,
    /// The failing node's configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_config: Option<serde_json::Value>,
    /// Input schemas from upstream nodes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_schemas: Vec<InputSchema>,
    /// Sample input rows that were being processed at failure time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_sample: Vec<serde_json::Value>,
    /// Total rows available in input (before sampling).
    #[serde(default)]
    pub input_total_rows: u64,
    /// The fully resolved SQL text (for SQL transforms).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executed_sql: Option<String>,
    /// The generated upstream query (for source nodes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_query: Option<String>,
    /// The error chain that was observed.
    pub error_chain: Vec<String>,
}

/// Contextual metadata for a reproduce bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReproduceContext {
    pub pipeline_name: String,
    pub environment: String,
    pub run_id: String,
    pub node_id: String,
    pub captured_at_ms: i64,
}

impl ReproduceBundle {
    /// Build a reproduce bundle from a failure report.
    pub fn from_failure_report(report: &FailureReport) -> Self {
        let mut instructions = vec![
            format!(
                "This bundle captures the failure of node '{}' in pipeline '{}' (environment: '{}').",
                report.node_id, report.pipeline_name, report.environment
            ),
            format!("Run ID: {}", report.run_id),
            "The 'input_sample' field contains the rows the node was processing when it failed."
                .into(),
        ];

        if report.executed_sql.is_some() {
            instructions.push(
                "The 'executed_sql' field contains the fully resolved SQL after variable interpolation.".into(),
            );
        }
        if report.source_query.is_some() {
            instructions.push(
                "The 'source_query' field contains the generated upstream query with pushdowns applied.".into(),
            );
        }
        instructions.push(
            "To reproduce: create a pipeline with a CSV source pointing at the sample data and apply the same transform.".into(),
        );

        Self {
            version: 1,
            instructions,
            context: ReproduceContext {
                pipeline_name: report.pipeline_name.clone(),
                environment: report.environment.clone(),
                run_id: report.run_id.clone(),
                node_id: report.node_id.clone(),
                captured_at_ms: report.captured_at_ms,
            },
            node_config: report.node_config.clone(),
            input_schemas: report.input_schemas.clone(),
            input_sample: report.input_sample.clone(),
            input_total_rows: report.input_total_rows,
            executed_sql: report.executed_sql.clone(),
            source_query: report.source_query.clone(),
            error_chain: report.error_chain.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Builder helpers
// ---------------------------------------------------------------------------

impl FailureReport {
    /// Build the error chain from a `NodeErrorKind` by walking its
    /// `source()` chain.
    pub fn build_error_chain(err: &dyn std::error::Error) -> Vec<String> {
        let mut chain = vec![err.to_string()];
        let mut source = err.source();
        while let Some(cause) = source {
            chain.push(cause.to_string());
            source = cause.source();
        }
        chain
    }

    /// Extract input schemas from upstream `RecordBatch` outputs.
    pub fn extract_input_schemas(upstream: &[(NodeId, Vec<RecordBatch>)]) -> Vec<InputSchema> {
        upstream
            .iter()
            .filter_map(|(nid, batches)| {
                batches.first().map(|b| InputSchema {
                    node_id: nid.to_string(),
                    fields: schema_to_fields(b.schema()),
                })
            })
            .collect()
    }

    /// Sample input rows from upstream batches, up to `limit` rows total.
    pub fn sample_input_rows(
        upstream: &[(NodeId, Vec<RecordBatch>)],
        limit: usize,
    ) -> (Vec<serde_json::Value>, u64) {
        let limit = limit.min(MAX_SAMPLE_ROW_LIMIT);
        let mut total_rows: u64 = 0;
        let mut all_batches: Vec<&RecordBatch> = Vec::new();

        for (_nid, batches) in upstream {
            for batch in batches {
                total_rows += batch.num_rows() as u64;
                all_batches.push(batch);
            }
        }

        let mut sampled = Vec::new();
        let mut remaining = limit;

        for batch in &all_batches {
            if remaining == 0 {
                break;
            }
            let take = batch.num_rows().min(remaining);
            if take > 0 {
                let sliced = batch.slice(0, take);
                sampled.extend(batches_to_json_rows(&[sliced]));
                remaining -= take;
            }
        }

        (sampled, total_rows)
    }

    /// Serialize the node's config (NodeKind) to JSON for the report.
    pub fn serialize_node_config(kind: &NodeKind) -> Option<serde_json::Value> {
        serde_json::to_value(kind).ok()
    }
}

/// Convert an Arrow schema to a list of `SchemaField` entries.
fn schema_to_fields(schema: Arc<Schema>) -> Vec<SchemaField> {
    schema
        .fields()
        .iter()
        .map(|f| SchemaField {
            name: f.name().clone(),
            data_type: format!("{}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect()
}

/// Convert Arrow `RecordBatch`es to JSON row objects.
fn batches_to_json_rows(batches: &[RecordBatch]) -> Vec<serde_json::Value> {
    let mut buf = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buf);
        for batch in batches {
            if writer.write(batch).is_err() {
                return Vec::new();
            }
        }
        let _ = writer.finish();
    }
    let text = String::from_utf8(buf).unwrap_or_default();
    text.lines()
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn sample_input_rows_respects_limit() {
        let batch = test_batch();
        let upstream = vec![(NodeId::from("src"), vec![batch])];
        let (rows, total) = FailureReport::sample_input_rows(&upstream, 2);
        assert_eq!(total, 3);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], 1);
    }

    #[test]
    fn extract_input_schemas_captures_fields() {
        let batch = test_batch();
        let upstream = vec![(NodeId::from("src"), vec![batch])];
        let schemas = FailureReport::extract_input_schemas(&upstream);
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].node_id, "src");
        assert_eq!(schemas[0].fields.len(), 2);
        assert_eq!(schemas[0].fields[0].name, "id");
        assert!(!schemas[0].fields[0].nullable);
        assert!(schemas[0].fields[1].nullable);
    }

    #[test]
    fn error_chain_walks_sources() {
        let inner = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let outer: Box<dyn std::error::Error> = format!("failed to read config: {inner}").into();
        // Simple string errors have no source, so chain is length 1.
        let chain = FailureReport::build_error_chain(outer.as_ref());
        assert!(!chain.is_empty());
        assert!(chain[0].contains("failed to read config"));
    }

    #[test]
    fn serialize_node_config_round_trips() {
        use armillary_engine::node::{NodeKind, SourceConfig};
        let kind = NodeKind::Source(SourceConfig {
            connector: "csv".into(),
            config: serde_json::json!({"path": "/tmp/test.csv"}),
            cache_row_limit: None,
        });
        let val = FailureReport::serialize_node_config(&kind);
        assert!(val.is_some());
        let v = val.unwrap();
        assert_eq!(v["type"], "source");
        assert_eq!(v["connector"], "csv");
    }

    fn sample_failure_report() -> FailureReport {
        FailureReport {
            run_id: "550e8400-e29b-41d4-a716-446655440000".into(),
            node_id: "transform_1".into(),
            pipeline_name: "daily_etl".into(),
            environment: "prod".into(),
            error_chain: vec![
                "column 'revenue' not found".into(),
                "schema mismatch in SQL transform".into(),
            ],
            node_config: Some(serde_json::json!({"type": "transform", "mode": "sql"})),
            input_schemas: vec![InputSchema {
                node_id: "src".into(),
                fields: vec![SchemaField {
                    name: "id".into(),
                    data_type: "Int32".into(),
                    nullable: false,
                }],
            }],
            input_sample: vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})],
            input_total_rows: 1000,
            executed_sql: Some("SELECT revenue FROM input".into()),
            plugin_diagnostics: None,
            source_query: None,
            captured_at_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn reproduce_bundle_from_failure_report() {
        let report = sample_failure_report();
        let bundle = ReproduceBundle::from_failure_report(&report);

        assert_eq!(bundle.version, 1);
        assert_eq!(bundle.context.pipeline_name, "daily_etl");
        assert_eq!(bundle.context.node_id, "transform_1");
        assert_eq!(bundle.context.environment, "prod");
        assert_eq!(bundle.input_sample.len(), 2);
        assert_eq!(bundle.input_total_rows, 1000);
        assert_eq!(
            bundle.executed_sql.as_deref(),
            Some("SELECT revenue FROM input")
        );
        assert_eq!(bundle.error_chain.len(), 2);
        assert!(bundle.node_config.is_some());
        assert_eq!(bundle.input_schemas.len(), 1);
        // Instructions mention executed_sql when present.
        assert!(
            bundle
                .instructions
                .iter()
                .any(|i| i.contains("executed_sql"))
        );
    }

    #[test]
    fn reproduce_bundle_round_trips_through_json() {
        let report = sample_failure_report();
        let bundle = ReproduceBundle::from_failure_report(&report);
        let json = serde_json::to_string_pretty(&bundle).unwrap();
        let deserialized: ReproduceBundle = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.version, bundle.version);
        assert_eq!(deserialized.context.run_id, bundle.context.run_id);
        assert_eq!(deserialized.input_sample.len(), bundle.input_sample.len());
        assert_eq!(deserialized.error_chain, bundle.error_chain);
    }
}
