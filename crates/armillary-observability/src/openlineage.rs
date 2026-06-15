// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! OpenLineage event emission.
//!
//! Emits [OpenLineage](https://openlineage.io/) START, COMPLETE, FAIL, and
//! ABORT events so armillary pipelines are discoverable in data catalogs like
//! Marquez, DataHub, OpenMetadata, and Atlas.
//!
//! No official Rust client exists, so we emit JSON directly — the wire format
//! is a simple HTTP POST of a `RunEvent` JSON object.

use crate::config::OpenLineageConfig;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

// ---------------------------------------------------------------------------
// OpenLineage wire-format types (subset of the spec we need)
// ---------------------------------------------------------------------------

/// Top-level OpenLineage run event.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEvent {
    /// ISO 8601 timestamp.
    pub event_time: String,
    /// `START`, `COMPLETE`, `FAIL`, or `ABORT`.
    pub event_type: EventType,
    /// URI identifying the producer.
    pub producer: String,
    /// JSON pointer to the schema version.
    pub schema_url: String,
    pub run: Run,
    pub job: Job,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<InputDataset>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<OutputDataset>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    Start,
    Complete,
    Fail,
    Abort,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<RunFacets>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<ErrorMessageFacet>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorMessageFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub programming_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Job {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InputDataset {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<DatasetFacets>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutputDataset {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<DatasetFacets>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaDatasetFacet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_lineage: Option<ColumnLineageDatasetFacet>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaDatasetFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnLineageDatasetFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// Map from output column name to its input field references.
    pub fields: HashMap<String, ColumnLineageField>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnLineageField {
    pub input_fields: Vec<InputFieldRef>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InputFieldRef {
    pub namespace: String,
    pub name: String,
    pub field: String,
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PRODUCER: &str = "https://github.com/cache-has/armillary";
const SCHEMA_URL: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";
const SCHEMA_FACET_URL: &str =
    "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet";
const COLUMN_LINEAGE_FACET_URL: &str = "https://openlineage.io/spec/facets/1-0-2/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet";
const ERROR_MESSAGE_FACET_URL: &str = "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet";

// ---------------------------------------------------------------------------
// Fingerprint → Dataset mapping
// ---------------------------------------------------------------------------

/// Parsed dataset identity from a resource fingerprint string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetId {
    pub namespace: String,
    pub name: String,
}

/// Parse a `ResourceFingerprint` string into an OpenLineage dataset
/// namespace and name.
///
/// Examples:
/// - `postgres://host:5432/db/schema.table` → namespace=`postgres://host:5432`, name=`db.schema.table`
/// - `file:///path/to/data.csv` → namespace=`file://`, name=`/path/to/data.csv`
/// - `s3://bucket/key/path` → namespace=`s3://bucket`, name=`key/path`
pub fn parse_fingerprint(fingerprint: &str) -> DatasetId {
    // Try to split on "://" for URI-style fingerprints.
    if let Some(rest) = fingerprint.strip_prefix("file://") {
        return DatasetId {
            namespace: "file://".to_string(),
            name: rest.to_string(),
        };
    }

    if let Some(idx) = fingerprint.find("://") {
        let scheme = &fingerprint[..idx];
        let after_scheme = &fingerprint[idx + 3..];

        match scheme {
            "s3" | "gs" | "az" | "abfs" | "abfss" => {
                // s3://bucket/key → namespace=s3://bucket, name=key
                if let Some(slash) = after_scheme.find('/') {
                    let bucket = &after_scheme[..slash];
                    let key = &after_scheme[slash + 1..];
                    DatasetId {
                        namespace: format!("{scheme}://{bucket}"),
                        name: key.to_string(),
                    }
                } else {
                    DatasetId {
                        namespace: format!("{scheme}://{after_scheme}"),
                        name: String::new(),
                    }
                }
            }
            _ => {
                // postgres://host:5432/db/schema.table → namespace=postgres://host:5432, name=db.schema.table
                // Split on first '/' after host to get the path portion.
                if let Some(slash) = after_scheme.find('/') {
                    let authority = &after_scheme[..slash];
                    let path = &after_scheme[slash + 1..];
                    // Convert path separators to dots for database-style resources.
                    let name = path.replace('/', ".");
                    DatasetId {
                        namespace: format!("{scheme}://{authority}"),
                        name,
                    }
                } else {
                    DatasetId {
                        namespace: fingerprint.to_string(),
                        name: String::new(),
                    }
                }
            }
        }
    } else {
        // No scheme — treat the whole string as a name with a default namespace.
        DatasetId {
            namespace: "default".to_string(),
            name: fingerprint.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Arrow Schema → SchemaDatasetFacet
// ---------------------------------------------------------------------------

/// Convert an Arrow schema's fields to OpenLineage schema fields.
///
/// Takes field names and their Arrow `DataType` display strings.
pub fn arrow_fields_to_schema_facet(fields: &[(String, String)]) -> SchemaDatasetFacet {
    SchemaDatasetFacet {
        producer: PRODUCER.to_string(),
        schema_url: SCHEMA_FACET_URL.to_string(),
        fields: fields
            .iter()
            .map(|(name, dtype)| SchemaField {
                name: name.clone(),
                field_type: dtype.clone(),
                description: None,
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Column Lineage → ColumnLineageDatasetFacet
// ---------------------------------------------------------------------------

/// A simplified column lineage edge for OpenLineage emission.
///
/// Armillary's internal lineage model is richer (relationship kinds, confidence
/// levels). OpenLineage flattens this to "output column ← input fields".
#[derive(Debug, Clone)]
pub struct ColumnEdgeSimple {
    /// Downstream (output) column name.
    pub output_column: String,
    /// Upstream resource fingerprint.
    pub input_fingerprint: String,
    /// Upstream column name.
    pub input_column: String,
}

/// Build a column lineage facet from simplified edges.
///
/// The `default_namespace` is used for datasets whose fingerprint doesn't
/// parse to a known scheme.
pub fn build_column_lineage_facet(edges: &[ColumnEdgeSimple]) -> ColumnLineageDatasetFacet {
    let mut fields: HashMap<String, Vec<InputFieldRef>> = HashMap::new();

    for edge in edges {
        let dataset_id = parse_fingerprint(&edge.input_fingerprint);
        fields
            .entry(edge.output_column.clone())
            .or_default()
            .push(InputFieldRef {
                namespace: dataset_id.namespace,
                name: dataset_id.name,
                field: edge.input_column.clone(),
            });
    }

    ColumnLineageDatasetFacet {
        producer: PRODUCER.to_string(),
        schema_url: COLUMN_LINEAGE_FACET_URL.to_string(),
        fields: fields
            .into_iter()
            .map(|(col, input_fields)| (col, ColumnLineageField { input_fields }))
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// OpenLineageClient
// ---------------------------------------------------------------------------

/// HTTP client for emitting OpenLineage events.
///
/// Holds the configuration and a reusable HTTP client. All errors are
/// caught internally — emission failures never propagate to callers.
pub struct OpenLineageClient {
    config: OpenLineageConfig,
    http: reqwest::Client,
}

impl OpenLineageClient {
    /// Create a new client from configuration.
    ///
    /// Returns `None` if OpenLineage is disabled.
    pub fn new(config: &OpenLineageConfig) -> Option<Arc<Self>> {
        if !config.enabled {
            return None;
        }
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Some(Arc::new(Self {
            config: config.clone(),
            http,
        }))
    }

    /// The configured namespace for jobs.
    pub fn namespace(&self) -> &str {
        &self.config.namespace
    }

    /// Whether column lineage facets should be included.
    pub fn include_column_lineage(&self) -> bool {
        self.config.include_column_lineage
    }

    /// Emit a START event for a pipeline run.
    pub async fn emit_start(
        &self,
        pipeline_id: &str,
        environment: &str,
        run_id: &str,
        inputs: Vec<InputDataset>,
    ) {
        let event = RunEvent {
            event_time: now_iso8601(),
            event_type: EventType::Start,
            producer: PRODUCER.to_string(),
            schema_url: SCHEMA_URL.to_string(),
            run: Run {
                run_id: to_uuid_string(run_id),
                facets: None,
            },
            job: Job {
                namespace: self.config.namespace.clone(),
                name: format!("{pipeline_id}.{environment}"),
            },
            inputs,
            outputs: Vec::new(),
        };
        self.send(event).await;
    }

    /// Emit a COMPLETE event for a pipeline run.
    pub async fn emit_complete(
        &self,
        pipeline_id: &str,
        environment: &str,
        run_id: &str,
        inputs: Vec<InputDataset>,
        outputs: Vec<OutputDataset>,
    ) {
        let event = RunEvent {
            event_time: now_iso8601(),
            event_type: EventType::Complete,
            producer: PRODUCER.to_string(),
            schema_url: SCHEMA_URL.to_string(),
            run: Run {
                run_id: to_uuid_string(run_id),
                facets: None,
            },
            job: Job {
                namespace: self.config.namespace.clone(),
                name: format!("{pipeline_id}.{environment}"),
            },
            inputs,
            outputs,
        };
        self.send(event).await;
    }

    /// Emit a FAIL event for a pipeline run.
    pub async fn emit_fail(
        &self,
        pipeline_id: &str,
        environment: &str,
        run_id: &str,
        error: &str,
        inputs: Vec<InputDataset>,
        outputs: Vec<OutputDataset>,
    ) {
        let event = RunEvent {
            event_time: now_iso8601(),
            event_type: EventType::Fail,
            producer: PRODUCER.to_string(),
            schema_url: SCHEMA_URL.to_string(),
            run: Run {
                run_id: to_uuid_string(run_id),
                facets: Some(RunFacets {
                    error_message: Some(ErrorMessageFacet {
                        producer: PRODUCER.to_string(),
                        schema_url: ERROR_MESSAGE_FACET_URL.to_string(),
                        message: error.to_string(),
                        programming_language: Some("Rust".to_string()),
                        stack_trace: None,
                    }),
                }),
            },
            job: Job {
                namespace: self.config.namespace.clone(),
                name: format!("{pipeline_id}.{environment}"),
            },
            inputs,
            outputs,
        };
        self.send(event).await;
    }

    /// Emit an ABORT event for a pipeline run.
    pub async fn emit_abort(&self, pipeline_id: &str, environment: &str, run_id: &str) {
        let event = RunEvent {
            event_time: now_iso8601(),
            event_type: EventType::Abort,
            producer: PRODUCER.to_string(),
            schema_url: SCHEMA_URL.to_string(),
            run: Run {
                run_id: to_uuid_string(run_id),
                facets: None,
            },
            job: Job {
                namespace: self.config.namespace.clone(),
                name: format!("{pipeline_id}.{environment}"),
            },
            inputs: Vec::new(),
            outputs: Vec::new(),
        };
        self.send(event).await;
    }

    /// POST the event to the configured endpoint. Failures are logged but
    /// never propagated — OpenLineage must never break pipeline execution.
    async fn send(&self, event: RunEvent) {
        let result = self
            .http
            .post(&self.config.endpoint)
            .json(&event)
            .send()
            .await;
        match result {
            Ok(resp) if resp.status().is_success() => {
                tracing::debug!(
                    event_type = ?event.event_type,
                    job = %event.job.name,
                    "OpenLineage event emitted"
                );
            }
            Ok(resp) => {
                warn!(
                    status = %resp.status(),
                    job = %event.job.name,
                    "OpenLineage receiver returned non-success status"
                );
            }
            Err(e) => {
                warn!(
                    error = %e,
                    job = %event.job.name,
                    "failed to emit OpenLineage event"
                );
            }
        }
    }
}

impl std::fmt::Debug for OpenLineageClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenLineageClient")
            .field("config", &self.config)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_iso8601() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Convert an Armillary run ID to a UUID string.
///
/// Armillary run IDs are UUIDs but stored as plain strings. If the string is
/// already a valid UUID, use it directly. Otherwise generate a deterministic
/// UUID v5 from the string so the same run ID always produces the same
/// OpenLineage run ID.
fn to_uuid_string(run_id: &str) -> String {
    if uuid::Uuid::parse_str(run_id).is_ok() {
        return run_id.to_string();
    }
    // Deterministic UUID v5 from the run ID string using a fixed namespace.
    let ns = uuid::Uuid::NAMESPACE_URL;
    uuid::Uuid::new_v5(&ns, run_id.as_bytes()).to_string()
}

// ---------------------------------------------------------------------------
// Dataset builder helpers (for use by the executor)
// ---------------------------------------------------------------------------

/// Build an `InputDataset` from a fingerprint and optional schema fields.
pub fn input_dataset(
    fingerprint: &str,
    schema_fields: Option<&[(String, String)]>,
) -> InputDataset {
    let id = parse_fingerprint(fingerprint);
    let facets = schema_fields.map(|fields| DatasetFacets {
        schema: Some(arrow_fields_to_schema_facet(fields)),
        column_lineage: None,
    });
    InputDataset {
        namespace: id.namespace,
        name: id.name,
        facets,
    }
}

/// Build an `OutputDataset` from a fingerprint, optional schema fields, and
/// optional column lineage edges.
pub fn output_dataset(
    fingerprint: &str,
    schema_fields: Option<&[(String, String)]>,
    column_lineage: Option<&[ColumnEdgeSimple]>,
) -> OutputDataset {
    let id = parse_fingerprint(fingerprint);
    let schema_facet = schema_fields.map(arrow_fields_to_schema_facet);
    let lineage_facet = column_lineage
        .filter(|edges| !edges.is_empty())
        .map(build_column_lineage_facet);
    let facets = if schema_facet.is_some() || lineage_facet.is_some() {
        Some(DatasetFacets {
            schema: schema_facet,
            column_lineage: lineage_facet,
        })
    } else {
        None
    };
    OutputDataset {
        namespace: id.namespace,
        name: id.name,
        facets,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_postgres_fingerprint() {
        let id = parse_fingerprint("postgres://db.example.com:5432/analytics/public.orders");
        assert_eq!(id.namespace, "postgres://db.example.com:5432");
        assert_eq!(id.name, "analytics.public.orders");
    }

    #[test]
    fn parse_file_fingerprint() {
        let id = parse_fingerprint("file:///data/orders.csv");
        assert_eq!(id.namespace, "file://");
        assert_eq!(id.name, "/data/orders.csv");
    }

    #[test]
    fn parse_s3_fingerprint() {
        let id = parse_fingerprint("s3://my-bucket/path/to/orders/");
        assert_eq!(id.namespace, "s3://my-bucket");
        assert_eq!(id.name, "path/to/orders/");
    }

    #[test]
    fn parse_no_scheme_fingerprint() {
        let id = parse_fingerprint("some_table");
        assert_eq!(id.namespace, "default");
        assert_eq!(id.name, "some_table");
    }

    #[test]
    fn schema_facet_from_fields() {
        let fields = vec![
            ("id".to_string(), "Int64".to_string()),
            ("name".to_string(), "Utf8".to_string()),
        ];
        let facet = arrow_fields_to_schema_facet(&fields);
        assert_eq!(facet.fields.len(), 2);
        assert_eq!(facet.fields[0].name, "id");
        assert_eq!(facet.fields[0].field_type, "Int64");
        assert_eq!(facet.fields[1].name, "name");
    }

    #[test]
    fn column_lineage_facet_groups_by_output() {
        let edges = vec![
            ColumnEdgeSimple {
                output_column: "total".to_string(),
                input_fingerprint: "postgres://host:5432/db/public.orders".to_string(),
                input_column: "amount".to_string(),
            },
            ColumnEdgeSimple {
                output_column: "total".to_string(),
                input_fingerprint: "postgres://host:5432/db/public.orders".to_string(),
                input_column: "tax".to_string(),
            },
            ColumnEdgeSimple {
                output_column: "customer_name".to_string(),
                input_fingerprint: "postgres://host:5432/db/public.customers".to_string(),
                input_column: "name".to_string(),
            },
        ];
        let facet = build_column_lineage_facet(&edges);
        assert_eq!(facet.fields.len(), 2);
        assert_eq!(facet.fields["total"].input_fields.len(), 2);
        assert_eq!(facet.fields["customer_name"].input_fields.len(), 1);
    }

    #[test]
    fn run_event_serializes_to_valid_json() {
        let event = RunEvent {
            event_time: "2026-04-10T10:00:00.000Z".to_string(),
            event_type: EventType::Start,
            producer: PRODUCER.to_string(),
            schema_url: SCHEMA_URL.to_string(),
            run: Run {
                run_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                facets: None,
            },
            job: Job {
                namespace: "analytics".to_string(),
                name: "orders_ingest.prod".to_string(),
            },
            inputs: vec![InputDataset {
                namespace: "postgres://host:5432".to_string(),
                name: "db.public.orders".to_string(),
                facets: None,
            }],
            outputs: Vec::new(),
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["eventType"], "START");
        assert_eq!(json["job"]["name"], "orders_ingest.prod");
        assert_eq!(json["inputs"][0]["namespace"], "postgres://host:5432");
        // outputs should be absent (skip_serializing_if)
        assert!(json.get("outputs").is_none());
    }

    #[test]
    fn fail_event_includes_error_facet() {
        let event = RunEvent {
            event_time: "2026-04-10T10:00:00.000Z".to_string(),
            event_type: EventType::Fail,
            producer: PRODUCER.to_string(),
            schema_url: SCHEMA_URL.to_string(),
            run: Run {
                run_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                facets: Some(RunFacets {
                    error_message: Some(ErrorMessageFacet {
                        producer: PRODUCER.to_string(),
                        schema_url: ERROR_MESSAGE_FACET_URL.to_string(),
                        message: "connection refused".to_string(),
                        programming_language: Some("Rust".to_string()),
                        stack_trace: None,
                    }),
                }),
            },
            job: Job {
                namespace: "analytics".to_string(),
                name: "orders_ingest.prod".to_string(),
            },
            inputs: Vec::new(),
            outputs: Vec::new(),
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["eventType"], "FAIL");
        assert_eq!(
            json["run"]["facets"]["errorMessage"]["message"],
            "connection refused"
        );
    }

    #[test]
    fn to_uuid_preserves_valid_uuid() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(to_uuid_string(id), id);
    }

    #[test]
    fn to_uuid_converts_non_uuid_deterministically() {
        let id = "run-123";
        let result1 = to_uuid_string(id);
        let result2 = to_uuid_string(id);
        assert_eq!(result1, result2);
        // Should be a valid UUID.
        assert!(uuid::Uuid::parse_str(&result1).is_ok());
    }

    #[test]
    fn input_dataset_helper() {
        let ds = input_dataset(
            "postgres://host:5432/db/public.orders",
            Some(&[
                ("id".to_string(), "Int64".to_string()),
                ("name".to_string(), "Utf8".to_string()),
            ]),
        );
        assert_eq!(ds.namespace, "postgres://host:5432");
        assert_eq!(ds.name, "db.public.orders");
        assert!(ds.facets.is_some());
        assert_eq!(ds.facets.unwrap().schema.unwrap().fields.len(), 2);
    }

    #[test]
    fn output_dataset_helper_with_lineage() {
        let ds = output_dataset(
            "s3://bucket/output/",
            Some(&[("total".to_string(), "Float64".to_string())]),
            Some(&[ColumnEdgeSimple {
                output_column: "total".to_string(),
                input_fingerprint: "postgres://host:5432/db/public.orders".to_string(),
                input_column: "amount".to_string(),
            }]),
        );
        assert_eq!(ds.namespace, "s3://bucket");
        let facets = ds.facets.unwrap();
        assert!(facets.schema.is_some());
        assert!(facets.column_lineage.is_some());
    }

    #[test]
    fn client_returns_none_when_disabled() {
        let config = OpenLineageConfig::default();
        assert!(!config.enabled);
        assert!(OpenLineageClient::new(&config).is_none());
    }
}
