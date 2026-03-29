// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline management API routes.

use crate::api::{ApiError, PaginatedResponse, Pagination};
use crate::state::AppState;
use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use flux_datafusion::{
    ColumnStats, ExecutionOptions, PipelineExecutor, PreviewOptions, RunId, compute_column_stats,
};
use flux_engine::pipeline_store::PipelineId;
use flux_engine::{Pipeline, SampleConfig};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::mpsc;
use tracing::error;

/// Build the `/pipelines` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_pipelines).post(create_pipeline))
        .route("/import", post(import_pipeline))
        .route("/export", post(bulk_export))
        .route(
            "/{id}",
            get(get_pipeline)
                .put(update_pipeline)
                .delete(delete_pipeline),
        )
        .route("/{id}/export", get(export_pipeline))
        .route("/{id}/run", post(run_pipeline))
        .route("/{id}/preview", post(preview_pipeline))
        .route("/{id}/runs", get(list_runs))
        .route("/{id}/runs/{run_id}", get(get_run))
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Pipeline response with metadata (returned from CRUD endpoints).
#[derive(Debug, Serialize)]
struct PipelineResponse {
    id: PipelineId,
    pipeline: Pipeline,
    created_at: u64,
    updated_at: u64,
    last_run_at: Option<u64>,
    run_count: u32,
}

impl From<flux_engine::PipelineRecord> for PipelineResponse {
    fn from(r: flux_engine::PipelineRecord) -> Self {
        Self {
            id: r.id,
            pipeline: r.pipeline,
            created_at: system_time_to_ms(r.created_at),
            updated_at: system_time_to_ms(r.updated_at),
            last_run_at: r.last_run_at.map(system_time_to_ms),
            run_count: r.run_count,
        }
    }
}

/// How to handle name conflicts during import.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ImportConflict {
    /// Reject the import if the name already exists (default).
    #[default]
    Reject,
    /// Rename the imported pipeline by appending a suffix.
    Rename,
    /// Overwrite the existing pipeline with the imported definition.
    Overwrite,
}

/// Request body for pipeline import.
#[derive(Debug, Deserialize)]
struct ImportRequest {
    /// The pipeline definition JSON (as an object, not a string).
    pipeline: serde_json::Value,
    /// How to handle name conflicts.
    #[serde(default)]
    on_conflict: ImportConflict,
}

/// Response from pipeline import.
#[derive(Debug, Serialize)]
struct ImportResponse {
    #[serde(flatten)]
    pipeline: PipelineResponse,
    /// Non-fatal warnings (e.g. undefined variable references).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    /// Connector compatibility warnings.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    connector_warnings: Vec<String>,
}

/// Request body for triggering a pipeline run.
#[derive(Debug, Deserialize)]
struct RunRequest {
    #[serde(default = "default_environment")]
    environment: String,
    /// Runtime variable overrides (highest precedence).
    #[serde(default)]
    variables: std::collections::HashMap<String, serde_json::Value>,
}

fn default_environment() -> String {
    "dev".to_string()
}

/// Request body for pipeline preview.
#[derive(Debug, Deserialize)]
struct PreviewRequest {
    #[serde(default)]
    sample: Option<SampleConfig>,
    /// Runtime variable overrides for preview.
    #[serde(default)]
    variables: std::collections::HashMap<String, serde_json::Value>,
}

/// Serializable preview node result (Arrow schemas/batches → JSON).
#[derive(Debug, Serialize)]
pub struct PreviewNodeResponse {
    pub node_id: String,
    pub columns: Vec<ColumnInfo>,
    pub row_count: u64,
    pub duration_ms: u64,
    pub rows: Vec<serde_json::Value>,
    pub column_stats: Vec<ColumnStats>,
}

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Full preview response.
#[derive(Debug, Serialize)]
struct PreviewResponse {
    pipeline_name: String,
    execution_order: Vec<String>,
    nodes: Vec<PreviewNodeResponse>,
    duration_ms: u64,
    sample_method: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /api/pipelines` — list all pipelines with pagination.
async fn list_pipelines(
    State(state): State<AppState>,
    Query(page): Query<Pagination>,
) -> Result<Json<PaginatedResponse<PipelineResponse>>, (StatusCode, Json<ApiError>)> {
    let total = state
        .pipeline_store
        .count()
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let records = state
        .pipeline_store
        .list(page.limit, page.offset)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(PaginatedResponse {
        data: records.into_iter().map(PipelineResponse::from).collect(),
        total,
        limit: page.limit,
        offset: page.offset,
    }))
}

/// `GET /api/pipelines/:id` — get a single pipeline by ID.
async fn get_pipeline(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<PipelineResponse>, (StatusCode, Json<ApiError>)> {
    let pipeline_id = parse_pipeline_id(&id)?;
    let record = state
        .pipeline_store
        .get(&pipeline_id)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("pipeline", &id))?;
    Ok(Json(PipelineResponse::from(record)))
}

/// `POST /api/pipelines` — create a new pipeline.
async fn create_pipeline(
    State(state): State<AppState>,
    Json(pipeline): Json<Pipeline>,
) -> Result<(StatusCode, Json<PipelineResponse>), (StatusCode, Json<ApiError>)> {
    if pipeline.name.trim().is_empty() {
        return Err(ApiError::bad_request("pipeline name must not be empty"));
    }

    let record = state.pipeline_store.create(pipeline).map_err(|e| {
        use flux_engine::PipelineStoreError;
        match &e {
            PipelineStoreError::NameConflict(name) => {
                ApiError::conflict(format!("pipeline `{name}` already exists"))
            }
            _ => ApiError::internal(e.to_string()),
        }
    })?;

    Ok((StatusCode::CREATED, Json(PipelineResponse::from(record))))
}

/// `PUT /api/pipelines/:id` — update an existing pipeline.
async fn update_pipeline(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(pipeline): Json<Pipeline>,
) -> Result<Json<PipelineResponse>, (StatusCode, Json<ApiError>)> {
    let pipeline_id = parse_pipeline_id(&id)?;

    if pipeline.name.trim().is_empty() {
        return Err(ApiError::bad_request("pipeline name must not be empty"));
    }

    let record = state
        .pipeline_store
        .update(&pipeline_id, pipeline)
        .map_err(|e| {
            use flux_engine::PipelineStoreError;
            match &e {
                PipelineStoreError::NotFound(_) => ApiError::not_found("pipeline", &id),
                PipelineStoreError::NameConflict(name) => {
                    ApiError::conflict(format!("pipeline `{name}` already exists"))
                }
                _ => ApiError::internal(e.to_string()),
            }
        })?;

    Ok(Json(PipelineResponse::from(record)))
}

/// `DELETE /api/pipelines/:id` — delete a pipeline.
async fn delete_pipeline(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    let pipeline_id = parse_pipeline_id(&id)?;
    state.pipeline_store.delete(&pipeline_id).map_err(|e| {
        use flux_engine::PipelineStoreError;
        match &e {
            PipelineStoreError::NotFound(_) => ApiError::not_found("pipeline", &id),
            _ => ApiError::internal(e.to_string()),
        }
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/pipelines/:id/run` — trigger pipeline execution.
async fn run_pipeline(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<RunRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ApiError>)> {
    let pipeline_id = parse_pipeline_id(&id)?;
    let record = state
        .pipeline_store
        .get(&pipeline_id)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("pipeline", &id))?;

    let provider_registry = state.connector_registry.to_provider_registry();

    // Set up a progress channel that forwards execution events to the
    // broadcast channel for WebSocket clients.
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
    let event_tx = state.event_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = progress_rx.recv().await {
            let _ = event_tx.send(event);
        }
    });

    // Validate variable overrides against declared types.
    let override_errors =
        flux_engine::variables::validate_overrides(&record.pipeline, &req.variables);
    if !override_errors.is_empty() {
        return Err(ApiError::bad_request(override_errors.join("; ")));
    }

    let options = ExecutionOptions {
        environment: req.environment,
        run_store: Some(Arc::clone(&state.run_store)),
        cancel: Arc::new(AtomicBool::new(false)),
        environment_resolver: None,
        progress: Some(progress_tx),
        variable_overrides: req.variables,
    };

    let (_result, run) = PipelineExecutor::execute(&record.pipeline, &provider_registry, &options)
        .await
        .map_err(|e| {
            error!(pipeline = %record.pipeline.name, error = %e, "pipeline execution failed");
            ApiError::internal(e.to_string())
        })?;

    // Update run metadata (last_run_at, run_count) — best-effort.
    if let Err(e) = state.pipeline_store.record_run(&pipeline_id) {
        error!(pipeline = %record.pipeline.name, error = %e, "failed to record run metadata");
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::to_value(&run).unwrap()),
    ))
}

/// `POST /api/pipelines/:id/preview` — run preview on sample data.
async fn preview_pipeline(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<PreviewRequest>,
) -> Result<Json<PreviewResponse>, (StatusCode, Json<ApiError>)> {
    let pipeline_id = parse_pipeline_id(&id)?;
    let record = state
        .pipeline_store
        .get(&pipeline_id)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("pipeline", &id))?;

    let provider_registry = state.connector_registry.to_provider_registry();
    // Use request sample config, then pipeline default, then global default.
    let sample = req
        .sample
        .or_else(|| record.pipeline.sample_config.clone())
        .unwrap_or_default();
    let options = PreviewOptions {
        sample,
        cancel: Arc::new(AtomicBool::new(false)),
        progress: None,
        variable_overrides: req.variables,
    };

    let preview = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        PipelineExecutor::preview(&record.pipeline, &provider_registry, &options),
    )
    .await
    .map_err(|_| {
        ApiError::gateway_timeout("preview timed out after 5 seconds")
    })?
    .map_err(|e| {
        error!(pipeline = %record.pipeline.name, error = %e, "preview failed");
        ApiError::internal(e.to_string())
    })?;

    let sample_method = format_sample_method(&preview.sample_config);

    let nodes: Vec<PreviewNodeResponse> = preview
        .execution_order
        .iter()
        .filter_map(|nid| {
            preview.nodes.get(nid).map(|nr| {
                let columns: Vec<ColumnInfo> = nr
                    .schema
                    .fields()
                    .iter()
                    .map(|f| ColumnInfo {
                        name: f.name().clone(),
                        data_type: format!("{}", f.data_type()),
                        nullable: f.is_nullable(),
                    })
                    .collect();

                let rows = batches_to_json_rows(&nr.batches);
                let column_stats = compute_column_stats(&nr.batches);

                PreviewNodeResponse {
                    node_id: nid.0.clone(),
                    columns,
                    row_count: nr.row_count,
                    duration_ms: nr.duration.as_millis() as u64,
                    rows,
                    column_stats,
                }
            })
        })
        .collect();

    Ok(Json(PreviewResponse {
        pipeline_name: preview.pipeline_name,
        execution_order: preview
            .execution_order
            .iter()
            .map(|n| n.0.clone())
            .collect(),
        nodes,
        duration_ms: preview.duration.as_millis() as u64,
        sample_method,
    }))
}

/// `GET /api/pipelines/:id/runs` — list execution history for a pipeline.
async fn list_runs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(page): Query<Pagination>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let pipeline_id = parse_pipeline_id(&id)?;
    let record = state
        .pipeline_store
        .get(&pipeline_id)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("pipeline", &id))?;

    let runs = state
        .run_store
        .list_runs(Some(&record.pipeline.name), page.limit)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(serde_json::to_value(&runs).unwrap()))
}

/// `GET /api/pipelines/:id/runs/:run_id` — get detailed run results.
async fn get_run(
    State(state): State<AppState>,
    Path((id, run_id_str)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    // Validate pipeline exists.
    let pipeline_id = parse_pipeline_id(&id)?;
    let _record = state
        .pipeline_store
        .get(&pipeline_id)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("pipeline", &id))?;

    let run_uuid = uuid::Uuid::parse_str(&run_id_str)
        .map_err(|_| ApiError::bad_request(format!("invalid run ID: {run_id_str}")))?;
    let run_id = RunId(run_uuid);

    let run = state
        .run_store
        .get_run(&run_id)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("run", &run_id_str))?;

    Ok(Json(serde_json::to_value(&run).unwrap()))
}

// ---------------------------------------------------------------------------
// Import / Export handlers
// ---------------------------------------------------------------------------

/// `GET /api/pipelines/:id/export` — download pipeline definition as a JSON file.
async fn export_pipeline(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, (StatusCode, Json<ApiError>)> {
    let pipeline_id = parse_pipeline_id(&id)?;
    let record = state
        .pipeline_store
        .get(&pipeline_id)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("pipeline", &id))?;

    let json = record
        .pipeline
        .to_json()
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let filename = sanitize_filename(&record.pipeline.name);
    Ok((
        StatusCode::OK,
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/json".to_string(),
            ),
            (
                axum::http::header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}.json\""),
            ),
        ],
        json,
    )
        .into_response())
}

/// `POST /api/pipelines/import` — import a pipeline from JSON.
async fn import_pipeline(
    State(state): State<AppState>,
    Json(req): Json<ImportRequest>,
) -> Result<(StatusCode, Json<ImportResponse>), (StatusCode, Json<ApiError>)> {
    // Parse and validate the pipeline definition.
    let json_str = serde_json::to_string(&req.pipeline)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let (mut pipeline, import_warnings) =
        Pipeline::from_json_with_warnings(&json_str).map_err(|e| match e {
            flux_engine::ImportError::Json(je) => {
                ApiError::bad_request(format!("invalid pipeline JSON: {je}"))
            }
            flux_engine::ImportError::Validation(errors) => ApiError::bad_request(format!(
                "pipeline validation failed: {}",
                errors
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            )),
        })?;

    // Collect non-fatal warnings.
    let warnings: Vec<String> = import_warnings
        .undefined_variables
        .iter()
        .map(|w| w.to_string())
        .collect();

    // Check connector compatibility (non-fatal warnings).
    let connector_warnings = match state.connector_registry.validate_pipeline(&pipeline) {
        Ok(()) => vec![],
        Err(errors) => errors,
    };

    // Handle name conflicts.
    let existing = state
        .pipeline_store
        .get_by_name(&pipeline.name)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let record = if let Some(existing_record) = existing {
        match req.on_conflict {
            ImportConflict::Reject => {
                return Err(ApiError::conflict(format!(
                    "pipeline `{}` already exists (use on_conflict: \"rename\" or \"overwrite\")",
                    pipeline.name
                )));
            }
            ImportConflict::Rename => {
                // Find a unique name by appending a counter.
                let base_name = pipeline.name.clone();
                let mut counter = 2u32;
                loop {
                    let candidate = format!("{base_name} ({counter})");
                    let conflict = state
                        .pipeline_store
                        .get_by_name(&candidate)
                        .map_err(|e| ApiError::internal(e.to_string()))?;
                    if conflict.is_none() {
                        pipeline.name = candidate;
                        break;
                    }
                    counter += 1;
                    if counter > 100 {
                        return Err(ApiError::internal(
                            "could not find a unique name after 100 attempts",
                        ));
                    }
                }
                state
                    .pipeline_store
                    .create(pipeline)
                    .map_err(|e| ApiError::internal(e.to_string()))?
            }
            ImportConflict::Overwrite => state
                .pipeline_store
                .update(&existing_record.id, pipeline)
                .map_err(|e| ApiError::internal(e.to_string()))?,
        }
    } else {
        state
            .pipeline_store
            .create(pipeline)
            .map_err(|e| ApiError::internal(e.to_string()))?
    };

    Ok((
        StatusCode::CREATED,
        Json(ImportResponse {
            pipeline: PipelineResponse::from(record),
            warnings,
            connector_warnings,
        }),
    ))
}

/// `POST /api/pipelines/export` — bulk export all pipelines as a JSON object.
async fn bulk_export(
    State(state): State<AppState>,
) -> Result<Response, (StatusCode, Json<ApiError>)> {
    let total = state
        .pipeline_store
        .count()
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let records = state
        .pipeline_store
        .list(total, 0)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    // Build a map of pipeline name -> definition.
    let mut export: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for record in &records {
        let value = serde_json::to_value(&record.pipeline)
            .map_err(|e| ApiError::internal(e.to_string()))?;
        export.insert(record.pipeline.name.clone(), value);
    }

    let json = serde_json::to_string_pretty(&export)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok((
        StatusCode::OK,
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/json".to_string(),
            ),
            (
                axum::http::header::CONTENT_DISPOSITION,
                "attachment; filename=\"horizon-flux-pipelines.json\"".to_string(),
            ),
        ],
        json,
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn format_sample_method(config: &SampleConfig) -> String {
    match config {
        SampleConfig::FirstN { count } => format!("first {count}"),
        SampleConfig::Random { count, .. } => format!("random {count}"),
        SampleConfig::Full => "full".to_string(),
    }
}

fn parse_pipeline_id(s: &str) -> Result<PipelineId, (StatusCode, Json<ApiError>)> {
    s.parse::<PipelineId>()
        .map_err(|_| ApiError::bad_request(format!("invalid pipeline ID: {s}")))
}

fn system_time_to_ms(t: std::time::SystemTime) -> u64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Sanitize a pipeline name for use as a filename.
fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

/// Convert Arrow RecordBatches to JSON row objects.
pub fn batches_to_json_rows(
    batches: &[arrow::record_batch::RecordBatch],
) -> Vec<serde_json::Value> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        for batch in batches {
            if let Err(e) = writer.write(batch) {
                error!("failed to serialize Arrow batch to JSON: {e}");
                break;
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

    #[test]
    fn column_info_serializes_nullable() {
        let col = ColumnInfo {
            name: "age".into(),
            data_type: "Int32".into(),
            nullable: true,
        };
        let json = serde_json::to_value(&col).unwrap();
        assert_eq!(json["name"], "age");
        assert_eq!(json["data_type"], "Int32");
        assert_eq!(json["nullable"], true);

        let not_null = ColumnInfo {
            name: "id".into(),
            data_type: "Int64".into(),
            nullable: false,
        };
        let json2 = serde_json::to_value(&not_null).unwrap();
        assert_eq!(json2["nullable"], false);
    }
}
