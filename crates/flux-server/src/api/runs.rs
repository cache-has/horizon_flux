// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Top-level run API routes (not scoped under a pipeline).
//!
//! These endpoints allow fetching run details and comparing runs by ID
//! without requiring the pipeline ID in the URL path.

use crate::api::ApiError;
use crate::state::AppState;
use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use flux_datafusion::RunId;
use serde::Serialize;
use std::collections::HashMap;

/// Build the `/runs` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/{run_id}", get(get_run_by_id))
        .route("/{run_id}/compare/{other_id}", get(compare_runs))
}

/// `GET /api/runs/:run_id` — get a run by ID without needing the pipeline ID.
async fn get_run_by_id(
    State(state): State<AppState>,
    Path(run_id_str): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
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
// Run comparison (planning doc 37, sub-feature 1)
// ---------------------------------------------------------------------------

/// Per-node comparison entry in the run comparison response.
#[derive(Debug, Serialize)]
struct NodeComparison {
    node_id: String,
    duration_ms_a: Option<u64>,
    duration_ms_b: Option<u64>,
    duration_delta_ms: Option<i64>,
    rows_in_a: Option<u64>,
    rows_in_b: Option<u64>,
    rows_in_delta: Option<i64>,
    rows_out_a: Option<u64>,
    rows_out_b: Option<u64>,
    rows_out_delta: Option<i64>,
    error_a: Option<String>,
    error_b: Option<String>,
    /// Present only when one run has a node the other doesn't.
    #[serde(skip_serializing_if = "Option::is_none")]
    presence: Option<String>,
}

/// Test result comparison entry.
#[derive(Debug, Serialize)]
struct TestComparison {
    node_id: String,
    passed_a: Option<bool>,
    passed_b: Option<bool>,
    changed: bool,
}

/// Full run comparison response.
#[derive(Debug, Serialize)]
struct RunComparisonResponse {
    run_id_a: String,
    run_id_b: String,
    pipeline_name_a: String,
    pipeline_name_b: String,
    status_a: String,
    status_b: String,
    duration_ms_a: Option<u64>,
    duration_ms_b: Option<u64>,
    duration_delta_ms: Option<i64>,
    total_rows_out_a: u64,
    total_rows_out_b: u64,
    total_rows_out_delta: i64,
    node_comparisons: Vec<NodeComparison>,
    test_comparisons: Vec<TestComparison>,
}

/// `GET /api/runs/:run_id/compare/:other_id` — compare two runs side-by-side.
async fn compare_runs(
    State(state): State<AppState>,
    Path((run_id_a_str, run_id_b_str)): Path<(String, String)>,
) -> Result<Json<RunComparisonResponse>, (StatusCode, Json<ApiError>)> {
    let run_uuid_a = uuid::Uuid::parse_str(&run_id_a_str)
        .map_err(|_| ApiError::bad_request(format!("invalid run ID: {run_id_a_str}")))?;
    let run_uuid_b = uuid::Uuid::parse_str(&run_id_b_str)
        .map_err(|_| ApiError::bad_request(format!("invalid run ID: {run_id_b_str}")))?;

    let run_a = state
        .run_store
        .get_run(&RunId(run_uuid_a))
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("run", &run_id_a_str))?;

    let run_b = state
        .run_store
        .get_run(&RunId(run_uuid_b))
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("run", &run_id_b_str))?;

    // Build node stats maps keyed by node_id.
    let stats_a: HashMap<&str, _> = run_a
        .node_stats
        .iter()
        .map(|s| (s.node_id.0.as_str(), s))
        .collect();
    let stats_b: HashMap<&str, _> = run_b
        .node_stats
        .iter()
        .map(|s| (s.node_id.0.as_str(), s))
        .collect();

    // Collect all node IDs from both runs.
    let mut all_node_ids: Vec<&str> = stats_a.keys().copied().collect();
    for id in stats_b.keys() {
        if !stats_a.contains_key(id) {
            all_node_ids.push(id);
        }
    }
    all_node_ids.sort();

    let mut node_comparisons = Vec::with_capacity(all_node_ids.len());
    let mut total_rows_out_a: u64 = 0;
    let mut total_rows_out_b: u64 = 0;

    for node_id in &all_node_ids {
        let a = stats_a.get(node_id);
        let b = stats_b.get(node_id);

        let dur_a = a.map(|s| s.duration_ms());
        let dur_b = b.map(|s| s.duration_ms());
        let rows_in_a = a.map(|s| s.rows_in);
        let rows_in_b = b.map(|s| s.rows_in);
        let rows_out_a = a.map(|s| s.rows_out);
        let rows_out_b = b.map(|s| s.rows_out);

        total_rows_out_a += rows_out_a.unwrap_or(0);
        total_rows_out_b += rows_out_b.unwrap_or(0);

        let presence = match (a, b) {
            (Some(_), None) => Some("only_in_a".to_string()),
            (None, Some(_)) => Some("only_in_b".to_string()),
            _ => None,
        };

        node_comparisons.push(NodeComparison {
            node_id: node_id.to_string(),
            duration_ms_a: dur_a,
            duration_ms_b: dur_b,
            duration_delta_ms: match (dur_a, dur_b) {
                (Some(a), Some(b)) => Some(b as i64 - a as i64),
                _ => None,
            },
            rows_in_a,
            rows_in_b,
            rows_in_delta: match (rows_in_a, rows_in_b) {
                (Some(a), Some(b)) => Some(b as i64 - a as i64),
                _ => None,
            },
            rows_out_a,
            rows_out_b,
            rows_out_delta: match (rows_out_a, rows_out_b) {
                (Some(a), Some(b)) => Some(b as i64 - a as i64),
                _ => None,
            },
            error_a: a.and_then(|s| s.error.clone()),
            error_b: b.and_then(|s| s.error.clone()),
            presence,
        });
    }

    // Compare test results.
    let tests_a: HashMap<&str, bool> = run_a
        .test_results
        .iter()
        .map(|t| (t.node_id.0.as_str(), t.passed))
        .collect();
    let tests_b: HashMap<&str, bool> = run_b
        .test_results
        .iter()
        .map(|t| (t.node_id.0.as_str(), t.passed))
        .collect();

    let mut test_node_ids: Vec<&str> = tests_a.keys().copied().collect();
    for id in tests_b.keys() {
        if !tests_a.contains_key(id) {
            test_node_ids.push(id);
        }
    }
    test_node_ids.sort();

    let test_comparisons: Vec<TestComparison> = test_node_ids
        .iter()
        .map(|id| {
            let pa = tests_a.get(id).copied();
            let pb = tests_b.get(id).copied();
            TestComparison {
                node_id: id.to_string(),
                passed_a: pa,
                passed_b: pb,
                changed: pa != pb,
            }
        })
        .collect();

    let dur_a = run_a.duration_ms();
    let dur_b = run_b.duration_ms();

    Ok(Json(RunComparisonResponse {
        run_id_a: run_a.id.to_string(),
        run_id_b: run_b.id.to_string(),
        pipeline_name_a: run_a.pipeline_name,
        pipeline_name_b: run_b.pipeline_name,
        status_a: run_a.status.as_str().to_string(),
        status_b: run_b.status.as_str().to_string(),
        duration_ms_a: dur_a,
        duration_ms_b: dur_b,
        duration_delta_ms: match (dur_a, dur_b) {
            (Some(a), Some(b)) => Some(b as i64 - a as i64),
            _ => None,
        },
        total_rows_out_a,
        total_rows_out_b,
        total_rows_out_delta: total_rows_out_b as i64 - total_rows_out_a as i64,
        node_comparisons,
        test_comparisons,
    }))
}
