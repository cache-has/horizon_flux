// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Freshness SLA API routes (planning doc 37, sub-feature 3).
//!
//! Exposes SLA compliance status and evaluation history for resources that
//! have declared freshness SLAs in their annotation metadata.

use crate::api::ApiError;
use crate::state::AppState;
use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use flux_engine::SlaEvaluation;
use serde::{Deserialize, Serialize};

/// Build the `/sla` sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/status", get(list_status))
        .route("/status/{fingerprint}", get(get_status))
        .route("/history/{fingerprint}", get(get_history))
}

// ---------------------------------------------------------------------------
// Query / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct StatusListQuery {
    /// Filter by status (ok, warning, breach, unknown).
    #[serde(default)]
    status: Option<String>,
    /// Filter by tag on the resource.
    #[serde(default)]
    tag: Option<String>,
    /// Filter by owner team on the resource.
    #[serde(default)]
    owner: Option<String>,
    /// Environment for catalog resolution (defaults to "default").
    #[serde(default = "default_env")]
    env: String,
}

fn default_env() -> String {
    "default".into()
}

#[derive(Debug, Serialize)]
struct SlaStatusResponse {
    data: Vec<SlaStatusEntry>,
    total: usize,
}

/// A single resource's SLA status, enriched with catalog metadata.
#[derive(Debug, Serialize)]
struct SlaStatusEntry {
    fingerprint: String,
    name: String,
    #[serde(flatten)]
    evaluation: SlaEvaluation,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
}

#[derive(Debug, Serialize)]
struct SlaDetailResponse {
    evaluation: SlaEvaluation,
    history: Vec<SlaEvaluation>,
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    50
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /api/sla/status` — list current SLA compliance for all resources with SLAs.
async fn list_status(
    State(state): State<AppState>,
    Query(q): Query<StatusListQuery>,
) -> Result<Json<SlaStatusResponse>, (StatusCode, Json<ApiError>)> {
    let sla_store = state.sla_store.as_ref().ok_or_else(|| {
        ApiError::internal("SLA storage not configured")
    })?;

    let evaluations = sla_store
        .latest_evaluations()
        .map_err(|e| ApiError::internal(e.to_string()))?;

    // Build catalog for name/tag/owner enrichment.
    let catalog = super::catalog::build_catalog_public(&state, &q.env)?;

    let mut entries: Vec<SlaStatusEntry> = evaluations
        .into_iter()
        .map(|eval| {
            let fp = flux_engine::ResourceFingerprint::new(&eval.fingerprint);
            let catalog_entry = catalog.get(&fp);
            let name = catalog_entry
                .map(|e| e.name.clone())
                .unwrap_or_else(|| eval.fingerprint.clone());
            let tags = catalog_entry
                .map(|e| e.tags.clone())
                .unwrap_or_default();
            let owner = catalog_entry
                .and_then(|e| e.owner.as_ref())
                .and_then(|o| o.team.clone());

            SlaStatusEntry {
                fingerprint: eval.fingerprint.clone(),
                name,
                evaluation: eval,
                tags,
                owner,
            }
        })
        .collect();

    // Apply filters.
    if let Some(status_filter) = &q.status {
        entries.retain(|e| e.evaluation.status.as_str() == status_filter);
    }
    if let Some(tag_filter) = &q.tag {
        entries.retain(|e| e.tags.iter().any(|t| t == tag_filter));
    }
    if let Some(owner_filter) = &q.owner {
        entries.retain(|e| e.owner.as_ref().is_some_and(|o| o == owner_filter));
    }

    let total = entries.len();
    Ok(Json(SlaStatusResponse { data: entries, total }))
}

/// `GET /api/sla/status/:fingerprint` — current SLA status + recent history for a resource.
async fn get_status(
    State(state): State<AppState>,
    Path(fingerprint): Path<String>,
) -> Result<Json<SlaDetailResponse>, (StatusCode, Json<ApiError>)> {
    let sla_store = state.sla_store.as_ref().ok_or_else(|| {
        ApiError::internal("SLA storage not configured")
    })?;

    // Axum's Path extractor already URL-decodes the parameter.

    let evaluation = sla_store
        .latest_evaluation(&fingerprint)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found("SLA evaluation", &fingerprint))?;

    let history = sla_store
        .evaluation_history(&fingerprint, 20)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(SlaDetailResponse { evaluation, history }))
}

/// `GET /api/sla/history/:fingerprint` — historical evaluations for a resource.
async fn get_history(
    State(state): State<AppState>,
    Path(fingerprint): Path<String>,
    Query(q): Query<HistoryQuery>,
) -> Result<Json<Vec<SlaEvaluation>>, (StatusCode, Json<ApiError>)> {
    let sla_store = state.sla_store.as_ref().ok_or_else(|| {
        ApiError::internal("SLA storage not configured")
    })?;

    let history = sla_store
        .evaluation_history(&fingerprint, q.limit)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(history))
}
