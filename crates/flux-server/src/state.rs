// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared application state for all API handlers.

use flux_connectors::ConnectorRegistry;
use flux_datafusion::{EnvironmentStore, RunStore};
use flux_engine::PipelineStore;
use flux_secrets::SecretStore;
use std::sync::{Arc, Mutex};

/// Shared state available to all request handlers via Axum's `State` extractor.
#[derive(Clone)]
pub struct AppState {
    pub pipeline_store: Arc<PipelineStore>,
    pub run_store: Arc<RunStore>,
    pub connector_registry: Arc<ConnectorRegistry>,
    pub environment_store: Arc<EnvironmentStore>,
    /// Optional encrypted secret store. `None` when `HORIZON_FLUX_SECRET_PASSWORD`
    /// is not set or the store has not been initialized via `horizon-flux secret init`.
    pub secret_store: Option<Arc<Mutex<SecretStore>>>,
}
