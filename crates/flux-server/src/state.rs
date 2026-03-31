// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared application state for all API handlers.

use flux_connectors::ConnectorRegistry;
use flux_datafusion::{EnvironmentStore, ExecutionEvent, OutputCache, RunStore};
use flux_engine::PipelineStore;
use flux_secrets::SecretStore;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Default capacity for the execution event broadcast channel.
const EVENT_CHANNEL_CAPACITY: usize = 256;

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
    /// Broadcast channel for real-time execution events (WebSocket consumers
    /// subscribe via `event_tx.subscribe()`).
    pub event_tx: broadcast::Sender<ExecutionEvent>,
    /// On-disk cache for materialized node outputs (preview reads from here).
    pub output_cache: Arc<OutputCache>,
}

impl AppState {
    /// Create a new broadcast sender for execution events.
    pub fn new_event_channel() -> broadcast::Sender<ExecutionEvent> {
        broadcast::channel(EVENT_CHANNEL_CAPACITY).0
    }
}
