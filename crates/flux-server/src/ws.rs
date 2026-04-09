// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! WebSocket endpoint for real-time pipeline execution events.
//!
//! Clients connect to `/ws` and receive JSON-serialized [`ExecutionEvent`]
//! messages as pipelines execute. Clients can optionally send a subscribe
//! message to filter events to specific run IDs.

use crate::state::AppState;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use flux_datafusion::{ExecutionEvent, RunId};
use serde::Deserialize;
use std::collections::HashSet;
use tracing::{debug, warn};

/// Client-to-server messages for filtering events.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    /// Subscribe to events for specific run IDs. Replaces any previous filter.
    Subscribe { run_ids: Vec<RunId> },
    /// Clear filters — receive all events.
    Unsubscribe,
}

/// Handle WebSocket upgrade request.
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    debug!("WebSocket client connected");
    let mut rx = state.event_tx.subscribe();
    let mut plugin_rx = state.plugin_event_tx.subscribe();
    let mut catalog_rx = state.catalog_event_tx.subscribe();
    let mut filter: Option<HashSet<RunId>> = None;

    loop {
        tokio::select! {
            // Catalog events (metadata annotation changes). Not subject to
            // run-id filtering.
            result = catalog_rx.recv() => {
                match result {
                    Ok(event) => {
                        match serde_json::to_string(&event) {
                            Ok(json) => {
                                if socket.send(Message::Text(json.into())).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => warn!("Failed to serialize catalog event: {e}"),
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("WebSocket client lagged, dropped {n} catalog events");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }

            // Plugin lifecycle events. Not subject to run-id filtering.
            result = plugin_rx.recv() => {
                match result {
                    Ok(event) => {
                        match serde_json::to_string(&event) {
                            Ok(json) => {
                                if socket.send(Message::Text(json.into())).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => warn!("Failed to serialize plugin event: {e}"),
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("WebSocket client lagged, dropped {n} plugin events");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }

            // Receive events from the broadcast channel and forward to client.
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        // Apply run ID filter if set.
                        if let Some(ref ids) = filter {
                            if !event_matches_filter(&event, ids) {
                                continue;
                            }
                        }

                        match serde_json::to_string(&event) {
                            Ok(json) => {
                                if socket.send(Message::Text(json.into())).await.is_err() {
                                    break; // Client disconnected.
                                }
                            }
                            Err(e) => {
                                warn!("Failed to serialize execution event: {e}");
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("WebSocket client lagged, dropped {n} events");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break; // Channel closed (server shutting down).
                    }
                }
            }

            // Receive messages from the client (subscribe/unsubscribe).
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(ClientMessage::Subscribe { run_ids }) => {
                                debug!(count = run_ids.len(), "WebSocket client subscribed to runs");
                                filter = Some(run_ids.into_iter().collect());
                            }
                            Ok(ClientMessage::Unsubscribe) => {
                                debug!("WebSocket client cleared filter");
                                filter = None;
                            }
                            Err(e) => {
                                debug!("Ignoring invalid client message: {e}");
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {} // Ignore ping/pong/binary.
                }
            }
        }
    }

    debug!("WebSocket client disconnected");
}

/// Check if an event's run ID is in the filter set.
fn event_matches_filter(event: &ExecutionEvent, ids: &HashSet<RunId>) -> bool {
    let run_id = match event {
        ExecutionEvent::RunStarted { run_id, .. }
        | ExecutionEvent::NodeStarted { run_id, .. }
        | ExecutionEvent::NodeCompleted { run_id, .. }
        | ExecutionEvent::NodeFailed { run_id, .. }
        | ExecutionEvent::TestNodePassed { run_id, .. }
        | ExecutionEvent::TestNodeFailed { run_id, .. }
        | ExecutionEvent::RunCompleted { run_id, .. } => run_id,
        // Trigger and backfill events are not scoped to a run — always pass through.
        ExecutionEvent::TriggerChanged { .. } | ExecutionEvent::Backfill(_) => return true,
    };
    ids.contains(run_id)
}
