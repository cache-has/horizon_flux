// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Desktop notification helpers using `notify-rust`.

use flux_datafusion::RunId;
use flux_engine::NodeId;
use tracing::warn;

/// Notify that a pipeline run completed successfully.
pub fn send_success(run_id: &RunId, duration_ms: u64) {
    let secs = duration_ms as f64 / 1000.0;
    let body = format!("Run {run_id} completed in {secs:.1}s");
    send("Pipeline Succeeded", &body);
}

/// Notify that a pipeline run failed.
pub fn send_failure(run_id: &RunId, error: Option<&str>) {
    let body = match error {
        Some(e) => format!("Run {run_id} failed: {e}"),
        None => format!("Run {run_id} failed"),
    };
    send("Pipeline Failed", &body);
}

/// Notify that a specific node in a pipeline run failed.
pub fn send_node_failure(run_id: &RunId, node_id: &NodeId, error: &str) {
    let body = format!("Run {run_id} — node '{node_id}' failed: {error}");
    send("Pipeline Node Failed", &body);
}

fn send(summary: &str, body: &str) {
    if let Err(e) = notify_rust::Notification::new()
        .appname("Horizon Flux")
        .summary(summary)
        .body(body)
        .timeout(notify_rust::Timeout::Milliseconds(5000))
        .show()
    {
        warn!("Failed to send desktop notification: {e}");
    }
}

#[cfg(test)]
mod tests {
    fn format_success_body(run_id: &str, duration_ms: u64) -> String {
        let secs = duration_ms as f64 / 1000.0;
        format!("Run {run_id} completed in {secs:.1}s")
    }

    fn format_failure_body(run_id: &str, error: Option<&str>) -> String {
        match error {
            Some(e) => format!("Run {run_id} failed: {e}"),
            None => format!("Run {run_id} failed"),
        }
    }

    #[test]
    fn format_success_body_shows_seconds() {
        let body = format_success_body("abc-123", 12_500);
        assert_eq!(body, "Run abc-123 completed in 12.5s");
    }

    #[test]
    fn format_failure_body_with_error() {
        let body = format_failure_body("abc-123", Some("division by zero"));
        assert_eq!(body, "Run abc-123 failed: division by zero");
    }

    #[test]
    fn format_failure_body_without_error() {
        let body = format_failure_body("abc-123", None);
        assert_eq!(body, "Run abc-123 failed");
    }
}
