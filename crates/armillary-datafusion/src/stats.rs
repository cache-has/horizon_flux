// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-node execution statistics.

use armillary_engine::NodeId;
use std::time::{Duration, Instant};

/// Statistics captured during a single node's execution.
#[derive(Debug, Clone)]
pub struct NodeStats {
    pub node_id: NodeId,
    pub start_time: Instant,
    pub end_time: Instant,
    /// Total rows received from upstream nodes (0 for source nodes).
    pub rows_in: u64,
    /// Total rows produced by this node.
    pub rows_out: u64,
    /// Error message, if the node failed.
    pub error: Option<String>,
}

impl NodeStats {
    /// Wall-clock duration of this node's execution.
    pub fn duration(&self) -> Duration {
        self.end_time.duration_since(self.start_time)
    }
}
