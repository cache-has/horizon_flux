// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline execution result.

use crate::stats::NodeStats;
use crate::test_assertion::TestNodeResult;
use armillary_engine::NodeId;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// The result of a complete pipeline execution.
#[derive(Debug)]
pub struct PipelineResult {
    pub pipeline_name: String,
    pub start_time: Instant,
    pub end_time: Instant,
    /// Output `RecordBatch`es keyed by the node that produced them.
    pub node_outputs: HashMap<NodeId, Vec<RecordBatch>>,
    /// Per-node execution statistics, in execution order.
    pub node_stats: Vec<NodeStats>,
    /// Results from test nodes executed during the pipeline run.
    pub test_results: Vec<TestNodeResult>,
}

impl PipelineResult {
    /// Wall-clock duration of the entire pipeline run.
    pub fn duration(&self) -> Duration {
        self.end_time.duration_since(self.start_time)
    }

    /// Sum of `rows_out` across all nodes.
    pub fn total_rows_out(&self) -> u64 {
        self.node_stats.iter().map(|s| s.rows_out).sum()
    }

    /// Get the output batches for a specific node.
    pub fn output_of(&self, node_id: &NodeId) -> Option<&Vec<RecordBatch>> {
        self.node_outputs.get(node_id)
    }
}
