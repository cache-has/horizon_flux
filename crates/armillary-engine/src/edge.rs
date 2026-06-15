// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::node::NodeId;
use serde::{Deserialize, Serialize};

/// A directed edge in the pipeline DAG: data flows from `from` to `to`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
}

impl Edge {
    pub fn new(from: impl Into<NodeId>, to: impl Into<NodeId>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
        }
    }
}
