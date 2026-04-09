// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Incremental execution coordinator for sink materializations (planning doc 27).
//!
//! This module owns the *pre-pass* that turns a pipeline's incremental
//! materialization policies into a concrete plan the executor can act on
//! before any I/O happens. The pre-pass exists for one reason: ambiguity
//! that would otherwise turn into a 10M-row scan with no warning needs to
//! become a clear error *before* we connect to anything.
//!
//! The shape:
//!
//! - For each sink with `read_mode: incremental`, walk the DAG backwards to
//!   the source(s) that feed it. Build an [`IncrementalReadPlan`] keyed by
//!   source node id describing what filter to apply when that source is
//!   read.
//! - For each such sink, also build an [`IncrementalSinkPlan`] carrying the
//!   loaded [`IncrementalState`] (or `None` for first runs) and the parsed
//!   lookback. The executor consults this when persisting the new state and
//!   computing the [`MaterializationReceipt`] after the sink commits.
//!
//! Errors raised here are deliberate hard failures. The dbt papercut we are
//! avoiding is "system says everything is fine while quietly doing the
//! wrong thing." If the watermark column is missing, types mismatch, or two
//! sinks fight over a source with conflicting watermark columns, the run
//! fails before any source is read. The escape hatch is `--full-refresh`
//! (skips the filter entirely) or `incremental reset` (clears the state),
//! both of which are explicit user actions.

use crate::error::{IncrementalStateError, NodeErrorKind};
use crate::incremental_state::IncrementalState;
use crate::storage::IncrementalStateStorage;
use crate::watermark::{LookbackDuration, WatermarkError, parse_lookback};
use flux_engine::Pipeline;
use flux_engine::materialization::{FirstRun, MaterializationPolicy, ReadMode, WatermarkType};
use flux_engine::node::{NodeId, NodeKind};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

/// Plan applied to a single source node when its associated incremental
/// sink wants a watermark filter on the read.
#[derive(Debug, Clone)]
pub struct IncrementalReadPlan {
    /// Sink the plan came from. Used purely for error messages.
    pub sink_node_id: NodeId,
    /// Watermark column name to filter on.
    pub column: String,
    /// Declared watermark type from the sink's policy.
    pub wtype: WatermarkType,
    /// Loaded state, or `None` when this is the first run for this sink.
    pub state: Option<IncrementalState>,
    /// Parsed lookback duration to subtract from the stored watermark before
    /// constructing the filter scalar.
    pub lookback: LookbackDuration,
    /// What to do when state is `None`.
    pub first_run: FirstRun,
}

/// Plan attached to an incremental sink. Carries the state the executor
/// needs to persist a new row after the write commits.
#[derive(Debug, Clone)]
pub struct IncrementalSinkPlan {
    pub policy: MaterializationPolicy,
    pub state: Option<IncrementalState>,
    pub lookback: LookbackDuration,
}

/// Combined output of [`build_plans`].
#[derive(Debug, Default)]
pub struct IncrementalPlans {
    pub source_plans: HashMap<NodeId, IncrementalReadPlan>,
    pub sink_plans: HashMap<NodeId, IncrementalSinkPlan>,
}

/// Errors raised by the pre-pass. All are hard failures — see module-level
/// docs for the design rationale.
#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error(
        "incremental sink `{sink}`: conflicting watermark column on source `{source_id}` (already declared as `{existing}` by sink `{other_sink}`, this sink declared `{column}`)"
    )]
    ConflictingWatermarkColumn {
        sink: String,
        other_sink: String,
        source_id: String,
        existing: String,
        column: String,
    },

    #[error(
        "incremental sink `{sink}`: conflicting watermark type on source `{source_id}` (already declared as `{existing}` by sink `{other_sink}`, this sink declared `{wtype}`)"
    )]
    ConflictingWatermarkType {
        sink: String,
        other_sink: String,
        source_id: String,
        existing: String,
        wtype: String,
    },

    #[error(
        "incremental sink `{sink}` has no upstream source nodes — incremental read mode requires at least one source"
    )]
    NoUpstreamSource { sink: String },

    #[error(
        "incremental sink `{sink}`: first_run policy is `fail` and no incremental state exists yet — rerun with `--bootstrap-incremental` to perform the initial load"
    )]
    BootstrapRequired { sink: String },

    #[error("incremental state load failed: {0}")]
    State(#[from] IncrementalStateError),

    #[error("watermark error: {0}")]
    Watermark(#[from] WatermarkError),
}

impl From<CoordinatorError> for NodeErrorKind {
    fn from(err: CoordinatorError) -> Self {
        NodeErrorKind::Source(Box::new(std::io::Error::other(err.to_string())))
    }
}

/// Walk the pipeline once and build read+sink plans for every incremental sink.
///
/// `pipeline_id` is whatever string identifies this pipeline in the metadata
/// store; the executor passes `pipeline.name` here to match the run-store
/// convention.
pub fn build_plans(
    pipeline: &Pipeline,
    pipeline_id: &str,
    environment: &str,
    state_store: Option<&Arc<dyn IncrementalStateStorage>>,
    full_refresh: bool,
    bootstrap_incremental: bool,
) -> Result<IncrementalPlans, CoordinatorError> {
    let mut plans = IncrementalPlans::default();

    for node in &pipeline.nodes {
        let NodeKind::Sink(sink_cfg) = &node.kind else {
            continue;
        };
        let Some(policy) = sink_cfg.materialization.as_ref() else {
            continue;
        };
        if policy.read_mode != ReadMode::Incremental {
            continue;
        }
        let watermark = policy
            .watermark
            .as_ref()
            .expect("validate_policy guarantees watermark presence under incremental");

        let lookback = parse_lookback(&policy.lookback)?;

        // Load existing state (None when this is a first run).
        let state = if let Some(store) = state_store {
            store.load_state(pipeline_id, &node.id.0, environment)?
        } else {
            None
        };

        // First-run gating. `--full-refresh` is allowed to skip this check
        // because it intentionally re-runs the pipeline as if no state
        // existed and then advances state at the end.
        if state.is_none() && !full_refresh {
            match policy.first_run {
                FirstRun::Full => { /* no-op: read everything */ }
                FirstRun::Fail => {
                    if !bootstrap_incremental {
                        return Err(CoordinatorError::BootstrapRequired {
                            sink: node.id.0.clone(),
                        });
                    }
                }
            }
        }

        plans.sink_plans.insert(
            node.id.clone(),
            IncrementalSinkPlan {
                policy: policy.clone(),
                state: state.clone(),
                lookback,
            },
        );

        // Walk DAG backwards from this sink to find every reachable source
        // node, and propagate the read plan onto each. `--full-refresh`
        // skips filter propagation entirely so the read is unfiltered.
        if full_refresh {
            continue;
        }
        let sources = collect_upstream_sources(pipeline, &node.id);
        if sources.is_empty() {
            return Err(CoordinatorError::NoUpstreamSource {
                sink: node.id.0.clone(),
            });
        }
        for src_id in sources {
            let new_plan = IncrementalReadPlan {
                sink_node_id: node.id.clone(),
                column: watermark.column.clone(),
                wtype: watermark.watermark_type,
                state: state.clone(),
                lookback,
                first_run: policy.first_run,
            };
            match plans.source_plans.get(&src_id) {
                None => {
                    plans.source_plans.insert(src_id, new_plan);
                }
                Some(existing) => {
                    if existing.column != new_plan.column {
                        return Err(CoordinatorError::ConflictingWatermarkColumn {
                            sink: node.id.0.clone(),
                            other_sink: existing.sink_node_id.0.clone(),
                            source_id: src_id.0.clone(),
                            existing: existing.column.clone(),
                            column: new_plan.column,
                        });
                    }
                    if existing.wtype != new_plan.wtype {
                        return Err(CoordinatorError::ConflictingWatermarkType {
                            sink: node.id.0.clone(),
                            other_sink: existing.sink_node_id.0.clone(),
                            source_id: src_id.0.clone(),
                            existing: format!("{:?}", existing.wtype),
                            wtype: format!("{:?}", new_plan.wtype),
                        });
                    }
                    // Same column, same type — merge by taking the
                    // *minimum* stored watermark so neither sink is
                    // shorted. None always wins (it means "first run, read
                    // everything") because it represents a wider window.
                    let merged_state = match (&existing.state, &new_plan.state) {
                        (None, _) | (_, None) => None,
                        (Some(a), Some(b)) => Some(if a.watermark_value <= b.watermark_value {
                            a.clone()
                        } else {
                            b.clone()
                        }),
                    };
                    let merged_lookback = LookbackDuration {
                        seconds: existing.lookback.seconds.max(new_plan.lookback.seconds),
                    };
                    plans.source_plans.insert(
                        src_id,
                        IncrementalReadPlan {
                            sink_node_id: existing.sink_node_id.clone(),
                            column: existing.column.clone(),
                            wtype: existing.wtype,
                            state: merged_state,
                            lookback: merged_lookback,
                            first_run: existing.first_run,
                        },
                    );
                }
            }
        }
    }

    Ok(plans)
}

/// BFS from a sink back to the source nodes that feed it.
fn collect_upstream_sources(pipeline: &Pipeline, sink: &NodeId) -> Vec<NodeId> {
    let mut visited = std::collections::HashSet::new();
    let mut queue: VecDeque<NodeId> = VecDeque::new();
    queue.push_back(sink.clone());
    let mut sources = Vec::new();
    while let Some(current) = queue.pop_front() {
        if !visited.insert(current.clone()) {
            continue;
        }
        let upstream = pipeline.upstream_of(&current);
        if upstream.is_empty() {
            // Either a source, or an orphaned non-source. Only count source nodes.
            if let Some(node) = pipeline.node(&current) {
                if matches!(node.kind, NodeKind::Source(_)) {
                    sources.push(current);
                }
            }
            continue;
        }
        for u in upstream {
            queue.push_back(u.clone());
        }
    }
    sources
}

#[cfg(test)]
mod tests {
    use super::*;
    use flux_engine::edge::Edge;
    use flux_engine::materialization::{ReadMode, Watermark, WatermarkType, WriteStrategy};
    use flux_engine::node::{Node, Position, SinkConfig, SourceConfig};

    fn source(id: &str) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.into(),
            kind: NodeKind::Source(SourceConfig {
                connector: "csv".into(),
                config: serde_json::Value::Null,
                cache_row_limit: None,
            }),
            position: Position::default(),
            pinned_position: false,
        }
    }

    fn incr_sink(id: &str, column: &str, wtype: WatermarkType) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.into(),
            kind: NodeKind::Sink(SinkConfig {
                connector: "stdout".into(),
                config: serde_json::Value::Null,
                materialization: Some(MaterializationPolicy {
                    read_mode: ReadMode::Incremental,
                    write_strategy: WriteStrategy::Append,
                    watermark: Some(Watermark {
                        column: column.into(),
                        watermark_type: wtype,
                    }),
                    ..Default::default()
                }),
            }),
            position: Position::default(),
            pinned_position: false,
        }
    }

    fn pipeline_with(nodes: Vec<Node>, edges: Vec<Edge>) -> Pipeline {
        Pipeline {
            name: "test".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: Default::default(),
            environment_overrides: Default::default(),
            sample_config: None,
            cache_row_limit: None,
            code_dir: None,
            nodes,
            edges,
        }
    }

    #[test]
    fn first_run_full_creates_plan_without_state() {
        let p = pipeline_with(
            vec![
                source("src"),
                incr_sink("sink", "ts", WatermarkType::Timestamp),
            ],
            vec![Edge::new("src", "sink")],
        );
        let plans = build_plans(&p, "test", "dev", None, false, false).unwrap();
        assert_eq!(plans.sink_plans.len(), 1);
        assert_eq!(plans.source_plans.len(), 1);
        let plan = plans.source_plans.get(&NodeId::new("src")).unwrap();
        assert_eq!(plan.column, "ts");
        assert!(plan.state.is_none());
    }

    #[test]
    fn first_run_fail_without_bootstrap_errors() {
        let mut sink = incr_sink("sink", "ts", WatermarkType::Timestamp);
        if let NodeKind::Sink(ref mut s) = sink.kind {
            s.materialization.as_mut().unwrap().first_run = FirstRun::Fail;
        }
        let p = pipeline_with(vec![source("src"), sink], vec![Edge::new("src", "sink")]);
        let err = build_plans(&p, "test", "dev", None, false, false).unwrap_err();
        assert!(matches!(err, CoordinatorError::BootstrapRequired { .. }));
    }

    #[test]
    fn first_run_fail_with_bootstrap_succeeds() {
        let mut sink = incr_sink("sink", "ts", WatermarkType::Timestamp);
        if let NodeKind::Sink(ref mut s) = sink.kind {
            s.materialization.as_mut().unwrap().first_run = FirstRun::Fail;
        }
        let p = pipeline_with(vec![source("src"), sink], vec![Edge::new("src", "sink")]);
        let plans = build_plans(&p, "test", "dev", None, false, true).unwrap();
        assert_eq!(plans.source_plans.len(), 1);
    }

    #[test]
    fn full_refresh_skips_source_plans() {
        let p = pipeline_with(
            vec![
                source("src"),
                incr_sink("sink", "ts", WatermarkType::Timestamp),
            ],
            vec![Edge::new("src", "sink")],
        );
        let plans = build_plans(&p, "test", "dev", None, true, false).unwrap();
        assert_eq!(plans.sink_plans.len(), 1);
        assert!(plans.source_plans.is_empty());
    }

    #[test]
    fn conflicting_columns_on_shared_source_errors() {
        let p = pipeline_with(
            vec![
                source("src"),
                incr_sink("sink_a", "ts_a", WatermarkType::Timestamp),
                incr_sink("sink_b", "ts_b", WatermarkType::Timestamp),
            ],
            vec![Edge::new("src", "sink_a"), Edge::new("src", "sink_b")],
        );
        let err = build_plans(&p, "test", "dev", None, false, false).unwrap_err();
        assert!(matches!(
            err,
            CoordinatorError::ConflictingWatermarkColumn { .. }
        ));
    }

    #[test]
    fn matching_columns_on_shared_source_merge() {
        let p = pipeline_with(
            vec![
                source("src"),
                incr_sink("sink_a", "ts", WatermarkType::Timestamp),
                incr_sink("sink_b", "ts", WatermarkType::Timestamp),
            ],
            vec![Edge::new("src", "sink_a"), Edge::new("src", "sink_b")],
        );
        let plans = build_plans(&p, "test", "dev", None, false, false).unwrap();
        assert_eq!(plans.source_plans.len(), 1);
        assert_eq!(plans.sink_plans.len(), 2);
    }
}
