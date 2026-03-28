// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! DAG validation and topological sorting for pipeline execution ordering.

use crate::error::DagError;
use crate::node::{NodeId, NodeKind};
use crate::pipeline::Pipeline;
use std::collections::{HashMap, HashSet, VecDeque};

/// Validate the pipeline DAG structure.
///
/// Checks performed:
/// - Pipeline is non-empty
/// - No duplicate node IDs
/// - No duplicate edges
/// - All edges reference known nodes
/// - Source nodes have no upstream edges
/// - Sink nodes have no downstream edges
/// - Transform and sink nodes have at least one upstream edge
/// - No cycles
/// - No orphan nodes (disconnected from all edges, unless the pipeline has exactly one node)
pub fn validate(pipeline: &Pipeline) -> Result<(), Vec<DagError>> {
    let mut errors = Vec::new();

    if pipeline.nodes.is_empty() {
        return Err(vec![DagError::EmptyPipeline]);
    }

    // Check for duplicate node IDs.
    let mut seen_ids = HashSet::new();
    for node in &pipeline.nodes {
        if !seen_ids.insert(&node.id) {
            errors.push(DagError::DuplicateNodeId(node.id.clone()));
        }
    }

    // Check for duplicate edges and unknown node references.
    let mut seen_edges = HashSet::new();
    for edge in &pipeline.edges {
        if !seen_ids.contains(&edge.from) {
            errors.push(DagError::UnknownNode(edge.from.clone()));
        }
        if !seen_ids.contains(&edge.to) {
            errors.push(DagError::UnknownNode(edge.to.clone()));
        }
        if !seen_edges.insert((&edge.from, &edge.to)) {
            errors.push(DagError::DuplicateEdge {
                from: edge.from.clone(),
                to: edge.to.clone(),
            });
        }
    }

    // Build adjacency info for type-compatibility checks.
    let mut has_upstream: HashSet<&NodeId> = HashSet::new();
    let mut has_downstream: HashSet<&NodeId> = HashSet::new();
    for edge in &pipeline.edges {
        has_upstream.insert(&edge.to);
        has_downstream.insert(&edge.from);
    }

    for node in &pipeline.nodes {
        match &node.kind {
            NodeKind::Source(_) => {
                if has_upstream.contains(&node.id) {
                    errors.push(DagError::SourceHasUpstream(node.id.clone()));
                }
            }
            NodeKind::Transform(_) => {
                if !has_upstream.contains(&node.id) {
                    errors.push(DagError::TransformMissingUpstream(node.id.clone()));
                }
            }
            NodeKind::Sink(_) => {
                if has_downstream.contains(&node.id) {
                    errors.push(DagError::SinkHasDownstream(node.id.clone()));
                }
                if !has_upstream.contains(&node.id) {
                    errors.push(DagError::SinkMissingUpstream(node.id.clone()));
                }
            }
        }
    }

    // Orphan detection: nodes with no edges at all (skip for single-node pipelines).
    if pipeline.nodes.len() > 1 {
        for node in &pipeline.nodes {
            if !has_upstream.contains(&node.id) && !has_downstream.contains(&node.id) {
                errors.push(DagError::OrphanNode(node.id.clone()));
            }
        }
    }

    // Cycle detection via Kahn's algorithm (only reliable when the graph structure is sound).
    if errors.is_empty() {
        if let Err(cycle_node) = detect_cycle(pipeline) {
            errors.push(DagError::CycleDetected(cycle_node));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Returns an error with a node involved in the cycle, or Ok(()) if acyclic.
fn detect_cycle(pipeline: &Pipeline) -> Result<(), NodeId> {
    let (in_degree, children) = build_adjacency(pipeline);
    let mut in_deg: HashMap<&NodeId, usize> = in_degree;
    let mut queue: VecDeque<&NodeId> = in_deg
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(id, _)| *id)
        .collect();

    let mut visited = 0usize;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        if let Some(kids) = children.get(node) {
            for kid in kids {
                if let Some(deg) = in_deg.get_mut(kid) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(kid);
                    }
                }
            }
        }
    }

    if visited == pipeline.nodes.len() {
        Ok(())
    } else {
        // Find a node still with non-zero in-degree (part of a cycle).
        let cycle_node = in_deg
            .iter()
            .find(|(_, deg)| **deg > 0)
            .map(|(id, _)| (*id).clone())
            .expect("should have at least one node in the cycle");
        Err(cycle_node)
    }
}

/// Compute a topological ordering of the pipeline nodes (Kahn's algorithm).
///
/// Returns node IDs in execution order. Nodes with no dependency ordering between
/// them may appear in any relative order.
///
/// Assumes the DAG has already been validated (no cycles). Panics if cycles exist.
pub fn topological_sort(pipeline: &Pipeline) -> Vec<NodeId> {
    let (in_degree, children) = build_adjacency(pipeline);
    let mut in_deg: HashMap<&NodeId, usize> = in_degree;
    let mut queue: VecDeque<&NodeId> = in_deg
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(id, _)| *id)
        .collect();

    let mut order = Vec::with_capacity(pipeline.nodes.len());
    while let Some(node) = queue.pop_front() {
        order.push(node.clone());
        if let Some(kids) = children.get(node) {
            for kid in kids {
                if let Some(deg) = in_deg.get_mut(kid) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(kid);
                    }
                }
            }
        }
    }

    assert_eq!(
        order.len(),
        pipeline.nodes.len(),
        "topological_sort called on a pipeline with cycles"
    );
    order
}

/// Build in-degree map and adjacency list from the pipeline edges.
fn build_adjacency(
    pipeline: &Pipeline,
) -> (HashMap<&NodeId, usize>, HashMap<&NodeId, Vec<&NodeId>>) {
    let mut in_degree: HashMap<&NodeId, usize> = HashMap::new();
    let mut children: HashMap<&NodeId, Vec<&NodeId>> = HashMap::new();

    for node in &pipeline.nodes {
        in_degree.entry(&node.id).or_insert(0);
    }
    for edge in &pipeline.edges {
        *in_degree.entry(&edge.to).or_insert(0) += 1;
        children.entry(&edge.from).or_default().push(&edge.to);
    }

    (in_degree, children)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::Edge;
    use crate::node::*;
    use crate::pipeline::Variable;
    use std::collections::HashMap;

    fn source_node(id: &str) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.to_string(),
            kind: NodeKind::Source(SourceConfig {
                connector: "csv".into(),
                config: serde_json::Value::Null,
            }),
            position: Position::default(),
            pinned_position: false,
        }
    }

    fn transform_node(id: &str) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.to_string(),
            kind: NodeKind::Transform(TransformConfig {
                mode: TransformMode::Sql,
                code: "SELECT * FROM upstream".into(),
                materialized: false,
            }),
            position: Position::default(),
            pinned_position: false,
        }
    }

    fn sink_node(id: &str) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.to_string(),
            kind: NodeKind::Sink(SinkConfig {
                connector: "stdout".into(),
                config: serde_json::Value::Null,
            }),
            position: Position::default(),
            pinned_position: false,
        }
    }

    fn simple_pipeline() -> Pipeline {
        Pipeline {
            name: "test".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: HashMap::new(),
            environment_overrides: HashMap::new(),
            nodes: vec![
                source_node("src"),
                transform_node("xform"),
                sink_node("sink"),
            ],
            edges: vec![Edge::new("src", "xform"), Edge::new("xform", "sink")],
        }
    }

    #[test]
    fn valid_linear_pipeline() {
        let p = simple_pipeline();
        assert!(validate(&p).is_ok());
    }

    #[test]
    fn topological_order_linear() {
        let p = simple_pipeline();
        let order = topological_sort(&p);
        let ids: Vec<&str> = order.iter().map(|id| id.0.as_str()).collect();
        assert_eq!(ids, vec!["src", "xform", "sink"]);
    }

    #[test]
    fn multi_input_transform() {
        let p = Pipeline {
            name: "multi_input".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: HashMap::new(),
            environment_overrides: HashMap::new(),
            nodes: vec![
                source_node("src_a"),
                source_node("src_b"),
                transform_node("join"),
                sink_node("out"),
            ],
            edges: vec![
                Edge::new("src_a", "join"),
                Edge::new("src_b", "join"),
                Edge::new("join", "out"),
            ],
        };
        assert!(validate(&p).is_ok());
        let order = topological_sort(&p);
        // Both sources must come before the join, join before out.
        let pos = |id: &str| order.iter().position(|n| n.0 == id).unwrap();
        assert!(pos("src_a") < pos("join"));
        assert!(pos("src_b") < pos("join"));
        assert!(pos("join") < pos("out"));
    }

    #[test]
    fn detects_cycle() {
        let p = Pipeline {
            name: "cyclic".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: HashMap::new(),
            environment_overrides: HashMap::new(),
            nodes: vec![
                source_node("a"),
                transform_node("b"),
                transform_node("c"),
                sink_node("d"),
            ],
            edges: vec![
                Edge::new("a", "b"),
                Edge::new("b", "c"),
                Edge::new("c", "b"), // cycle: b <-> c
                Edge::new("c", "d"),
            ],
        };
        let errs = validate(&p).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, DagError::CycleDetected(_))));
    }

    #[test]
    fn detects_orphan() {
        let p = Pipeline {
            name: "orphan".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: HashMap::new(),
            environment_overrides: HashMap::new(),
            nodes: vec![
                source_node("src"),
                transform_node("xform"),
                sink_node("sink"),
                source_node("orphan"), // not connected
            ],
            edges: vec![Edge::new("src", "xform"), Edge::new("xform", "sink")],
        };
        let errs = validate(&p).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, DagError::OrphanNode(_))));
    }

    #[test]
    fn detects_source_with_upstream() {
        let p = Pipeline {
            name: "bad_source".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: HashMap::new(),
            environment_overrides: HashMap::new(),
            nodes: vec![source_node("a"), source_node("b"), sink_node("out")],
            edges: vec![
                Edge::new("a", "b"), // b is a source but has upstream
                Edge::new("b", "out"),
            ],
        };
        let errs = validate(&p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, DagError::SourceHasUpstream(_)))
        );
    }

    #[test]
    fn detects_empty_pipeline() {
        let p = Pipeline {
            name: "empty".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: HashMap::new(),
            environment_overrides: HashMap::new(),
            nodes: vec![],
            edges: vec![],
        };
        let errs = validate(&p).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, DagError::EmptyPipeline)));
    }

    #[test]
    fn detects_duplicate_node_id() {
        let p = Pipeline {
            name: "dup".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: HashMap::new(),
            environment_overrides: HashMap::new(),
            nodes: vec![source_node("a"), source_node("a"), sink_node("out")],
            edges: vec![Edge::new("a", "out")],
        };
        let errs = validate(&p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, DagError::DuplicateNodeId(_)))
        );
    }

    #[test]
    fn detects_unknown_node_in_edge() {
        let p = Pipeline {
            name: "unknown".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: HashMap::new(),
            environment_overrides: HashMap::new(),
            nodes: vec![source_node("src"), sink_node("sink")],
            edges: vec![
                Edge::new("src", "ghost"), // ghost doesn't exist
                Edge::new("src", "sink"),
            ],
        };
        let errs = validate(&p).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, DagError::UnknownNode(_))));
    }

    #[test]
    fn diamond_dag_valid() {
        // src -> a, src -> b, a -> join, b -> join, join -> sink
        let p = Pipeline {
            name: "diamond".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: HashMap::new(),
            environment_overrides: HashMap::new(),
            nodes: vec![
                source_node("src"),
                transform_node("a"),
                transform_node("b"),
                transform_node("join"),
                sink_node("sink"),
            ],
            edges: vec![
                Edge::new("src", "a"),
                Edge::new("src", "b"),
                Edge::new("a", "join"),
                Edge::new("b", "join"),
                Edge::new("join", "sink"),
            ],
        };
        assert!(validate(&p).is_ok());
        let order = topological_sort(&p);
        let pos = |id: &str| order.iter().position(|n| n.0 == id).unwrap();
        assert!(pos("src") < pos("a"));
        assert!(pos("src") < pos("b"));
        assert!(pos("a") < pos("join"));
        assert!(pos("b") < pos("join"));
        assert!(pos("join") < pos("sink"));
    }

    #[test]
    fn pipeline_serde_roundtrip() {
        let p = simple_pipeline();
        let json = serde_json::to_string_pretty(&p).unwrap();
        let p2: Pipeline = serde_json::from_str(&json).unwrap();
        assert_eq!(p2.name, p.name);
        assert_eq!(p2.nodes.len(), 3);
        assert_eq!(p2.edges.len(), 2);
    }

    #[test]
    fn variable_serde() {
        let var = Variable {
            var_type: crate::pipeline::VariableType::String,
            default: Some(serde_json::json!("hello")),
        };
        let json = serde_json::to_string(&var).unwrap();
        let var2: Variable = serde_json::from_str(&json).unwrap();
        assert_eq!(var2.var_type, crate::pipeline::VariableType::String);
    }
}
