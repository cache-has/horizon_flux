// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cross-pipeline lineage tracking.
//!
//! Tracks resource-level dependencies between pipelines: when one pipeline's
//! sink writes to a resource that another pipeline's source reads from, a
//! lineage edge exists between them.

use crate::node::NodeKind;
use crate::pipeline::Pipeline;
use crate::pipeline_store::PipelineId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

// ---------------------------------------------------------------------------
// ResourceFingerprint
// ---------------------------------------------------------------------------

/// A canonical, secret-free identifier for an external resource.
///
/// Two connectors that read/write the same underlying resource (e.g. the same
/// Postgres table) must produce identical fingerprints. Fingerprints are
/// environment-scoped — the same table in dev and prod are different resources.
///
/// # Rules
/// - **Secret-aware:** never includes credentials, passwords, or tokens.
/// - **Canonicalized:** paths absolute, hostnames lowercased, identifiers unquoted.
/// - **Unambiguous:** different resources never collide; equivalent resources always match.
///
/// # Examples
/// - `postgres://db.example.com:5432/analytics/public.orders`
/// - `file:///absolute/path/to/data/orders.csv`
/// - `s3://my-bucket/path/to/orders/`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResourceFingerprint(pub String);

impl ResourceFingerprint {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl fmt::Display for ResourceFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// Resource binding (pipeline ↔ resource relationship)
// ---------------------------------------------------------------------------

/// Direction of a pipeline node's relationship to a resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingDirection {
    Source,
    Sink,
}

impl fmt::Display for BindingDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source => f.write_str("source"),
            Self::Sink => f.write_str("sink"),
        }
    }
}

/// A binding between a pipeline node and an external resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceBinding {
    pub pipeline_id: PipelineId,
    pub node_id: String,
    pub direction: BindingDirection,
    pub fingerprint: ResourceFingerprint,
}

// ---------------------------------------------------------------------------
// Lineage edge types
// ---------------------------------------------------------------------------

/// How a lineage edge was discovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeSource {
    /// Inferred statically from pipeline definitions at load time.
    Static,
    /// Observed from actual pipeline execution history.
    Observed,
}

/// A directed dependency edge between two pipelines.
///
/// Represents "upstream pipeline's sink writes to a resource that downstream
/// pipeline's source reads from."
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageEdge {
    /// The pipeline that produces the data.
    pub upstream_pipeline_id: PipelineId,
    /// The sink node in the upstream pipeline.
    pub upstream_node_id: String,
    /// The pipeline that consumes the data.
    pub downstream_pipeline_id: PipelineId,
    /// The source node in the downstream pipeline.
    pub downstream_node_id: String,
    /// The shared resource connecting them.
    pub fingerprint: ResourceFingerprint,
    /// How this edge was discovered.
    pub source: EdgeSource,
}

// ---------------------------------------------------------------------------
// Fingerprinting function type
// ---------------------------------------------------------------------------

/// Signature for connector fingerprinting functions.
///
/// Given a connector type name and its opaque JSON config, returns `None` if
/// the connector does not participate in lineage (e.g. stdout, REST API), or
/// `Some(fingerprint)` if it does.
pub type FingerprintFn = fn(&str, &serde_json::Value) -> Option<ResourceFingerprint>;

// ---------------------------------------------------------------------------
// LineageGraph
// ---------------------------------------------------------------------------

/// Cross-pipeline lineage graph built from resource fingerprint matching.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LineageGraph {
    /// All discovered edges.
    pub edges: Vec<LineageEdge>,
    /// Resource bindings used to build the graph.
    pub bindings: Vec<ResourceBinding>,
}

impl LineageGraph {
    /// Build a lineage graph from a set of pipelines using static analysis.
    ///
    /// `fingerprint_fn` is called for each source/sink node to compute its
    /// resource fingerprint. Pass `armillary_connectors::fingerprint::fingerprint`
    /// for built-in connectors.
    pub fn from_pipelines(
        pipelines: &[(PipelineId, &Pipeline)],
        fingerprint_fn: FingerprintFn,
    ) -> Self {
        let mut bindings = Vec::new();

        // Collect all resource bindings from all pipelines.
        for (pipeline_id, pipeline) in pipelines {
            for node in &pipeline.nodes {
                match &node.kind {
                    NodeKind::Source(src) => {
                        if let Some(fp) = fingerprint_fn(&src.connector, &src.config) {
                            bindings.push(ResourceBinding {
                                pipeline_id: pipeline_id.clone(),
                                node_id: node.id.0.clone(),
                                direction: BindingDirection::Source,
                                fingerprint: fp,
                            });
                        }
                    }
                    NodeKind::Sink(sink) => {
                        if let Some(fp) = fingerprint_fn(&sink.connector, &sink.config) {
                            bindings.push(ResourceBinding {
                                pipeline_id: pipeline_id.clone(),
                                node_id: node.id.0.clone(),
                                direction: BindingDirection::Sink,
                                fingerprint: fp,
                            });
                        }
                    }
                    _ => {}
                }
            }
        }

        // Match sinks to sources on the same fingerprint to create edges.
        let edges = Self::derive_static_edges(&bindings);

        LineageGraph { edges, bindings }
    }

    /// Derive static edges by matching sink fingerprints to source fingerprints.
    fn derive_static_edges(bindings: &[ResourceBinding]) -> Vec<LineageEdge> {
        // Index sinks by fingerprint.
        let mut sinks_by_fp: HashMap<&ResourceFingerprint, Vec<&ResourceBinding>> = HashMap::new();
        for b in bindings {
            if b.direction == BindingDirection::Sink {
                sinks_by_fp.entry(&b.fingerprint).or_default().push(b);
            }
        }

        let mut edges = Vec::new();
        for b in bindings {
            if b.direction != BindingDirection::Source {
                continue;
            }
            if let Some(sinks) = sinks_by_fp.get(&b.fingerprint) {
                for sink in sinks {
                    // Skip self-edges where the same node is both source and sink
                    // (shouldn't happen, but be safe).
                    if sink.pipeline_id == b.pipeline_id && sink.node_id == b.node_id {
                        continue;
                    }
                    edges.push(LineageEdge {
                        upstream_pipeline_id: sink.pipeline_id.clone(),
                        upstream_node_id: sink.node_id.clone(),
                        downstream_pipeline_id: b.pipeline_id.clone(),
                        downstream_node_id: b.node_id.clone(),
                        fingerprint: b.fingerprint.clone(),
                        source: EdgeSource::Static,
                    });
                }
            }
        }

        edges
    }

    /// Add observed edges (from runtime execution history).
    pub fn add_observed_edges(&mut self, edges: Vec<LineageEdge>) {
        // Deduplicate: don't add an observed edge if a static edge already
        // covers the same upstream→downstream pipeline+node pair.
        let existing: HashSet<(String, String, String, String)> = self
            .edges
            .iter()
            .map(|e| {
                (
                    e.upstream_pipeline_id.to_string(),
                    e.upstream_node_id.clone(),
                    e.downstream_pipeline_id.to_string(),
                    e.downstream_node_id.clone(),
                )
            })
            .collect();

        for edge in edges {
            let key = (
                edge.upstream_pipeline_id.to_string(),
                edge.upstream_node_id.clone(),
                edge.downstream_pipeline_id.to_string(),
                edge.downstream_node_id.clone(),
            );
            if !existing.contains(&key) {
                self.edges.push(edge);
            }
        }
    }

    /// Return all edges where `pipeline_id` is the downstream consumer.
    pub fn upstream_of(&self, pipeline_id: &PipelineId) -> Vec<&LineageEdge> {
        self.edges
            .iter()
            .filter(|e| &e.downstream_pipeline_id == pipeline_id)
            .collect()
    }

    /// Return all edges where `pipeline_id` is the upstream producer.
    pub fn downstream_of(&self, pipeline_id: &PipelineId) -> Vec<&LineageEdge> {
        self.edges
            .iter()
            .filter(|e| &e.upstream_pipeline_id == pipeline_id)
            .collect()
    }

    /// Return all unique pipeline IDs referenced in the graph.
    pub fn pipeline_ids(&self) -> HashSet<PipelineId> {
        let mut ids = HashSet::new();
        for edge in &self.edges {
            ids.insert(edge.upstream_pipeline_id.clone());
            ids.insert(edge.downstream_pipeline_id.clone());
        }
        ids
    }

    /// Detect cycles in the cross-pipeline lineage graph.
    ///
    /// Returns a list of cycles, where each cycle is a list of pipeline IDs
    /// forming a loop. Returns an empty vec if the graph is acyclic.
    pub fn detect_cycles(&self) -> Vec<Vec<PipelineId>> {
        // Build adjacency: pipeline → set of downstream pipelines.
        let mut adj: HashMap<&PipelineId, HashSet<&PipelineId>> = HashMap::new();
        let mut all_ids: HashSet<&PipelineId> = HashSet::new();

        for edge in &self.edges {
            adj.entry(&edge.upstream_pipeline_id)
                .or_default()
                .insert(&edge.downstream_pipeline_id);
            all_ids.insert(&edge.upstream_pipeline_id);
            all_ids.insert(&edge.downstream_pipeline_id);
        }

        // Use Kahn's algorithm to find nodes remaining after removing all
        // acyclic nodes — the remaining nodes are in cycles.
        let mut in_degree: HashMap<&PipelineId, usize> = HashMap::new();
        for id in &all_ids {
            in_degree.entry(id).or_insert(0);
        }
        for targets in adj.values() {
            for t in targets {
                *in_degree.entry(t).or_insert(0) += 1;
            }
        }

        let mut queue: VecDeque<&PipelineId> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(id, _)| *id)
            .collect();

        let mut removed = HashSet::new();
        while let Some(node) = queue.pop_front() {
            removed.insert(node);
            if let Some(targets) = adj.get(node) {
                for t in targets {
                    if let Some(deg) = in_degree.get_mut(t) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(t);
                        }
                    }
                }
            }
        }

        // Remaining nodes are in cycles. Extract connected components.
        let cycle_nodes: HashSet<&PipelineId> = all_ids.difference(&removed).copied().collect();

        if cycle_nodes.is_empty() {
            return Vec::new();
        }

        // Walk from each unvisited cycle node to extract cycle paths.
        let mut visited = HashSet::new();
        let mut cycles = Vec::new();

        for &start in &cycle_nodes {
            if visited.contains(start) {
                continue;
            }
            // BFS to find the connected component within cycle nodes.
            let mut component = Vec::new();
            let mut bfs_queue = VecDeque::new();
            bfs_queue.push_back(start);
            visited.insert(start);

            while let Some(node) = bfs_queue.pop_front() {
                component.push(node.clone());
                if let Some(targets) = adj.get(node) {
                    for t in targets {
                        if cycle_nodes.contains(t) && !visited.contains(*t) {
                            visited.insert(*t);
                            bfs_queue.push_back(*t);
                        }
                    }
                }
            }

            if !component.is_empty() {
                cycles.push(component);
            }
        }

        cycles
    }

    /// Topological sort of pipeline IDs in the lineage graph.
    ///
    /// Returns `Ok(sorted_ids)` in execution order (upstream first), or
    /// `Err(cycle_ids)` if cycles prevent a valid ordering.
    pub fn topological_sort(&self) -> Result<Vec<PipelineId>, Vec<PipelineId>> {
        let mut adj: HashMap<&PipelineId, HashSet<&PipelineId>> = HashMap::new();
        let mut all_ids: HashSet<&PipelineId> = HashSet::new();

        for edge in &self.edges {
            adj.entry(&edge.upstream_pipeline_id)
                .or_default()
                .insert(&edge.downstream_pipeline_id);
            all_ids.insert(&edge.upstream_pipeline_id);
            all_ids.insert(&edge.downstream_pipeline_id);
        }

        let mut in_degree: HashMap<&PipelineId, usize> = HashMap::new();
        for id in &all_ids {
            in_degree.entry(id).or_insert(0);
        }
        for targets in adj.values() {
            for t in targets {
                *in_degree.entry(t).or_insert(0) += 1;
            }
        }

        let mut queue: VecDeque<&PipelineId> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(id, _)| *id)
            .collect();

        let mut order = Vec::new();
        while let Some(node) = queue.pop_front() {
            order.push(node.clone());
            if let Some(targets) = adj.get(node) {
                for t in targets {
                    if let Some(deg) = in_degree.get_mut(t) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(t);
                        }
                    }
                }
            }
        }

        if order.len() == all_ids.len() {
            Ok(order)
        } else {
            let remaining: Vec<PipelineId> = in_degree
                .iter()
                .filter(|(_, deg)| **deg > 0)
                .map(|(id, _)| (*id).clone())
                .collect();
            Err(remaining)
        }
    }

    /// Return all pipelines transitively upstream of the given pipeline.
    pub fn all_upstream(&self, pipeline_id: &PipelineId) -> HashSet<PipelineId> {
        let mut result = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(pipeline_id);

        while let Some(current) = queue.pop_front() {
            for edge in &self.edges {
                if &edge.downstream_pipeline_id == current
                    && !result.contains(&edge.upstream_pipeline_id)
                {
                    result.insert(edge.upstream_pipeline_id.clone());
                    queue.push_back(&edge.upstream_pipeline_id);
                }
            }
        }

        result
    }

    /// Return all pipelines transitively downstream of the given pipeline.
    pub fn all_downstream(&self, pipeline_id: &PipelineId) -> HashSet<PipelineId> {
        let mut result = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(pipeline_id);

        while let Some(current) = queue.pop_front() {
            for edge in &self.edges {
                if &edge.upstream_pipeline_id == current
                    && !result.contains(&edge.downstream_pipeline_id)
                {
                    result.insert(edge.downstream_pipeline_id.clone());
                    queue.push_back(&edge.downstream_pipeline_id);
                }
            }
        }

        result
    }

    /// Find dangling references: sources that read from resources no sink produces.
    pub fn dangling_sources(&self) -> Vec<&ResourceBinding> {
        let produced: HashSet<&ResourceFingerprint> = self
            .bindings
            .iter()
            .filter(|b| b.direction == BindingDirection::Sink)
            .map(|b| &b.fingerprint)
            .collect();

        self.bindings
            .iter()
            .filter(|b| {
                b.direction == BindingDirection::Source && !produced.contains(&b.fingerprint)
            })
            .collect()
    }

    /// Find orphaned outputs: sinks that write to resources no source consumes.
    pub fn orphaned_sinks(&self) -> Vec<&ResourceBinding> {
        let consumed: HashSet<&ResourceFingerprint> = self
            .bindings
            .iter()
            .filter(|b| b.direction == BindingDirection::Source)
            .map(|b| &b.fingerprint)
            .collect();

        self.bindings
            .iter()
            .filter(|b| b.direction == BindingDirection::Sink && !consumed.contains(&b.fingerprint))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::Edge;
    use crate::node::*;
    use std::collections::BTreeMap;

    // -- Test helpers -------------------------------------------------------

    fn source_node(id: &str, connector: &str, config: serde_json::Value) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.to_string(),
            kind: NodeKind::Source(SourceConfig {
                connector: connector.into(),
                config,
                cache_row_limit: None,
            }),
            position: Position::default(),
            pinned_position: false,
            snippet_parent: None,
            snippet_name: None,
        }
    }

    fn sink_node(id: &str, connector: &str, config: serde_json::Value) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.to_string(),
            kind: NodeKind::Sink(SinkConfig {
                connector: connector.into(),
                config,
                materialization: None,
            }),
            position: Position::default(),
            pinned_position: false,
            snippet_parent: None,
            snippet_name: None,
        }
    }

    fn transform_node(id: &str) -> Node {
        Node {
            id: NodeId::new(id),
            name: id.to_string(),
            kind: NodeKind::Transform(TransformConfig {
                mode: TransformMode::Sql,
                code: "SELECT * FROM upstream".into(),
                code_path: None,
                materialized: false,
                cache_row_limit: None,
                lineage_annotations: None,
            }),
            position: Position::default(),
            pinned_position: false,
            snippet_parent: None,
            snippet_name: None,
        }
    }

    fn test_pipeline(name: &str, nodes: Vec<Node>, edges: Vec<Edge>) -> Pipeline {
        Pipeline {
            name: name.to_string(),
            version: 1,
            default_environment: "dev".into(),
            variables: BTreeMap::new(),
            environment_overrides: BTreeMap::new(),
            sample_config: None,
            cache_row_limit: None,
            code_dir: None,
            udfs_dir: None,
            snippets_dir: None,
            snippet: None,
            params: BTreeMap::new(),
            outputs: Vec::new(),
            nodes,
            edges,
        }
    }

    /// Simple test fingerprint function that extracts "resource" from config.
    fn test_fingerprint(
        _connector: &str,
        config: &serde_json::Value,
    ) -> Option<ResourceFingerprint> {
        config
            .get("resource")
            .and_then(|v| v.as_str())
            .map(|s| ResourceFingerprint::new(s.to_string()))
    }

    fn pid(n: u128) -> PipelineId {
        PipelineId(uuid::Uuid::from_u128(n))
    }

    // -- ResourceFingerprint tests ------------------------------------------

    #[test]
    fn fingerprint_display() {
        let fp = ResourceFingerprint::new("postgres://localhost:5432/db/public.users");
        assert_eq!(fp.to_string(), "postgres://localhost:5432/db/public.users");
    }

    #[test]
    fn fingerprint_equality() {
        let a = ResourceFingerprint::new("file:///data/orders.csv");
        let b = ResourceFingerprint::new("file:///data/orders.csv");
        let c = ResourceFingerprint::new("file:///data/other.csv");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn fingerprint_serde_roundtrip() {
        let fp = ResourceFingerprint::new("s3://bucket/path/");
        let json = serde_json::to_string(&fp).unwrap();
        assert_eq!(json, r#""s3://bucket/path/""#);
        let fp2: ResourceFingerprint = serde_json::from_str(&json).unwrap();
        assert_eq!(fp, fp2);
    }

    #[test]
    fn binding_direction_display() {
        assert_eq!(BindingDirection::Source.to_string(), "source");
        assert_eq!(BindingDirection::Sink.to_string(), "sink");
    }

    // -- Static edge derivation tests ---------------------------------------

    #[test]
    fn static_edge_from_matching_sink_source() {
        // Pipeline A writes to "table_orders", Pipeline B reads from "table_orders".
        let pipeline_a = test_pipeline(
            "producer",
            vec![
                source_node("src", "csv", serde_json::json!({"resource": "file_raw"})),
                transform_node("xform"),
                sink_node(
                    "sink",
                    "pg",
                    serde_json::json!({"resource": "table_orders"}),
                ),
            ],
            vec![Edge::new("src", "xform"), Edge::new("xform", "sink")],
        );
        let pipeline_b = test_pipeline(
            "consumer",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "table_orders"})),
                transform_node("xform"),
                sink_node("sink", "stdout", serde_json::json!({})),
            ],
            vec![Edge::new("src", "xform"), Edge::new("xform", "sink")],
        );

        let id_a = pid(1);
        let id_b = pid(2);
        let graph = LineageGraph::from_pipelines(
            &[(id_a.clone(), &pipeline_a), (id_b.clone(), &pipeline_b)],
            test_fingerprint,
        );

        assert_eq!(graph.edges.len(), 1);
        let edge = &graph.edges[0];
        assert_eq!(edge.upstream_pipeline_id, id_a);
        assert_eq!(edge.upstream_node_id, "sink");
        assert_eq!(edge.downstream_pipeline_id, id_b);
        assert_eq!(edge.downstream_node_id, "src");
        assert_eq!(edge.source, EdgeSource::Static);
    }

    #[test]
    fn no_edge_when_resources_differ() {
        let pipeline_a = test_pipeline(
            "producer",
            vec![
                source_node("src", "csv", serde_json::json!({"resource": "raw_a"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "table_a"})),
            ],
            vec![Edge::new("src", "sink")],
        );
        let pipeline_b = test_pipeline(
            "consumer",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "table_b"})),
                sink_node("sink", "stdout", serde_json::json!({})),
            ],
            vec![Edge::new("src", "sink")],
        );

        let graph = LineageGraph::from_pipelines(
            &[(pid(1), &pipeline_a), (pid(2), &pipeline_b)],
            test_fingerprint,
        );

        assert!(graph.edges.is_empty());
    }

    #[test]
    fn multiple_pipelines_sharing_resources() {
        // A → table_x → B, A → table_x → C (fan-out).
        let pipeline_a = test_pipeline(
            "producer",
            vec![
                source_node("src", "csv", serde_json::json!({"resource": "raw"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "table_x"})),
            ],
            vec![Edge::new("src", "sink")],
        );
        let pipeline_b = test_pipeline(
            "consumer_b",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "table_x"})),
                sink_node("sink", "stdout", serde_json::json!({})),
            ],
            vec![Edge::new("src", "sink")],
        );
        let pipeline_c = test_pipeline(
            "consumer_c",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "table_x"})),
                sink_node("sink", "stdout", serde_json::json!({})),
            ],
            vec![Edge::new("src", "sink")],
        );

        let graph = LineageGraph::from_pipelines(
            &[
                (pid(1), &pipeline_a),
                (pid(2), &pipeline_b),
                (pid(3), &pipeline_c),
            ],
            test_fingerprint,
        );

        assert_eq!(graph.edges.len(), 2);
        // Both edges should have upstream = pipeline A.
        assert!(graph.edges.iter().all(|e| e.upstream_pipeline_id == pid(1)));
    }

    // -- Upstream/downstream queries ----------------------------------------

    #[test]
    fn upstream_and_downstream_queries() {
        // A → B → C (linear chain).
        let pa = test_pipeline(
            "a",
            vec![
                source_node("src", "csv", serde_json::json!({"resource": "raw"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "table_1"})),
            ],
            vec![Edge::new("src", "sink")],
        );
        let pb = test_pipeline(
            "b",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "table_1"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "table_2"})),
            ],
            vec![Edge::new("src", "sink")],
        );
        let pc = test_pipeline(
            "c",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "table_2"})),
                sink_node("sink", "stdout", serde_json::json!({})),
            ],
            vec![Edge::new("src", "sink")],
        );

        let id_a = pid(1);
        let id_b = pid(2);
        let id_c = pid(3);
        let graph = LineageGraph::from_pipelines(
            &[
                (id_a.clone(), &pa),
                (id_b.clone(), &pb),
                (id_c.clone(), &pc),
            ],
            test_fingerprint,
        );

        assert_eq!(graph.edges.len(), 2);

        // Direct upstream/downstream.
        assert_eq!(graph.upstream_of(&id_b).len(), 1);
        assert_eq!(graph.downstream_of(&id_b).len(), 1);
        assert!(graph.upstream_of(&id_a).is_empty());
        assert!(graph.downstream_of(&id_c).is_empty());

        // Transitive.
        let all_up = graph.all_upstream(&id_c);
        assert_eq!(all_up.len(), 2);
        assert!(all_up.contains(&id_a));
        assert!(all_up.contains(&id_b));

        let all_down = graph.all_downstream(&id_a);
        assert_eq!(all_down.len(), 2);
        assert!(all_down.contains(&id_b));
        assert!(all_down.contains(&id_c));
    }

    // -- Cycle detection ----------------------------------------------------

    #[test]
    fn no_cycles_in_linear_chain() {
        let pa = test_pipeline(
            "a",
            vec![
                source_node("src", "csv", serde_json::json!({"resource": "raw"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "t1"})),
            ],
            vec![Edge::new("src", "sink")],
        );
        let pb = test_pipeline(
            "b",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "t1"})),
                sink_node("sink", "stdout", serde_json::json!({})),
            ],
            vec![Edge::new("src", "sink")],
        );

        let graph = LineageGraph::from_pipelines(&[(pid(1), &pa), (pid(2), &pb)], test_fingerprint);

        assert!(graph.detect_cycles().is_empty());
    }

    #[test]
    fn detects_two_pipeline_cycle() {
        // A writes t1, reads t2. B writes t2, reads t1. → cycle.
        let pa = test_pipeline(
            "a",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "t2"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "t1"})),
            ],
            vec![Edge::new("src", "sink")],
        );
        let pb = test_pipeline(
            "b",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "t1"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "t2"})),
            ],
            vec![Edge::new("src", "sink")],
        );

        let graph = LineageGraph::from_pipelines(&[(pid(1), &pa), (pid(2), &pb)], test_fingerprint);

        let cycles = graph.detect_cycles();
        assert_eq!(cycles.len(), 1);
        assert_eq!(cycles[0].len(), 2);
    }

    // -- Topological sort ---------------------------------------------------

    #[test]
    fn topological_sort_linear() {
        let pa = test_pipeline(
            "a",
            vec![
                source_node("src", "csv", serde_json::json!({"resource": "raw"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "t1"})),
            ],
            vec![Edge::new("src", "sink")],
        );
        let pb = test_pipeline(
            "b",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "t1"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "t2"})),
            ],
            vec![Edge::new("src", "sink")],
        );
        let pc = test_pipeline(
            "c",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "t2"})),
                sink_node("sink", "stdout", serde_json::json!({})),
            ],
            vec![Edge::new("src", "sink")],
        );

        let id_a = pid(1);
        let id_b = pid(2);
        let id_c = pid(3);
        let graph = LineageGraph::from_pipelines(
            &[
                (id_a.clone(), &pa),
                (id_b.clone(), &pb),
                (id_c.clone(), &pc),
            ],
            test_fingerprint,
        );

        let order = graph.topological_sort().unwrap();
        let pos = |id: &PipelineId| order.iter().position(|x| x == id).unwrap();
        assert!(pos(&id_a) < pos(&id_b));
        assert!(pos(&id_b) < pos(&id_c));
    }

    #[test]
    fn topological_sort_fails_on_cycle() {
        let pa = test_pipeline(
            "a",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "t2"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "t1"})),
            ],
            vec![Edge::new("src", "sink")],
        );
        let pb = test_pipeline(
            "b",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "t1"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "t2"})),
            ],
            vec![Edge::new("src", "sink")],
        );

        let graph = LineageGraph::from_pipelines(&[(pid(1), &pa), (pid(2), &pb)], test_fingerprint);

        assert!(graph.topological_sort().is_err());
    }

    // -- Dangling sources and orphaned sinks --------------------------------

    #[test]
    fn dangling_sources_detected() {
        // Pipeline reads from "external_table" that nobody writes to.
        let p = test_pipeline(
            "consumer",
            vec![
                source_node(
                    "src",
                    "pg",
                    serde_json::json!({"resource": "external_table"}),
                ),
                sink_node("sink", "stdout", serde_json::json!({})),
            ],
            vec![Edge::new("src", "sink")],
        );

        let graph = LineageGraph::from_pipelines(&[(pid(1), &p)], test_fingerprint);

        let dangling = graph.dangling_sources();
        assert_eq!(dangling.len(), 1);
        assert_eq!(
            dangling[0].fingerprint,
            ResourceFingerprint::new("external_table")
        );
    }

    #[test]
    fn orphaned_sinks_detected() {
        // Pipeline writes to "unused_table" that nobody reads from.
        let p = test_pipeline(
            "producer",
            vec![
                source_node("src", "csv", serde_json::json!({"resource": "raw"})),
                sink_node(
                    "sink",
                    "pg",
                    serde_json::json!({"resource": "unused_table"}),
                ),
            ],
            vec![Edge::new("src", "sink")],
        );

        let graph = LineageGraph::from_pipelines(&[(pid(1), &p)], test_fingerprint);

        let orphaned = graph.orphaned_sinks();
        // Both "raw" (source but no sink writes it) is N/A here since it's a source.
        // "unused_table" is a sink with no consumer.
        assert_eq!(orphaned.len(), 1);
        assert_eq!(
            orphaned[0].fingerprint,
            ResourceFingerprint::new("unused_table")
        );
    }

    // -- Self-referential pipeline (incremental) ----------------------------

    #[test]
    fn self_referential_pipeline_creates_self_edge() {
        // Pipeline reads and writes the same table (incremental pattern).
        let p = test_pipeline(
            "incremental",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "table_inc"})),
                transform_node("xform"),
                sink_node("sink", "pg", serde_json::json!({"resource": "table_inc"})),
            ],
            vec![Edge::new("src", "xform"), Edge::new("xform", "sink")],
        );

        let id = pid(1);
        let graph = LineageGraph::from_pipelines(&[(id.clone(), &p)], test_fingerprint);

        // Self-edge: the pipeline depends on itself.
        assert_eq!(graph.edges.len(), 1);
        let edge = &graph.edges[0];
        assert_eq!(edge.upstream_pipeline_id, id);
        assert_eq!(edge.downstream_pipeline_id, id);
    }

    // -- Observed edge deduplication ----------------------------------------

    #[test]
    fn observed_edges_deduplicated_against_static() {
        let pa = test_pipeline(
            "a",
            vec![
                source_node("src", "csv", serde_json::json!({"resource": "raw"})),
                sink_node("sink", "pg", serde_json::json!({"resource": "t1"})),
            ],
            vec![Edge::new("src", "sink")],
        );
        let pb = test_pipeline(
            "b",
            vec![
                source_node("src", "pg", serde_json::json!({"resource": "t1"})),
                sink_node("sink", "stdout", serde_json::json!({})),
            ],
            vec![Edge::new("src", "sink")],
        );

        let id_a = pid(1);
        let id_b = pid(2);
        let mut graph = LineageGraph::from_pipelines(
            &[(id_a.clone(), &pa), (id_b.clone(), &pb)],
            test_fingerprint,
        );
        assert_eq!(graph.edges.len(), 1);

        // Try to add the same edge as observed — should be deduplicated.
        graph.add_observed_edges(vec![LineageEdge {
            upstream_pipeline_id: id_a.clone(),
            upstream_node_id: "sink".into(),
            downstream_pipeline_id: id_b.clone(),
            downstream_node_id: "src".into(),
            fingerprint: ResourceFingerprint::new("t1"),
            source: EdgeSource::Observed,
        }]);

        assert_eq!(graph.edges.len(), 1); // Still just 1.

        // Add a genuinely new observed edge.
        graph.add_observed_edges(vec![LineageEdge {
            upstream_pipeline_id: id_a,
            upstream_node_id: "sink".into(),
            downstream_pipeline_id: pid(3),
            downstream_node_id: "src".into(),
            fingerprint: ResourceFingerprint::new("t1"),
            source: EdgeSource::Observed,
        }]);

        assert_eq!(graph.edges.len(), 2);
    }
}
