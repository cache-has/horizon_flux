// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Column-level lineage types.
//!
//! These types describe the provenance of individual columns through a pipeline:
//! which input columns contribute to each output column, through what kind of
//! relationship, and at what confidence level.
//!
//! The types live in `armillary-engine` (no DataFusion dependency) so they can be
//! used by the catalog, API, and frontend layers without pulling in the
//! execution engine.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

use crate::lineage::{BindingDirection, ResourceFingerprint};
use crate::node::NodeId;
use crate::pipeline_store::PipelineId;

// ---------------------------------------------------------------------------
// Column identity
// ---------------------------------------------------------------------------

/// A reference to a specific column in the lineage graph.
///
/// - **Internal columns** live inside a pipeline (produced by a transform).
/// - **External columns** live at pipeline boundaries (source/sink), identified
///   by the resource they belong to.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Pipeline that contains this column. Always set for internal columns;
    /// set for external columns when the owning pipeline is known.
    pub pipeline_id: Option<PipelineId>,
    /// Node that produces this column. `None` for external columns resolved
    /// only by resource fingerprint.
    pub node_id: Option<NodeId>,
    /// Column name.
    pub column: String,
    /// Resource fingerprint for external columns (source/sink boundaries).
    pub resource_fingerprint: Option<ResourceFingerprint>,
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref fp) = self.resource_fingerprint {
            write!(f, "{}:{}", fp, self.column)
        } else if let Some(ref node) = self.node_id {
            write!(f, "{}:{}", node, self.column)
        } else {
            write!(f, "{}", self.column)
        }
    }
}

// ---------------------------------------------------------------------------
// Relationship kinds
// ---------------------------------------------------------------------------

/// The kind of relationship between an upstream and downstream column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipKind {
    /// Value copied verbatim (projection passthrough, rename).
    Direct,
    /// Value computed from one or more input columns via an expression.
    Derived,
    /// Value is a type cast of an input column.
    Cast,
    /// Input column gates rows via a WHERE / HAVING clause.
    Filter,
    /// Input column is part of a join condition.
    JoinKey,
    /// Input column flows through a join without participating in the key.
    JoinPassthrough,
    /// Input column is a GROUP BY key.
    GroupBy,
    /// Input column is consumed by an aggregate function.
    AggregateInput,
    /// Input column is a window PARTITION BY key.
    WindowPartition,
    /// Input column is a window ORDER BY key.
    WindowOrder,
    /// Input column is the expression a window function operates on.
    WindowInput,
    /// Lineage could not be precisely determined (eager Python, etc.).
    Opaque,
}

impl fmt::Display for RelationshipKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Direct => "direct",
            Self::Derived => "derived",
            Self::Cast => "cast",
            Self::Filter => "filter",
            Self::JoinKey => "join_key",
            Self::JoinPassthrough => "join_passthrough",
            Self::GroupBy => "group_by",
            Self::AggregateInput => "aggregate_input",
            Self::WindowPartition => "window_partition",
            Self::WindowOrder => "window_order",
            Self::WindowInput => "window_input",
            Self::Opaque => "opaque",
        };
        f.write_str(s)
    }
}

// ---------------------------------------------------------------------------
// Confidence levels
// ---------------------------------------------------------------------------

/// How confident we are in a lineage edge's correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// Derived from walking the DataFusion logical plan — ground truth.
    Exact,
    /// Derived from walking a Polars LazyFrame plan.
    LazyFrame,
    /// User-provided annotation.
    Annotation,
    /// Conservative fallback (every output ↔ every input).
    Opaque,
}

impl fmt::Display for Confidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Exact => "exact",
            Self::LazyFrame => "lazyframe",
            Self::Annotation => "annotation",
            Self::Opaque => "opaque",
        };
        f.write_str(s)
    }
}

// ---------------------------------------------------------------------------
// Column edge
// ---------------------------------------------------------------------------

/// A single lineage edge connecting an upstream column to a downstream column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnEdge {
    /// The upstream (input / source) column.
    pub upstream_column: String,
    /// The upstream node that produces the column.
    pub upstream_node: Option<NodeId>,
    /// Resource fingerprint if the upstream is an external boundary.
    pub upstream_resource: Option<ResourceFingerprint>,
    /// The downstream (output) column.
    pub downstream_column: String,
    /// The downstream node that produces the column.
    pub downstream_node: Option<NodeId>,
    /// Resource fingerprint if the downstream is an external boundary.
    pub downstream_resource: Option<ResourceFingerprint>,
    /// The kind of relationship.
    pub relationship: RelationshipKind,
    /// Human-readable expression text (for tooltips).
    pub expression_text: Option<String>,
    /// How confident we are in this edge.
    pub confidence: Confidence,
}

// ---------------------------------------------------------------------------
// Per-node lineage result
// ---------------------------------------------------------------------------

/// The complete column lineage for a single transform node.
///
/// Maps each output column name to the set of edges describing where its
/// value (or influence) comes from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeColumnLineage {
    /// The node these edges belong to.
    pub node_id: NodeId,
    /// All lineage edges for this node's output columns.
    pub edges: Vec<ColumnEdge>,
    /// Validation warnings (e.g. annotation references a column not in schema).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl NodeColumnLineage {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            edges: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Return edges for a specific downstream (output) column.
    pub fn edges_for_column(&self, column: &str) -> Vec<&ColumnEdge> {
        self.edges
            .iter()
            .filter(|e| e.downstream_column == column)
            .collect()
    }

    /// Return all distinct output column names.
    pub fn output_columns(&self) -> Vec<&str> {
        let mut cols: Vec<&str> = self
            .edges
            .iter()
            .map(|e| e.downstream_column.as_str())
            .collect();
        cols.sort_unstable();
        cols.dedup();
        cols
    }
}

// ---------------------------------------------------------------------------
// User-provided lineage annotations (planning doc 35c)
// ---------------------------------------------------------------------------

/// A single annotated lineage edge provided by the user.
///
/// Used in `TransformConfig::lineage_annotations` and the
/// `@column_lineage(outputs=...)` Python decorator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageAnnotationEdge {
    /// The upstream (input) column that contributes to the output.
    pub upstream_column: String,
    /// The downstream (output) column being produced.
    pub downstream_column: String,
    /// The relationship kind (defaults to "derived" when omitted).
    #[serde(default = "default_derived")]
    pub relationship: RelationshipKind,
}

fn default_derived() -> RelationshipKind {
    RelationshipKind::Derived
}

/// Per-node lineage annotations provided by the user.
///
/// These serve as an escape hatch for eager Python code where lineage
/// cannot be derived automatically from a LazyFrame plan.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageAnnotations {
    /// Annotated edges. Each edge declares that a specific upstream column
    /// contributes to a specific downstream column.
    #[serde(default)]
    pub edges: Vec<LineageAnnotationEdge>,
}

impl LineageAnnotations {
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Cross-pipeline column lineage (planning doc 35 — cross-pipeline derivation)
// ---------------------------------------------------------------------------

/// Canonicalize a column name for cross-pipeline matching.
///
/// - Strips surrounding double quotes (SQL quoted identifiers).
/// - Lowercases unquoted identifiers (SQL identifiers are case-insensitive
///   when unquoted).
/// - Trims leading/trailing whitespace.
///
/// Quoted identifiers (`"Name"`) are preserved in their original case after
/// quote removal, since SQL standards treat quoted identifiers as
/// case-sensitive. However, for pragmatic cross-system matching (e.g.
/// Postgres → BigQuery), callers may choose to lowercase the result.
pub fn canonicalize_column(name: &str) -> String {
    let trimmed = name.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        // Quoted identifier: strip quotes, preserve case.
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        // Unquoted: fold to lowercase.
        trimmed.to_lowercase()
    }
}

/// A column at a pipeline boundary identified by a resource fingerprint.
///
/// Used as input to [`derive_cross_pipeline_column_lineage`] to match sink
/// columns in one pipeline to source columns in another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryColumn {
    pub pipeline_id: PipelineId,
    pub node_id: NodeId,
    pub column: String,
    pub fingerprint: ResourceFingerprint,
    pub direction: BindingDirection,
}

/// A cross-pipeline column lineage edge connecting a sink column in one
/// pipeline to a source column in another through a shared resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPipelineColumnEdge {
    /// The pipeline that writes the column (sink side).
    pub upstream_pipeline_id: PipelineId,
    /// The sink node that writes the column.
    pub upstream_node_id: NodeId,
    /// The pipeline that reads the column (source side).
    pub downstream_pipeline_id: PipelineId,
    /// The source node that reads the column.
    pub downstream_node_id: NodeId,
    /// The column name (from the upstream/sink side).
    pub column: String,
    /// The shared resource fingerprint connecting them.
    pub fingerprint: ResourceFingerprint,
}

/// Whether a one-sided column is on the sink or source side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OneSidedKind {
    /// Column exists in the sink but no downstream source reads it.
    SinkOnly,
    /// Column exists in the source but the upstream sink doesn't write it.
    SourceOnly,
}

impl fmt::Display for OneSidedKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SinkOnly => f.write_str("sink_only"),
            Self::SourceOnly => f.write_str("source_only"),
        }
    }
}

/// A column at a pipeline boundary with no corresponding match on the other
/// side. Indicates potential schema drift between connected pipelines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OneSidedColumn {
    pub pipeline_id: PipelineId,
    pub node_id: NodeId,
    pub column: String,
    pub fingerprint: ResourceFingerprint,
    pub side: OneSidedKind,
}

/// Result of cross-pipeline column lineage derivation.
#[derive(Debug, Clone, Default)]
pub struct CrossPipelineColumnLineage {
    /// Matched column edges across pipelines.
    pub edges: Vec<CrossPipelineColumnEdge>,
    /// Columns that exist on only one side of a pipeline boundary.
    pub one_sided: Vec<OneSidedColumn>,
}

/// Derive cross-pipeline column-level lineage by matching sink boundary
/// columns in one pipeline to source boundary columns in another via shared
/// resource fingerprints.
///
/// Columns are matched using [`canonicalize_column`] for case-insensitive,
/// quote-stripped comparison. Unmatched columns are reported as
/// [`OneSidedColumn`] warnings for schema drift detection.
pub fn derive_cross_pipeline_column_lineage(
    boundary_columns: &[BoundaryColumn],
) -> CrossPipelineColumnLineage {
    // Index boundary columns by (fingerprint, canonical_column_name).
    let mut sinks: HashMap<(String, String), Vec<&BoundaryColumn>> = HashMap::new();
    let mut sources: HashMap<(String, String), Vec<&BoundaryColumn>> = HashMap::new();

    for bc in boundary_columns {
        let canonical = canonicalize_column(&bc.column);
        let key = (bc.fingerprint.0.clone(), canonical);
        match bc.direction {
            BindingDirection::Sink => sinks.entry(key).or_default().push(bc),
            BindingDirection::Source => sources.entry(key).or_default().push(bc),
        }
    }

    let mut result = CrossPipelineColumnLineage::default();

    // Matched edges: sink writes → source reads through the same resource.
    for (key, sink_cols) in &sinks {
        if let Some(source_cols) = sources.get(key) {
            for sink in sink_cols {
                for source in source_cols {
                    // Skip self-references within the same pipeline + node.
                    if sink.pipeline_id == source.pipeline_id && sink.node_id == source.node_id {
                        continue;
                    }
                    result.edges.push(CrossPipelineColumnEdge {
                        upstream_pipeline_id: sink.pipeline_id.clone(),
                        upstream_node_id: sink.node_id.clone(),
                        downstream_pipeline_id: source.pipeline_id.clone(),
                        downstream_node_id: source.node_id.clone(),
                        column: sink.column.clone(),
                        fingerprint: sink.fingerprint.clone(),
                    });
                }
            }
        } else {
            // Sink-only: no downstream source reads this column.
            for sink in sink_cols {
                result.one_sided.push(OneSidedColumn {
                    pipeline_id: sink.pipeline_id.clone(),
                    node_id: sink.node_id.clone(),
                    column: sink.column.clone(),
                    fingerprint: sink.fingerprint.clone(),
                    side: OneSidedKind::SinkOnly,
                });
            }
        }
    }

    // Source-only: no upstream sink writes this column.
    for (key, source_cols) in &sources {
        if !sinks.contains_key(key) {
            for source in source_cols {
                result.one_sided.push(OneSidedColumn {
                    pipeline_id: source.pipeline_id.clone(),
                    node_id: source.node_id.clone(),
                    column: source.column.clone(),
                    fingerprint: source.fingerprint.clone(),
                    side: OneSidedKind::SourceOnly,
                });
            }
        }
    }

    // Sort for deterministic output.
    result.edges.sort_by(|a, b| {
        a.fingerprint
            .0
            .cmp(&b.fingerprint.0)
            .then_with(|| a.column.cmp(&b.column))
            .then_with(|| {
                a.upstream_pipeline_id
                    .to_string()
                    .cmp(&b.upstream_pipeline_id.to_string())
            })
            .then_with(|| {
                a.downstream_pipeline_id
                    .to_string()
                    .cmp(&b.downstream_pipeline_id.to_string())
            })
    });
    result.one_sided.sort_by(|a, b| {
        a.fingerprint
            .0
            .cmp(&b.fingerprint.0)
            .then_with(|| a.column.cmp(&b.column))
            .then_with(|| a.side.to_string().cmp(&b.side.to_string()))
    });

    result
}

// ---------------------------------------------------------------------------
// Column lineage query engine (planning doc 35 — query engine)
// ---------------------------------------------------------------------------

/// A key that uniquely identifies a column in the lineage graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnKey {
    pub pipeline_id: PipelineId,
    pub node_id: NodeId,
    pub column: String,
}

impl fmt::Display for ColumnKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.pipeline_id, self.node_id, self.column)
    }
}

/// Options for controlling lineage trace queries.
#[derive(Debug, Clone)]
pub struct TraceOptions {
    /// Maximum traversal depth (default: 10).
    pub max_depth: usize,
    /// Only follow edges with these relationship kinds. Empty = all.
    pub relationship_filter: HashSet<RelationshipKind>,
    /// Only follow edges with these confidence levels. Empty = all.
    pub confidence_filter: HashSet<Confidence>,
}

impl Default for TraceOptions {
    fn default() -> Self {
        Self {
            max_depth: 10,
            relationship_filter: HashSet::new(),
            confidence_filter: HashSet::new(),
        }
    }
}

/// A single edge in a lineage trace result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEdge {
    pub upstream: ColumnKey,
    pub downstream: ColumnKey,
    pub relationship: RelationshipKind,
    pub confidence: Confidence,
    pub expression_text: Option<String>,
    /// Depth at which this edge was found (1 = direct neighbor).
    pub depth: usize,
}

/// Result of a lineage trace query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceResult {
    /// The starting column.
    pub origin: ColumnKey,
    /// All edges found during traversal, ordered by depth.
    pub edges: Vec<TraceEdge>,
    /// Whether the trace was truncated by the depth limit.
    pub truncated: bool,
}

/// Internal edge used in the adjacency lists.
#[derive(Debug, Clone)]
struct InternalEdge {
    target: ColumnKey,
    relationship: RelationshipKind,
    confidence: Confidence,
    expression_text: Option<String>,
}

/// In-memory column lineage graph supporting bounded upstream/downstream
/// traversal with relationship-kind and confidence filters.
///
/// Constructed from intra-pipeline edges and cross-pipeline edges, then
/// queried via [`upstream_trace`](Self::upstream_trace) and
/// [`downstream_trace`](Self::downstream_trace).
pub struct ColumnLineageGraph {
    /// downstream key → upstream neighbors
    upstream_adj: HashMap<ColumnKey, Vec<InternalEdge>>,
    /// upstream key → downstream neighbors
    downstream_adj: HashMap<ColumnKey, Vec<InternalEdge>>,
    /// (fingerprint, canonical_column) → column keys at that boundary
    fingerprint_index: HashMap<(String, String), Vec<ColumnKey>>,
}

impl ColumnLineageGraph {
    /// Build a traversable graph from intra-pipeline edges and cross-pipeline
    /// edges.
    ///
    /// `pipeline_edges` provides each pipeline's `ColumnEdge` set.
    /// `cross_pipeline_edges` links sink columns in one pipeline to source
    /// columns in another.
    pub fn new(
        pipeline_edges: &[(PipelineId, &[ColumnEdge])],
        cross_pipeline_edges: &[CrossPipelineColumnEdge],
    ) -> Self {
        let mut upstream_adj: HashMap<ColumnKey, Vec<InternalEdge>> = HashMap::new();
        let mut downstream_adj: HashMap<ColumnKey, Vec<InternalEdge>> = HashMap::new();
        let mut fingerprint_index: HashMap<(String, String), Vec<ColumnKey>> = HashMap::new();

        // Index intra-pipeline edges.
        for (pipeline_id, edges) in pipeline_edges {
            for edge in *edges {
                let downstream_node = edge
                    .downstream_node
                    .clone()
                    .unwrap_or_else(|| NodeId::new("__unknown__"));
                let upstream_node = edge
                    .upstream_node
                    .clone()
                    .unwrap_or_else(|| NodeId::new("__unknown__"));

                let downstream_key = ColumnKey {
                    pipeline_id: pipeline_id.clone(),
                    node_id: downstream_node,
                    column: edge.downstream_column.clone(),
                };
                let upstream_key = ColumnKey {
                    pipeline_id: pipeline_id.clone(),
                    node_id: upstream_node,
                    column: edge.upstream_column.clone(),
                };

                // Build fingerprint index for boundary columns.
                if let Some(ref fp) = edge.downstream_resource {
                    let canonical = canonicalize_column(&edge.downstream_column);
                    fingerprint_index
                        .entry((fp.0.clone(), canonical))
                        .or_default()
                        .push(downstream_key.clone());
                }
                if let Some(ref fp) = edge.upstream_resource {
                    let canonical = canonicalize_column(&edge.upstream_column);
                    fingerprint_index
                        .entry((fp.0.clone(), canonical))
                        .or_default()
                        .push(upstream_key.clone());
                }

                // upstream adjacency: downstream → upstream
                upstream_adj
                    .entry(downstream_key.clone())
                    .or_default()
                    .push(InternalEdge {
                        target: upstream_key.clone(),
                        relationship: edge.relationship,
                        confidence: edge.confidence,
                        expression_text: edge.expression_text.clone(),
                    });

                // downstream adjacency: upstream → downstream
                downstream_adj
                    .entry(upstream_key)
                    .or_default()
                    .push(InternalEdge {
                        target: downstream_key,
                        relationship: edge.relationship,
                        confidence: edge.confidence,
                        expression_text: edge.expression_text.clone(),
                    });
            }
        }

        // Index cross-pipeline edges as Direct/Exact connections.
        for cp in cross_pipeline_edges {
            let upstream_key = ColumnKey {
                pipeline_id: cp.upstream_pipeline_id.clone(),
                node_id: cp.upstream_node_id.clone(),
                column: cp.column.clone(),
            };
            let downstream_key = ColumnKey {
                pipeline_id: cp.downstream_pipeline_id.clone(),
                node_id: cp.downstream_node_id.clone(),
                column: cp.column.clone(),
            };

            upstream_adj
                .entry(downstream_key.clone())
                .or_default()
                .push(InternalEdge {
                    target: upstream_key.clone(),
                    relationship: RelationshipKind::Direct,
                    confidence: Confidence::Exact,
                    expression_text: None,
                });
            downstream_adj
                .entry(upstream_key)
                .or_default()
                .push(InternalEdge {
                    target: downstream_key,
                    relationship: RelationshipKind::Direct,
                    confidence: Confidence::Exact,
                    expression_text: None,
                });
        }

        // Deduplicate fingerprint index entries.
        for entries in fingerprint_index.values_mut() {
            entries.sort_by(|a, b| {
                a.pipeline_id
                    .to_string()
                    .cmp(&b.pipeline_id.to_string())
                    .then_with(|| a.node_id.0.cmp(&b.node_id.0))
                    .then_with(|| a.column.cmp(&b.column))
            });
            entries.dedup();
        }

        Self {
            upstream_adj,
            downstream_adj,
            fingerprint_index,
        }
    }

    /// Trace all upstream (input) columns that contribute to the given column,
    /// up to `opts.max_depth` hops.
    pub fn upstream_trace(&self, key: &ColumnKey, opts: &TraceOptions) -> TraceResult {
        self.trace(key, &self.upstream_adj, opts, true)
    }

    /// Trace all downstream (output) columns that depend on the given column,
    /// up to `opts.max_depth` hops.
    pub fn downstream_trace(&self, key: &ColumnKey, opts: &TraceOptions) -> TraceResult {
        self.trace(key, &self.downstream_adj, opts, false)
    }

    /// Resolve a `(resource_fingerprint, column_name)` pair to all matching
    /// column keys in the graph. Used by API endpoints that identify columns
    /// by their external resource identity.
    pub fn resolve_by_fingerprint(
        &self,
        fingerprint: &ResourceFingerprint,
        column: &str,
    ) -> Vec<&ColumnKey> {
        let canonical = canonicalize_column(column);
        self.fingerprint_index
            .get(&(fingerprint.0.clone(), canonical))
            .map(|keys| keys.iter().collect())
            .unwrap_or_default()
    }

    /// Return all distinct column keys present in the graph.
    pub fn all_columns(&self) -> Vec<&ColumnKey> {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for key in self.upstream_adj.keys().chain(self.downstream_adj.keys()) {
            if seen.insert(key) {
                result.push(key);
            }
        }
        result
    }

    /// Shared BFS implementation for upstream and downstream traces.
    fn trace(
        &self,
        start: &ColumnKey,
        adj: &HashMap<ColumnKey, Vec<InternalEdge>>,
        opts: &TraceOptions,
        is_upstream: bool,
    ) -> TraceResult {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut result_edges = Vec::new();
        let mut truncated = false;

        visited.insert(start.clone());
        queue.push_back((start.clone(), 0_usize));

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= opts.max_depth {
                truncated = true;
                continue;
            }

            if let Some(neighbors) = adj.get(&current) {
                for edge in neighbors {
                    // Apply relationship filter.
                    if !opts.relationship_filter.is_empty()
                        && !opts.relationship_filter.contains(&edge.relationship)
                    {
                        continue;
                    }
                    // Apply confidence filter.
                    if !opts.confidence_filter.is_empty()
                        && !opts.confidence_filter.contains(&edge.confidence)
                    {
                        continue;
                    }

                    let (upstream, downstream) = if is_upstream {
                        (edge.target.clone(), current.clone())
                    } else {
                        (current.clone(), edge.target.clone())
                    };

                    result_edges.push(TraceEdge {
                        upstream,
                        downstream,
                        relationship: edge.relationship,
                        confidence: edge.confidence,
                        expression_text: edge.expression_text.clone(),
                        depth: depth + 1,
                    });

                    if visited.insert(edge.target.clone()) {
                        queue.push_back((edge.target.clone(), depth + 1));
                    }
                }
            }
        }

        TraceResult {
            origin: start.clone(),
            edges: result_edges,
            truncated,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(n: u128) -> PipelineId {
        PipelineId(uuid::Uuid::from_u128(n))
    }

    fn nid(s: &str) -> NodeId {
        NodeId::new(s)
    }

    fn fp(s: &str) -> ResourceFingerprint {
        ResourceFingerprint::new(s)
    }

    // -- canonicalize_column -------------------------------------------------

    #[test]
    fn canonicalize_unquoted_lowercases() {
        assert_eq!(canonicalize_column("MyColumn"), "mycolumn");
        assert_eq!(canonicalize_column("NAME"), "name");
    }

    #[test]
    fn canonicalize_quoted_strips_quotes_preserves_case() {
        assert_eq!(canonicalize_column("\"MyColumn\""), "MyColumn");
        assert_eq!(canonicalize_column("\"NAME\""), "NAME");
    }

    #[test]
    fn canonicalize_trims_whitespace() {
        assert_eq!(canonicalize_column("  name  "), "name");
        assert_eq!(canonicalize_column("  \"Name\"  "), "Name");
    }

    #[test]
    fn canonicalize_empty_and_single_char() {
        assert_eq!(canonicalize_column(""), "");
        assert_eq!(canonicalize_column("a"), "a");
        assert_eq!(canonicalize_column("\""), "\"");
    }

    // -- derive_cross_pipeline_column_lineage --------------------------------

    #[test]
    fn matched_columns_produce_edges() {
        let boundary = vec![
            BoundaryColumn {
                pipeline_id: pid(1),
                node_id: nid("sink1"),
                column: "id".into(),
                fingerprint: fp("postgres://db/public.orders"),
                direction: BindingDirection::Sink,
            },
            BoundaryColumn {
                pipeline_id: pid(2),
                node_id: nid("src1"),
                column: "id".into(),
                fingerprint: fp("postgres://db/public.orders"),
                direction: BindingDirection::Source,
            },
        ];

        let result = derive_cross_pipeline_column_lineage(&boundary);
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.one_sided.len(), 0);

        let edge = &result.edges[0];
        assert_eq!(edge.upstream_pipeline_id, pid(1));
        assert_eq!(edge.upstream_node_id, nid("sink1"));
        assert_eq!(edge.downstream_pipeline_id, pid(2));
        assert_eq!(edge.downstream_node_id, nid("src1"));
        assert_eq!(edge.column, "id");
    }

    #[test]
    fn case_insensitive_matching_for_unquoted() {
        let boundary = vec![
            BoundaryColumn {
                pipeline_id: pid(1),
                node_id: nid("sink1"),
                column: "OrderID".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Sink,
            },
            BoundaryColumn {
                pipeline_id: pid(2),
                node_id: nid("src1"),
                column: "orderid".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Source,
            },
        ];

        let result = derive_cross_pipeline_column_lineage(&boundary);
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.one_sided.len(), 0);
    }

    #[test]
    fn quoted_columns_match_case_sensitively() {
        let boundary = vec![
            BoundaryColumn {
                pipeline_id: pid(1),
                node_id: nid("sink1"),
                column: "\"Name\"".into(),
                fingerprint: fp("pg://t"),
                direction: BindingDirection::Sink,
            },
            BoundaryColumn {
                pipeline_id: pid(2),
                node_id: nid("src1"),
                column: "\"name\"".into(),
                fingerprint: fp("pg://t"),
                direction: BindingDirection::Source,
            },
        ];

        // "Name" != "name" when quoted → no match, both one-sided.
        let result = derive_cross_pipeline_column_lineage(&boundary);
        assert_eq!(result.edges.len(), 0);
        assert_eq!(result.one_sided.len(), 2);
    }

    #[test]
    fn one_sided_sink_only() {
        let boundary = vec![BoundaryColumn {
            pipeline_id: pid(1),
            node_id: nid("sink1"),
            column: "extra_col".into(),
            fingerprint: fp("pg://orders"),
            direction: BindingDirection::Sink,
        }];

        let result = derive_cross_pipeline_column_lineage(&boundary);
        assert_eq!(result.edges.len(), 0);
        assert_eq!(result.one_sided.len(), 1);
        assert_eq!(result.one_sided[0].side, OneSidedKind::SinkOnly);
        assert_eq!(result.one_sided[0].column, "extra_col");
    }

    #[test]
    fn one_sided_source_only() {
        let boundary = vec![BoundaryColumn {
            pipeline_id: pid(2),
            node_id: nid("src1"),
            column: "missing_upstream".into(),
            fingerprint: fp("pg://orders"),
            direction: BindingDirection::Source,
        }];

        let result = derive_cross_pipeline_column_lineage(&boundary);
        assert_eq!(result.edges.len(), 0);
        assert_eq!(result.one_sided.len(), 1);
        assert_eq!(result.one_sided[0].side, OneSidedKind::SourceOnly);
    }

    #[test]
    fn different_fingerprints_do_not_match() {
        let boundary = vec![
            BoundaryColumn {
                pipeline_id: pid(1),
                node_id: nid("sink1"),
                column: "id".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Sink,
            },
            BoundaryColumn {
                pipeline_id: pid(2),
                node_id: nid("src1"),
                column: "id".into(),
                fingerprint: fp("pg://customers"),
                direction: BindingDirection::Source,
            },
        ];

        let result = derive_cross_pipeline_column_lineage(&boundary);
        assert_eq!(result.edges.len(), 0);
        assert_eq!(result.one_sided.len(), 2);
    }

    #[test]
    fn same_pipeline_same_node_skipped() {
        let boundary = vec![
            BoundaryColumn {
                pipeline_id: pid(1),
                node_id: nid("n1"),
                column: "id".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Sink,
            },
            BoundaryColumn {
                pipeline_id: pid(1),
                node_id: nid("n1"),
                column: "id".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Source,
            },
        ];

        let result = derive_cross_pipeline_column_lineage(&boundary);
        assert_eq!(result.edges.len(), 0);
    }

    #[test]
    fn multiple_pipelines_fan_out() {
        // One sink → two sources reading the same resource.
        let boundary = vec![
            BoundaryColumn {
                pipeline_id: pid(1),
                node_id: nid("sink1"),
                column: "id".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Sink,
            },
            BoundaryColumn {
                pipeline_id: pid(2),
                node_id: nid("src1"),
                column: "id".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Source,
            },
            BoundaryColumn {
                pipeline_id: pid(3),
                node_id: nid("src2"),
                column: "id".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Source,
            },
        ];

        let result = derive_cross_pipeline_column_lineage(&boundary);
        assert_eq!(result.edges.len(), 2);
        assert_eq!(result.one_sided.len(), 0);
    }

    #[test]
    fn mixed_matched_and_one_sided() {
        let boundary = vec![
            // Matched: both sides have "id"
            BoundaryColumn {
                pipeline_id: pid(1),
                node_id: nid("sink1"),
                column: "id".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Sink,
            },
            BoundaryColumn {
                pipeline_id: pid(2),
                node_id: nid("src1"),
                column: "id".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Source,
            },
            // Sink-only: "extra" only in sink
            BoundaryColumn {
                pipeline_id: pid(1),
                node_id: nid("sink1"),
                column: "extra".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Sink,
            },
            // Source-only: "missing" only in source
            BoundaryColumn {
                pipeline_id: pid(2),
                node_id: nid("src1"),
                column: "missing".into(),
                fingerprint: fp("pg://orders"),
                direction: BindingDirection::Source,
            },
        ];

        let result = derive_cross_pipeline_column_lineage(&boundary);
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].column, "id");

        let sink_only: Vec<_> = result
            .one_sided
            .iter()
            .filter(|o| o.side == OneSidedKind::SinkOnly)
            .collect();
        let source_only: Vec<_> = result
            .one_sided
            .iter()
            .filter(|o| o.side == OneSidedKind::SourceOnly)
            .collect();
        assert_eq!(sink_only.len(), 1);
        assert_eq!(sink_only[0].column, "extra");
        assert_eq!(source_only.len(), 1);
        assert_eq!(source_only[0].column, "missing");
    }

    // -- ColumnLineageGraph query engine ---------------------------------------

    fn key(pipeline: u128, node: &str, col: &str) -> ColumnKey {
        ColumnKey {
            pipeline_id: pid(pipeline),
            node_id: nid(node),
            column: col.into(),
        }
    }

    /// Build a 3-node linear chain: src(A) → transform(B) → sink(C)
    /// within a single pipeline. A.id → B.id (direct), B.total (derived
    /// from A.price + A.qty), B.total → C.total (direct).
    /// Also adds a filter edge: A.status filters B.id.
    fn linear_chain_graph() -> ColumnLineageGraph {
        let edges = vec![
            // src A → transform B: id is direct passthrough
            ColumnEdge {
                upstream_column: "id".into(),
                upstream_node: Some(nid("A")),
                upstream_resource: Some(fp("pg://orders")),
                downstream_column: "id".into(),
                downstream_node: Some(nid("B")),
                downstream_resource: None,
                relationship: RelationshipKind::Direct,
                expression_text: None,
                confidence: Confidence::Exact,
            },
            // src A → transform B: price derived into total
            ColumnEdge {
                upstream_column: "price".into(),
                upstream_node: Some(nid("A")),
                upstream_resource: Some(fp("pg://orders")),
                downstream_column: "total".into(),
                downstream_node: Some(nid("B")),
                downstream_resource: None,
                relationship: RelationshipKind::Derived,
                expression_text: Some("price * qty".into()),
                confidence: Confidence::Exact,
            },
            // src A → transform B: qty derived into total
            ColumnEdge {
                upstream_column: "qty".into(),
                upstream_node: Some(nid("A")),
                upstream_resource: Some(fp("pg://orders")),
                downstream_column: "total".into(),
                downstream_node: Some(nid("B")),
                downstream_resource: None,
                relationship: RelationshipKind::Derived,
                expression_text: Some("price * qty".into()),
                confidence: Confidence::Exact,
            },
            // A.status filters B.id
            ColumnEdge {
                upstream_column: "status".into(),
                upstream_node: Some(nid("A")),
                upstream_resource: Some(fp("pg://orders")),
                downstream_column: "id".into(),
                downstream_node: Some(nid("B")),
                downstream_resource: None,
                relationship: RelationshipKind::Filter,
                expression_text: Some("status = 'active'".into()),
                confidence: Confidence::Exact,
            },
            // transform B → sink C: total passthrough
            ColumnEdge {
                upstream_column: "total".into(),
                upstream_node: Some(nid("B")),
                upstream_resource: None,
                downstream_column: "total".into(),
                downstream_node: Some(nid("C")),
                downstream_resource: Some(fp("pg://summary")),
                relationship: RelationshipKind::Direct,
                expression_text: None,
                confidence: Confidence::Exact,
            },
        ];

        let pipeline_edges = vec![(pid(1), edges.as_slice())];
        ColumnLineageGraph::new(&pipeline_edges, &[])
    }

    #[test]
    fn upstream_trace_linear_chain() {
        let graph = linear_chain_graph();
        let opts = TraceOptions::default();

        // Trace upstream from C.total → B.total → A.price, A.qty
        let result = graph.upstream_trace(&key(1, "C", "total"), &opts);
        assert!(!result.truncated);
        assert_eq!(result.edges.len(), 3); // C←B, B←A.price, B←A.qty

        // Depth 1: C.total ← B.total
        let depth1: Vec<_> = result.edges.iter().filter(|e| e.depth == 1).collect();
        assert_eq!(depth1.len(), 1);
        assert_eq!(depth1[0].upstream.node_id, nid("B"));
        assert_eq!(depth1[0].upstream.column, "total");

        // Depth 2: B.total ← A.price, B.total ← A.qty
        let depth2: Vec<_> = result.edges.iter().filter(|e| e.depth == 2).collect();
        assert_eq!(depth2.len(), 2);
    }

    #[test]
    fn downstream_trace_linear_chain() {
        let graph = linear_chain_graph();
        let opts = TraceOptions::default();

        // Trace downstream from A.price → B.total → C.total
        let result = graph.downstream_trace(&key(1, "A", "price"), &opts);
        assert!(!result.truncated);
        assert_eq!(result.edges.len(), 2); // A→B.total, B.total→C.total

        let depth1: Vec<_> = result.edges.iter().filter(|e| e.depth == 1).collect();
        assert_eq!(depth1.len(), 1);
        assert_eq!(depth1[0].downstream.node_id, nid("B"));
        assert_eq!(depth1[0].downstream.column, "total");
    }

    #[test]
    fn trace_depth_limit_truncation() {
        let graph = linear_chain_graph();
        let opts = TraceOptions {
            max_depth: 1,
            ..Default::default()
        };

        // Depth limit 1: only C.total ← B.total
        let result = graph.upstream_trace(&key(1, "C", "total"), &opts);
        assert!(result.truncated);
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].depth, 1);
    }

    #[test]
    fn relationship_filter_excludes_filter_edges() {
        let graph = linear_chain_graph();
        let opts = TraceOptions {
            relationship_filter: [RelationshipKind::Direct, RelationshipKind::Derived]
                .into_iter()
                .collect(),
            ..Default::default()
        };

        // Upstream from B.id: should get A.id (Direct) but NOT A.status (Filter)
        let result = graph.upstream_trace(&key(1, "B", "id"), &opts);
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].relationship, RelationshipKind::Direct);
        assert_eq!(result.edges[0].upstream.column, "id");
    }

    #[test]
    fn confidence_filter() {
        // Build a graph with mixed confidence.
        let edges = vec![
            ColumnEdge {
                upstream_column: "x".into(),
                upstream_node: Some(nid("A")),
                upstream_resource: None,
                downstream_column: "y".into(),
                downstream_node: Some(nid("B")),
                downstream_resource: None,
                relationship: RelationshipKind::Direct,
                expression_text: None,
                confidence: Confidence::Exact,
            },
            ColumnEdge {
                upstream_column: "z".into(),
                upstream_node: Some(nid("A")),
                upstream_resource: None,
                downstream_column: "y".into(),
                downstream_node: Some(nid("B")),
                downstream_resource: None,
                relationship: RelationshipKind::Opaque,
                expression_text: None,
                confidence: Confidence::Opaque,
            },
        ];

        let graph = ColumnLineageGraph::new(&[(pid(1), edges.as_slice())], &[]);

        let opts = TraceOptions {
            confidence_filter: [Confidence::Exact].into_iter().collect(),
            ..Default::default()
        };

        let result = graph.upstream_trace(&key(1, "B", "y"), &opts);
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].confidence, Confidence::Exact);
        assert_eq!(result.edges[0].upstream.column, "x");
    }

    #[test]
    fn cross_pipeline_traversal() {
        // Pipeline 1: A.id → sink B.id (resource pg://orders)
        let p1_edges = vec![ColumnEdge {
            upstream_column: "id".into(),
            upstream_node: Some(nid("A")),
            upstream_resource: None,
            downstream_column: "id".into(),
            downstream_node: Some(nid("B")),
            downstream_resource: Some(fp("pg://orders")),
            relationship: RelationshipKind::Direct,
            expression_text: None,
            confidence: Confidence::Exact,
        }];

        // Pipeline 2: source C.id → transform D.order_id
        let p2_edges = vec![ColumnEdge {
            upstream_column: "id".into(),
            upstream_node: Some(nid("C")),
            upstream_resource: Some(fp("pg://orders")),
            downstream_column: "order_id".into(),
            downstream_node: Some(nid("D")),
            downstream_resource: None,
            relationship: RelationshipKind::Direct,
            expression_text: None,
            confidence: Confidence::Exact,
        }];

        // Cross-pipeline edge: P1.B → P2.C via pg://orders.id
        let cross = vec![CrossPipelineColumnEdge {
            upstream_pipeline_id: pid(1),
            upstream_node_id: nid("B"),
            downstream_pipeline_id: pid(2),
            downstream_node_id: nid("C"),
            column: "id".into(),
            fingerprint: fp("pg://orders"),
        }];

        let pipeline_edges = vec![(pid(1), p1_edges.as_slice()), (pid(2), p2_edges.as_slice())];
        let graph = ColumnLineageGraph::new(&pipeline_edges, &cross);
        let opts = TraceOptions::default();

        // Upstream from D.order_id should cross pipeline boundary.
        let result = graph.upstream_trace(&key(2, "D", "order_id"), &opts);
        // D←C (depth 1), C←B cross-pipeline (depth 2), B←A (depth 3)
        assert_eq!(result.edges.len(), 3);
        assert!(!result.truncated);

        // Verify cross-pipeline edge exists.
        let cross_edge = result
            .edges
            .iter()
            .find(|e| e.upstream.pipeline_id == pid(1) && e.downstream.pipeline_id == pid(2))
            .expect("cross-pipeline edge should exist");
        assert_eq!(cross_edge.depth, 2);
    }

    #[test]
    fn resolve_by_fingerprint_returns_matching_keys() {
        let graph = linear_chain_graph();
        let keys = graph.resolve_by_fingerprint(&fp("pg://orders"), "id");
        assert!(!keys.is_empty());
        // Should resolve to A.id (source boundary with that fingerprint)
        assert!(
            keys.iter()
                .any(|k| k.node_id == nid("A") && k.column == "id")
        );
    }

    #[test]
    fn empty_graph_returns_empty_trace() {
        let graph = ColumnLineageGraph::new(&[], &[]);
        let opts = TraceOptions::default();
        let result = graph.upstream_trace(&key(1, "X", "col"), &opts);
        assert!(result.edges.is_empty());
        assert!(!result.truncated);
    }
}
