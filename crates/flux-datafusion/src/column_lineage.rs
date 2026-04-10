// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! DataFusion LogicalPlan walker for column-level lineage.
//!
//! Walks a DataFusion `LogicalPlan` bottom-up, tracking which input columns
//! contribute to each output column and through what kind of relationship.
//! This produces ground-truth lineage because it operates on the engine's
//! actual logical plan rather than parsing SQL text.

use std::collections::{HashMap, HashSet};

use datafusion::logical_expr::{
    Aggregate, Distinct, Expr, Filter, Join, JoinType, Limit, LogicalPlan, Projection, Sort,
    SubqueryAlias, Union, Window,
};

use flux_engine::column_lineage::{ColumnEdge, Confidence, NodeColumnLineage, RelationshipKind};
use flux_engine::lineage::ResourceFingerprint;
use flux_engine::node::NodeId;

// ---------------------------------------------------------------------------
// Internal lineage representation used during the walk
// ---------------------------------------------------------------------------

/// During the walk, each output column is tracked as a set of upstream
/// references with their relationship kinds. This is the "working" format
/// that gets converted to `ColumnEdge` at the end.
#[derive(Debug, Clone)]
struct UpstreamRef {
    /// The column name in the upstream node's output schema.
    column: String,
    /// Which table/node this column belongs to (DataFusion qualifier).
    qualifier: Option<String>,
    /// The relationship between this upstream column and the downstream.
    kind: RelationshipKind,
    /// Human-readable expression text.
    expression: Option<String>,
}

/// Per-column lineage: maps an output column name to its upstream references.
type ColumnLineageMap = HashMap<String, Vec<UpstreamRef>>;

// ---------------------------------------------------------------------------
// Expression analysis
// ---------------------------------------------------------------------------

/// Extract all column references from a DataFusion expression.
fn expr_column_refs(expr: &Expr) -> HashSet<(Option<String>, String)> {
    let cols = expr.column_refs();
    cols.into_iter()
        .map(|c| {
            let qualifier = c.relation.as_ref().map(|r| r.to_string());
            (qualifier, c.name.clone())
        })
        .collect()
}

/// Determine the relationship kind for an expression.
///
/// - A bare column reference → `Direct`
/// - A CAST expression → `Cast`
/// - Anything else with column refs → `Derived`
fn classify_expr(expr: &Expr) -> RelationshipKind {
    match expr {
        Expr::Column(_) => RelationshipKind::Direct,
        Expr::Cast(_) | Expr::TryCast(_) => RelationshipKind::Cast,
        Expr::Alias(alias) => classify_expr(&alias.expr),
        _ => RelationshipKind::Derived,
    }
}

/// Get the output name of an expression (what it's called in the output schema).
fn expr_output_name(expr: &Expr) -> String {
    match expr.schema_name().to_string() {
        name if !name.is_empty() => name,
        _ => format!("{expr}"),
    }
}

/// Get a human-readable expression string (for tooltips), but only for
/// non-trivial expressions.
fn expr_display(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(_) => None,
        Expr::Alias(alias) => expr_display(&alias.expr),
        _ => Some(format!("{expr}")),
    }
}

// ---------------------------------------------------------------------------
// Plan walker
// ---------------------------------------------------------------------------

/// Walk a `LogicalPlan` and produce a `ColumnLineageMap` that describes,
/// for every output column, which upstream (leaf-level) columns contribute
/// to it.
///
/// The walk is recursive: each node merges its children's lineage maps
/// and applies its own transformations.
fn walk_plan(plan: &LogicalPlan) -> ColumnLineageMap {
    match plan {
        // ----- Base cases -----
        LogicalPlan::TableScan(scan) => {
            // Every output column maps to itself — this is the leaf of the
            // lineage graph. The qualifier is the table alias.
            let schema = scan.projected_schema.as_ref();
            let qualifier = scan.table_name.to_string();
            let mut map = ColumnLineageMap::new();
            for field in schema.fields() {
                let col_name = field.name().clone();
                map.insert(
                    col_name.clone(),
                    vec![UpstreamRef {
                        column: col_name,
                        qualifier: Some(qualifier.clone()),
                        kind: RelationshipKind::Direct,
                        expression: None,
                    }],
                );
            }
            map
        }

        LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => {
            // Literal values have no upstream columns.
            let schema = plan.schema();
            let mut map = ColumnLineageMap::new();
            for field in schema.fields() {
                map.insert(field.name().clone(), Vec::new());
            }
            map
        }

        // ----- Projection -----
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            ..
        }) => {
            let child_map = walk_plan(input);
            let mut map = ColumnLineageMap::new();

            for (i, e) in expr.iter().enumerate() {
                // Use the schema field name, which is the unqualified output name.
                let out_name = schema
                    .fields()
                    .get(i)
                    .map(|f| f.name().clone())
                    .unwrap_or_else(|| expr_output_name(e));
                let kind = classify_expr(e);
                let display = expr_display(e);
                let refs = expr_column_refs(e);

                let mut upstreams = Vec::new();
                for (qualifier, col_name) in &refs {
                    // Resolve through the child's lineage map.
                    let resolved_name = resolve_column(&child_map, qualifier.as_deref(), col_name);
                    if let Some(child_refs) = child_map.get(&resolved_name) {
                        for upstream in child_refs {
                            upstreams.push(UpstreamRef {
                                column: upstream.column.clone(),
                                qualifier: upstream.qualifier.clone(),
                                kind: if kind == RelationshipKind::Direct {
                                    upstream.kind
                                } else {
                                    kind
                                },
                                expression: display.clone().or_else(|| upstream.expression.clone()),
                            });
                        }
                    } else {
                        // Column not found in child — keep as direct ref.
                        upstreams.push(UpstreamRef {
                            column: col_name.clone(),
                            qualifier: qualifier.clone(),
                            kind,
                            expression: display.clone(),
                        });
                    }
                }
                map.insert(out_name, upstreams);
            }
            map
        }

        // ----- Filter -----
        LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) => {
            let mut map = walk_plan(input);

            // The filter predicate's column references influence which rows
            // survive — record them as `Filter` edges on every output column.
            let filter_refs = expr_column_refs(predicate);
            let filter_upstreams: Vec<UpstreamRef> = filter_refs
                .iter()
                .filter_map(|(qualifier, col_name)| {
                    let resolved = resolve_column(&map, qualifier.as_deref(), col_name);
                    map.get(&resolved).map(|refs| {
                        refs.iter().map(|r| UpstreamRef {
                            column: r.column.clone(),
                            qualifier: r.qualifier.clone(),
                            kind: RelationshipKind::Filter,
                            expression: Some(format!("{predicate}")),
                        })
                    })
                })
                .flatten()
                .collect();

            // Add filter edges to every output column.
            for refs in map.values_mut() {
                refs.extend(filter_upstreams.clone());
            }
            map
        }

        // ----- Join -----
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            filter,
            join_type,
            ..
        }) => {
            let left_map = walk_plan(left);
            let right_map = walk_plan(right);
            let mut map = ColumnLineageMap::new();

            // Collect join-key column names for marking.
            let mut join_key_cols: HashSet<String> = HashSet::new();
            for (l_expr, r_expr) in on {
                for (_, col_name) in expr_column_refs(l_expr) {
                    join_key_cols.insert(col_name);
                }
                for (_, col_name) in expr_column_refs(r_expr) {
                    join_key_cols.insert(col_name);
                }
            }

            // Also collect filter-referenced columns.
            let filter_cols: HashSet<String> = filter
                .as_ref()
                .map(|f| expr_column_refs(f).into_iter().map(|(_, c)| c).collect())
                .unwrap_or_default();

            // Merge left and right maps, marking join keys and passthroughs.
            let is_nullable_left = matches!(join_type, JoinType::Right | JoinType::Full);
            let is_nullable_right = matches!(join_type, JoinType::Left | JoinType::Full);
            let _ = (is_nullable_left, is_nullable_right); // reserved for future use

            for (side_map, _is_nullable) in [
                (&left_map, is_nullable_left),
                (&right_map, is_nullable_right),
            ] {
                for (col_name, refs) in side_map {
                    let is_join_key = join_key_cols.contains(col_name);
                    let is_filter = filter_cols.contains(col_name);
                    let reclassified: Vec<UpstreamRef> = refs
                        .iter()
                        .map(|r| {
                            let kind = if is_join_key {
                                RelationshipKind::JoinKey
                            } else if is_filter {
                                RelationshipKind::Filter
                            } else {
                                RelationshipKind::JoinPassthrough
                            };
                            UpstreamRef {
                                column: r.column.clone(),
                                qualifier: r.qualifier.clone(),
                                kind,
                                expression: r.expression.clone(),
                            }
                        })
                        .collect();
                    map.entry(col_name.clone())
                        .or_default()
                        .extend(reclassified);
                }
            }
            map
        }

        // ----- Aggregate -----
        LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            ..
        }) => {
            let child_map = walk_plan(input);
            let mut map = ColumnLineageMap::new();

            // Group-by expressions.
            for e in group_expr {
                let out_name = expr_output_name(e);
                let refs = expr_column_refs(e);
                let mut upstreams = Vec::new();
                for (qualifier, col_name) in &refs {
                    let resolved = resolve_column(&child_map, qualifier.as_deref(), col_name);
                    if let Some(child_refs) = child_map.get(&resolved) {
                        for upstream in child_refs {
                            upstreams.push(UpstreamRef {
                                column: upstream.column.clone(),
                                qualifier: upstream.qualifier.clone(),
                                kind: RelationshipKind::GroupBy,
                                expression: None,
                            });
                        }
                    }
                }
                map.insert(out_name, upstreams);
            }

            // Aggregate expressions.
            for e in aggr_expr {
                let out_name = expr_output_name(e);
                let display = expr_display(e);
                let refs = expr_column_refs(e);
                let mut upstreams = Vec::new();
                for (qualifier, col_name) in &refs {
                    let resolved = resolve_column(&child_map, qualifier.as_deref(), col_name);
                    if let Some(child_refs) = child_map.get(&resolved) {
                        for upstream in child_refs {
                            upstreams.push(UpstreamRef {
                                column: upstream.column.clone(),
                                qualifier: upstream.qualifier.clone(),
                                kind: RelationshipKind::AggregateInput,
                                expression: display.clone(),
                            });
                        }
                    }
                }
                map.insert(out_name, upstreams);
            }
            map
        }

        // ----- Window -----
        LogicalPlan::Window(Window {
            input, window_expr, ..
        }) => {
            let mut map = walk_plan(input);

            for e in window_expr {
                let out_name = expr_output_name(e);
                let display = expr_display(e);
                let all_refs = expr_column_refs(e);

                // Classify window sub-expression references.
                let (partition_cols, order_cols, _input_cols) = classify_window_expr(e);

                let mut upstreams = Vec::new();
                for (qualifier, col_name) in &all_refs {
                    let resolved = resolve_column(&map, qualifier.as_deref(), col_name);
                    let child_refs = map.get(&resolved).cloned().unwrap_or_default();
                    for upstream in &child_refs {
                        let kind = if partition_cols.contains(col_name) {
                            RelationshipKind::WindowPartition
                        } else if order_cols.contains(col_name) {
                            RelationshipKind::WindowOrder
                        } else {
                            RelationshipKind::WindowInput
                        };
                        upstreams.push(UpstreamRef {
                            column: upstream.column.clone(),
                            qualifier: upstream.qualifier.clone(),
                            kind,
                            expression: display.clone(),
                        });
                    }
                }
                map.insert(out_name, upstreams);
            }
            map
        }

        // ----- Union -----
        LogicalPlan::Union(Union { inputs, schema }) => {
            let mut map = ColumnLineageMap::new();
            for field in schema.fields() {
                map.insert(field.name().clone(), Vec::new());
            }
            for input in inputs {
                let child_map = walk_plan(input);
                // Union merges columns by position — names come from the first
                // input but all inputs contribute.
                for (i, field) in schema.fields().iter().enumerate() {
                    let out_name = field.name();
                    // Find the corresponding child column by position.
                    let child_schema = input.schema();
                    if let Some(child_field) = child_schema.fields().get(i) {
                        if let Some(refs) = child_map.get(child_field.name()) {
                            map.entry(out_name.clone())
                                .or_default()
                                .extend(refs.clone());
                        }
                    }
                }
            }
            map
        }

        // ----- SubqueryAlias -----
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
            // SubqueryAlias just renames the relation — column lineage passes
            // through unchanged.
            walk_plan(input)
        }

        // ----- Distinct -----
        LogicalPlan::Distinct(distinct) => {
            let input = match distinct {
                Distinct::All(input) => input,
                Distinct::On(on) => &on.input,
            };
            walk_plan(input)
        }

        // ----- Sort -----
        LogicalPlan::Sort(Sort { input, .. }) => walk_plan(input),

        // ----- Limit -----
        LogicalPlan::Limit(Limit { input, .. }) => walk_plan(input),

        // ----- Repartition -----
        LogicalPlan::Repartition(repart) => walk_plan(&repart.input),

        // ----- Fallback: passthrough for unknown nodes -----
        other => {
            let inputs = other.inputs();
            if inputs.len() == 1 {
                walk_plan(inputs[0])
            } else if inputs.is_empty() {
                // Leaf node with no inputs — produce empty lineage.
                let schema = other.schema();
                let mut map = ColumnLineageMap::new();
                for field in schema.fields() {
                    map.insert(field.name().clone(), Vec::new());
                }
                map
            } else {
                // Multiple inputs — merge all.
                let mut map = ColumnLineageMap::new();
                for input in &inputs {
                    let child_map = walk_plan(input);
                    for (col, refs) in child_map {
                        map.entry(col).or_default().extend(refs);
                    }
                }
                map
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Window expression classification
// ---------------------------------------------------------------------------

/// Classify columns within a window expression into partition, order, and
/// input columns.
fn classify_window_expr(expr: &Expr) -> (HashSet<String>, HashSet<String>, HashSet<String>) {
    let mut partition_cols = HashSet::new();
    let mut order_cols = HashSet::new();
    let mut input_cols = HashSet::new();

    if let Expr::WindowFunction(wf) = unwrap_alias(expr) {
        // Partition-by columns.
        for p_expr in &wf.params.partition_by {
            for (_, col) in expr_column_refs(p_expr) {
                partition_cols.insert(col);
            }
        }
        // Order-by columns.
        for sort_expr in &wf.params.order_by {
            for (_, col) in expr_column_refs(&sort_expr.expr) {
                order_cols.insert(col);
            }
        }
        // The function's own arguments are input columns.
        for arg in &wf.params.args {
            for (_, col) in expr_column_refs(arg) {
                input_cols.insert(col);
            }
        }
    }

    (partition_cols, order_cols, input_cols)
}

/// Unwrap Alias wrappers to get at the inner expression.
fn unwrap_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => unwrap_alias(&alias.expr),
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Column resolution helper
// ---------------------------------------------------------------------------

/// Resolve a column reference against the lineage map, handling qualifiers.
///
/// DataFusion column references may or may not have a table qualifier. We try
/// multiple key patterns: unqualified name, qualified name, and schema-style
/// qualified names (e.g. `table.col`).
fn resolve_column(map: &ColumnLineageMap, qualifier: Option<&str>, name: &str) -> String {
    // Try unqualified name first.
    if map.contains_key(name) {
        return name.to_string();
    }
    // Try qualified name (e.g. "orders.id").
    if let Some(q) = qualifier {
        let qualified = format!("{q}.{name}");
        if map.contains_key(&qualified) {
            return qualified;
        }
    }
    name.to_string()
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Derive column-level lineage from a DataFusion `LogicalPlan`.
///
/// Given the logical plan of a SQL transform and the node ID that produced it,
/// returns a `NodeColumnLineage` with edges connecting each output column to
/// its upstream source columns.
///
/// The `table_to_node` map translates DataFusion table names (which correspond
/// to upstream node IDs registered as MemTable) back to pipeline `NodeId`s.
pub fn derive_column_lineage(
    plan: &LogicalPlan,
    node_id: &NodeId,
    table_to_node: &HashMap<String, NodeId>,
) -> NodeColumnLineage {
    let raw_map = walk_plan(plan);
    let mut result = NodeColumnLineage::new(node_id.clone());

    for (out_col, upstreams) in &raw_map {
        for upstream in upstreams {
            // Map the DataFusion qualifier (table name) back to a pipeline node.
            let upstream_node = upstream
                .qualifier
                .as_ref()
                .and_then(|q| table_to_node.get(q))
                .cloned();

            result.edges.push(ColumnEdge {
                upstream_column: upstream.column.clone(),
                upstream_node,
                upstream_resource: None,
                downstream_column: out_col.clone(),
                downstream_node: Some(node_id.clone()),
                downstream_resource: None,
                relationship: upstream.kind,
                expression_text: upstream.expression.clone(),
                confidence: Confidence::Exact,
            });
        }
    }

    // Sort edges for deterministic output.
    result.edges.sort_by(|a, b| {
        a.downstream_column
            .cmp(&b.downstream_column)
            .then_with(|| a.upstream_column.cmp(&b.upstream_column))
            .then_with(|| a.relationship.to_string().cmp(&b.relationship.to_string()))
    });

    result
}

/// Produce opaque (conservative) lineage for a node whose transform cannot
/// be introspected. Every output column is connected to every input column
/// with `RelationshipKind::Opaque`.
pub fn derive_opaque_lineage(
    node_id: &NodeId,
    input_columns: &[(NodeId, Vec<String>)],
    output_columns: &[String],
) -> NodeColumnLineage {
    let mut result = NodeColumnLineage::new(node_id.clone());

    for out_col in output_columns {
        for (upstream_node, upstream_cols) in input_columns {
            for upstream_col in upstream_cols {
                result.edges.push(ColumnEdge {
                    upstream_column: upstream_col.clone(),
                    upstream_node: Some(upstream_node.clone()),
                    upstream_resource: None,
                    downstream_column: out_col.clone(),
                    downstream_node: Some(node_id.clone()),
                    downstream_resource: None,
                    relationship: RelationshipKind::Opaque,
                    expression_text: None,
                    confidence: Confidence::Opaque,
                });
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Polars LazyFrame lineage conversion
// ---------------------------------------------------------------------------

/// Convert Python-extracted column lineage into a `NodeColumnLineage`.
///
/// Maps the string-based relationship and confidence from the Python walker
/// to the Rust enum types. Upstream node IDs are resolved from the
/// `input_columns` map (node → columns).
pub fn derive_python_lineage(
    node_id: &NodeId,
    python_lineage: &crate::python_runtime::PythonColumnLineage,
    input_columns: &[(NodeId, Vec<String>)],
) -> NodeColumnLineage {
    let mut result = NodeColumnLineage::new(node_id.clone());

    let confidence = match python_lineage.confidence.as_str() {
        "lazyframe" => Confidence::LazyFrame,
        "annotation" => Confidence::Annotation,
        _ => Confidence::Opaque,
    };

    // Build a reverse map: column_name → upstream NodeId.
    // If multiple upstream nodes have the same column name, the first wins
    // (matches Polars' behavior where earlier inputs shadow later ones).
    let mut col_to_node: std::collections::HashMap<&str, &NodeId> =
        std::collections::HashMap::new();
    for (nid, cols) in input_columns {
        for col in cols {
            col_to_node.entry(col.as_str()).or_insert(nid);
        }
    }

    for edge in &python_lineage.edges {
        let relationship = parse_relationship(&edge.relationship);
        let upstream_node = col_to_node.get(edge.upstream_column.as_str()).copied();

        result.edges.push(ColumnEdge {
            upstream_column: edge.upstream_column.clone(),
            upstream_node: upstream_node.cloned(),
            upstream_resource: None,
            downstream_column: edge.downstream_column.clone(),
            downstream_node: Some(node_id.clone()),
            downstream_resource: None,
            relationship,
            expression_text: edge.expression_text.clone(),
            confidence,
        });
    }

    result
}

// ---------------------------------------------------------------------------
// Annotation-based lineage (planning doc 35c)
// ---------------------------------------------------------------------------

/// Derive column lineage from user-provided annotations.
///
/// Annotations take precedence over both LazyFrame-derived and opaque lineage
/// (precedence: annotation > lazyframe > opaque). Each annotated edge is
/// emitted with `Confidence::Annotation`.
///
/// The `input_columns` map is used to resolve upstream column names to their
/// producing node IDs. When `output_columns` is provided, downstream column
/// references are validated against the actual output schema. Invalid
/// references produce warnings but do not prevent lineage from being emitted.
pub fn derive_annotation_lineage(
    node_id: &NodeId,
    annotations: &flux_engine::column_lineage::LineageAnnotations,
    input_columns: &[(NodeId, Vec<String>)],
    output_columns: Option<&[String]>,
) -> NodeColumnLineage {
    let mut result = NodeColumnLineage::new(node_id.clone());

    // Build reverse map: column_name → upstream NodeId.
    let mut col_to_node: std::collections::HashMap<&str, &NodeId> =
        std::collections::HashMap::new();
    let mut all_input_cols: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for (nid, cols) in input_columns {
        for col in cols {
            col_to_node.entry(col.as_str()).or_insert(nid);
            all_input_cols.insert(col.as_str());
        }
    }

    // Build output column set for downstream validation.
    let output_set: Option<std::collections::HashSet<&str>> =
        output_columns.map(|cols| cols.iter().map(|c| c.as_str()).collect());

    for edge in &annotations.edges {
        // Validate upstream column exists in input schemas.
        let upstream_node = col_to_node.get(edge.upstream_column.as_str()).copied();
        if !all_input_cols.contains(edge.upstream_column.as_str()) {
            result.warnings.push(format!(
                "annotation references upstream column '{}' which does not \
                 exist in any input schema",
                edge.upstream_column,
            ));
        }

        // Validate downstream column exists in output schema.
        if let Some(ref out) = output_set {
            if !out.contains(edge.downstream_column.as_str()) {
                result.warnings.push(format!(
                    "annotation references downstream column '{}' which does \
                     not exist in the output schema",
                    edge.downstream_column,
                ));
            }
        }

        result.edges.push(ColumnEdge {
            upstream_column: edge.upstream_column.clone(),
            upstream_node: upstream_node.cloned(),
            upstream_resource: None,
            downstream_column: edge.downstream_column.clone(),
            downstream_node: Some(node_id.clone()),
            downstream_resource: None,
            relationship: edge.relationship,
            expression_text: None,
            confidence: Confidence::Annotation,
        });
    }

    result
}

fn parse_relationship(s: &str) -> RelationshipKind {
    match s {
        "direct" => RelationshipKind::Direct,
        "derived" => RelationshipKind::Derived,
        "cast" => RelationshipKind::Cast,
        "filter" => RelationshipKind::Filter,
        "join_key" => RelationshipKind::JoinKey,
        "join_passthrough" => RelationshipKind::JoinPassthrough,
        "group_by" => RelationshipKind::GroupBy,
        "aggregate_input" => RelationshipKind::AggregateInput,
        "window_partition" => RelationshipKind::WindowPartition,
        "window_order" => RelationshipKind::WindowOrder,
        "window_input" => RelationshipKind::WindowInput,
        _ => RelationshipKind::Opaque,
    }
}

// ---------------------------------------------------------------------------
// Boundary edge derivation (planning doc 35 — cross-pipeline derivation)
// ---------------------------------------------------------------------------

/// Derive boundary column lineage edges for a **source** node.
///
/// Creates `Direct` edges from the external resource to the source node for
/// each column the source produces. These edges carry the resource
/// fingerprint on the upstream side, enabling cross-pipeline column matching.
pub fn derive_source_boundary_lineage(
    node_id: &NodeId,
    fingerprint: &ResourceFingerprint,
    columns: &[String],
) -> NodeColumnLineage {
    let mut result = NodeColumnLineage::new(node_id.clone());
    for col in columns {
        result.edges.push(ColumnEdge {
            upstream_column: col.clone(),
            upstream_node: None,
            upstream_resource: Some(fingerprint.clone()),
            downstream_column: col.clone(),
            downstream_node: Some(node_id.clone()),
            downstream_resource: None,
            relationship: RelationshipKind::Direct,
            expression_text: None,
            confidence: Confidence::Exact,
        });
    }
    result
}

/// Derive boundary column lineage edges for a **sink** node.
///
/// Creates `Direct` edges from the upstream node to the sink node with the
/// resource fingerprint set on the downstream side. These edges mark the
/// pipeline boundary so cross-pipeline column matching can identify what
/// columns a sink writes to an external resource.
pub fn derive_sink_boundary_lineage(
    node_id: &NodeId,
    fingerprint: &ResourceFingerprint,
    columns: &[String],
    upstream_node: &NodeId,
) -> NodeColumnLineage {
    let mut result = NodeColumnLineage::new(node_id.clone());
    for col in columns {
        result.edges.push(ColumnEdge {
            upstream_column: col.clone(),
            upstream_node: Some(upstream_node.clone()),
            upstream_resource: None,
            downstream_column: col.clone(),
            downstream_node: Some(node_id.clone()),
            downstream_resource: Some(fingerprint.clone()),
            relationship: RelationshipKind::Direct,
            expression_text: None,
            confidence: Confidence::Exact,
        });
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::*;
    use std::sync::Arc;

    /// Helper: create a SessionContext with a registered table.
    async fn ctx_with_tables(tables: &[(&str, Vec<(&str, DataType)>)]) -> SessionContext {
        let ctx = SessionContext::new();
        for (name, cols) in tables {
            let fields: Vec<Field> = cols
                .iter()
                .map(|(n, dt)| Field::new(*n, dt.clone(), true))
                .collect();
            let schema = Arc::new(Schema::new(fields));
            let batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
            let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
            ctx.register_table(
                datafusion::common::TableReference::bare(name.to_string()),
                Arc::new(mem),
            )
            .unwrap();
        }
        ctx
    }

    /// Helper: derive lineage from a SQL query against the given tables.
    async fn lineage_for_sql(
        tables: &[(&str, Vec<(&str, DataType)>)],
        sql: &str,
    ) -> NodeColumnLineage {
        let ctx = ctx_with_tables(tables).await;
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.logical_plan();
        let node_id = NodeId::from("test_node");
        let table_to_node: HashMap<String, NodeId> = tables
            .iter()
            .map(|(name, _)| (name.to_string(), NodeId::from(*name)))
            .collect();
        derive_column_lineage(plan, &node_id, &table_to_node)
    }

    /// Helper: find edges for a given downstream column.
    fn edges_for<'a>(lineage: &'a NodeColumnLineage, col: &str) -> Vec<&'a ColumnEdge> {
        lineage.edges_for_column(col)
    }

    // ------- Projection tests -------

    #[tokio::test]
    async fn test_simple_passthrough() {
        let lineage = lineage_for_sql(
            &[(
                "orders",
                vec![("id", DataType::Int64), ("amount", DataType::Float64)],
            )],
            "SELECT id, amount FROM orders",
        )
        .await;

        let id_edges = edges_for(&lineage, "id");
        assert_eq!(id_edges.len(), 1);
        assert_eq!(id_edges[0].upstream_column, "id");
        assert_eq!(id_edges[0].relationship, RelationshipKind::Direct);

        let amount_edges = edges_for(&lineage, "amount");
        assert_eq!(amount_edges.len(), 1);
        assert_eq!(amount_edges[0].upstream_column, "amount");
    }

    #[tokio::test]
    async fn test_derived_expression() {
        let lineage = lineage_for_sql(
            &[(
                "orders",
                vec![("price", DataType::Float64), ("qty", DataType::Int64)],
            )],
            "SELECT price * qty AS total FROM orders",
        )
        .await;

        let total_edges = edges_for(&lineage, "total");
        assert_eq!(total_edges.len(), 2);
        let upstream_cols: HashSet<&str> = total_edges
            .iter()
            .map(|e| e.upstream_column.as_str())
            .collect();
        assert!(upstream_cols.contains("price"));
        assert!(upstream_cols.contains("qty"));
        assert!(
            total_edges
                .iter()
                .all(|e| e.relationship == RelationshipKind::Derived)
        );
    }

    // ------- Filter test -------

    #[tokio::test]
    async fn test_filter_edges() {
        let lineage = lineage_for_sql(
            &[(
                "orders",
                vec![("id", DataType::Int64), ("status", DataType::Utf8)],
            )],
            "SELECT id FROM orders WHERE status = 'active'",
        )
        .await;

        let id_edges = edges_for(&lineage, "id");
        // Should have: 1 direct from id, 1 filter from status.
        let direct: Vec<_> = id_edges
            .iter()
            .filter(|e| e.relationship == RelationshipKind::Direct)
            .collect();
        let filter: Vec<_> = id_edges
            .iter()
            .filter(|e| e.relationship == RelationshipKind::Filter)
            .collect();
        assert_eq!(direct.len(), 1);
        assert_eq!(direct[0].upstream_column, "id");
        assert_eq!(filter.len(), 1);
        assert_eq!(filter[0].upstream_column, "status");
    }

    // ------- Join test -------

    #[tokio::test]
    async fn test_join_lineage() {
        let lineage = lineage_for_sql(
            &[
                (
                    "orders",
                    vec![
                        ("order_id", DataType::Int64),
                        ("customer_id", DataType::Int64),
                        ("amount", DataType::Float64),
                    ],
                ),
                (
                    "customers",
                    vec![("customer_id", DataType::Int64), ("name", DataType::Utf8)],
                ),
            ],
            "SELECT o.order_id, o.amount, c.name \
             FROM orders o JOIN customers c ON o.customer_id = c.customer_id",
        )
        .await;

        // order_id should be a passthrough from the join.
        let oid = edges_for(&lineage, "order_id");
        assert!(!oid.is_empty());

        // name should come from customers.
        let name = edges_for(&lineage, "name");
        assert!(!name.is_empty());
        assert!(name.iter().any(|e| e.upstream_column == "name"));
    }

    // ------- Aggregate test -------

    #[tokio::test]
    async fn test_aggregate_lineage() {
        let lineage = lineage_for_sql(
            &[(
                "sales",
                vec![("region", DataType::Utf8), ("amount", DataType::Float64)],
            )],
            "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
        )
        .await;

        let region = edges_for(&lineage, "region");
        assert!(
            region
                .iter()
                .any(|e| e.relationship == RelationshipKind::GroupBy),
            "region should be a group-by key"
        );

        let total = edges_for(&lineage, "total");
        assert!(
            total
                .iter()
                .any(|e| e.relationship == RelationshipKind::AggregateInput),
            "total should have aggregate_input edges"
        );
        assert!(total.iter().any(|e| e.upstream_column == "amount"));
    }

    // ------- Window test -------

    #[tokio::test]
    async fn test_window_function_lineage() {
        let lineage = lineage_for_sql(
            &[(
                "sales",
                vec![
                    ("region", DataType::Utf8),
                    ("date", DataType::Date32),
                    ("amount", DataType::Float64),
                ],
            )],
            "SELECT region, date, amount, \
                    SUM(amount) OVER (PARTITION BY region ORDER BY date) AS running_total \
             FROM sales",
        )
        .await;

        let rt = edges_for(&lineage, "running_total");
        assert!(!rt.is_empty(), "running_total should have lineage edges");

        let kinds: HashSet<RelationshipKind> = rt.iter().map(|e| e.relationship).collect();
        assert!(
            kinds.contains(&RelationshipKind::WindowPartition)
                || kinds.contains(&RelationshipKind::WindowInput)
                || kinds.contains(&RelationshipKind::WindowOrder),
            "window function should produce window-specific edge kinds, got: {kinds:?}"
        );
    }

    // ------- Union test -------

    #[tokio::test]
    async fn test_union_lineage() {
        let lineage = lineage_for_sql(
            &[
                (
                    "us_sales",
                    vec![("id", DataType::Int64), ("amount", DataType::Float64)],
                ),
                (
                    "eu_sales",
                    vec![("id", DataType::Int64), ("amount", DataType::Float64)],
                ),
            ],
            "SELECT id, amount FROM us_sales \
             UNION ALL \
             SELECT id, amount FROM eu_sales",
        )
        .await;

        let id_edges = edges_for(&lineage, "id");
        // Should have edges from both us_sales and eu_sales.
        let upstream_nodes: HashSet<Option<&NodeId>> =
            id_edges.iter().map(|e| e.upstream_node.as_ref()).collect();
        assert!(
            upstream_nodes.len() >= 2,
            "union should produce edges from both inputs"
        );
    }

    // ------- Opaque fallback test -------

    #[test]
    fn test_opaque_fallback() {
        let node_id = NodeId::from("python_node");
        let inputs = vec![(NodeId::from("src"), vec!["a".to_string(), "b".to_string()])];
        let outputs = vec!["x".to_string(), "y".to_string()];

        let lineage = derive_opaque_lineage(&node_id, &inputs, &outputs);

        // Every output × every input = 4 edges.
        assert_eq!(lineage.edges.len(), 4);
        assert!(
            lineage
                .edges
                .iter()
                .all(|e| e.relationship == RelationshipKind::Opaque)
        );
        assert!(
            lineage
                .edges
                .iter()
                .all(|e| e.confidence == Confidence::Opaque)
        );
    }

    // ------- Annotation lineage tests -------

    #[test]
    fn test_annotation_lineage_basic() {
        use flux_engine::column_lineage::{LineageAnnotationEdge, LineageAnnotations};

        let node_id = NodeId::from("eager_python");
        let inputs = vec![(
            NodeId::from("src"),
            vec!["price".to_string(), "qty".to_string()],
        )];
        let annotations = LineageAnnotations {
            edges: vec![
                LineageAnnotationEdge {
                    upstream_column: "price".to_string(),
                    downstream_column: "total".to_string(),
                    relationship: RelationshipKind::Derived,
                },
                LineageAnnotationEdge {
                    upstream_column: "qty".to_string(),
                    downstream_column: "total".to_string(),
                    relationship: RelationshipKind::Derived,
                },
            ],
        };

        let lineage = derive_annotation_lineage(&node_id, &annotations, &inputs, None);

        assert_eq!(lineage.edges.len(), 2);
        assert!(lineage.warnings.is_empty());
        assert!(
            lineage
                .edges
                .iter()
                .all(|e| e.confidence == Confidence::Annotation)
        );
        assert!(lineage.edges.iter().all(|e| e.downstream_column == "total"));
        let upstream_cols: HashSet<&str> = lineage
            .edges
            .iter()
            .map(|e| e.upstream_column.as_str())
            .collect();
        assert!(upstream_cols.contains("price"));
        assert!(upstream_cols.contains("qty"));
        // Upstream node should be resolved from input_columns.
        assert!(
            lineage
                .edges
                .iter()
                .all(|e| e.upstream_node == Some(NodeId::from("src")))
        );
    }

    #[test]
    fn test_annotation_overrides_opaque() {
        use flux_engine::column_lineage::{LineageAnnotationEdge, LineageAnnotations};

        let node_id = NodeId::from("python_node");
        let inputs = vec![(NodeId::from("src"), vec!["a".to_string(), "b".to_string()])];
        let outputs = vec!["x".to_string(), "y".to_string()];

        // Opaque produces 4 edges (every input × every output).
        let opaque = derive_opaque_lineage(&node_id, &inputs, &outputs);
        assert_eq!(opaque.edges.len(), 4);

        // Annotation is more precise: x comes only from a, y only from b.
        let annotations = LineageAnnotations {
            edges: vec![
                LineageAnnotationEdge {
                    upstream_column: "a".to_string(),
                    downstream_column: "x".to_string(),
                    relationship: RelationshipKind::Direct,
                },
                LineageAnnotationEdge {
                    upstream_column: "b".to_string(),
                    downstream_column: "y".to_string(),
                    relationship: RelationshipKind::Derived,
                },
            ],
        };
        let annotated = derive_annotation_lineage(&node_id, &annotations, &inputs, None);

        // Only 2 edges vs 4 opaque — annotation is more precise.
        assert_eq!(annotated.edges.len(), 2);
        assert!(
            annotated
                .edges
                .iter()
                .all(|e| e.confidence == Confidence::Annotation)
        );

        // Annotation confidence is higher-priority than Opaque.
        assert!(Confidence::Annotation < Confidence::Opaque);
    }

    #[test]
    fn test_annotation_confidence_ordering() {
        // Verify the precedence: Exact < LazyFrame < Annotation < Opaque.
        assert!(Confidence::Exact < Confidence::LazyFrame);
        assert!(Confidence::LazyFrame < Confidence::Annotation);
        assert!(Confidence::Annotation < Confidence::Opaque);
    }

    #[test]
    fn test_python_lineage_annotation_confidence() {
        // Verify that derive_python_lineage correctly maps "annotation"
        // confidence from the Python sidecar.
        use crate::python_runtime::{PythonColumnEdge, PythonColumnLineage};

        let node_id = NodeId::from("decorated_python");
        let inputs = vec![(NodeId::from("src"), vec!["col_a".to_string()])];
        let py_lineage = PythonColumnLineage {
            edges: vec![PythonColumnEdge {
                upstream_column: "col_a".to_string(),
                downstream_column: "col_b".to_string(),
                relationship: "derived".to_string(),
                expression_text: None,
            }],
            confidence: "annotation".to_string(),
            warnings: vec![],
        };

        let lineage = derive_python_lineage(&node_id, &py_lineage, &inputs);

        assert_eq!(lineage.edges.len(), 1);
        assert_eq!(lineage.edges[0].confidence, Confidence::Annotation);
    }

    // ------- Annotation validation tests -------

    #[test]
    fn test_annotation_validation_warns_on_bad_columns() {
        use flux_engine::column_lineage::{LineageAnnotationEdge, LineageAnnotations};

        let node_id = NodeId::from("xform");
        let inputs = vec![(
            NodeId::from("src"),
            vec!["price".to_string(), "qty".to_string()],
        )];
        let output_cols = vec!["total".to_string()];

        let annotations = LineageAnnotations {
            edges: vec![
                // Valid edge.
                LineageAnnotationEdge {
                    upstream_column: "price".to_string(),
                    downstream_column: "total".to_string(),
                    relationship: RelationshipKind::Derived,
                },
                // Bad upstream column.
                LineageAnnotationEdge {
                    upstream_column: "nonexistent_input".to_string(),
                    downstream_column: "total".to_string(),
                    relationship: RelationshipKind::Derived,
                },
                // Bad downstream column.
                LineageAnnotationEdge {
                    upstream_column: "qty".to_string(),
                    downstream_column: "bogus_output".to_string(),
                    relationship: RelationshipKind::Derived,
                },
            ],
        };

        let lineage =
            derive_annotation_lineage(&node_id, &annotations, &inputs, Some(&output_cols));

        // All 3 edges are still emitted (warnings don't suppress edges).
        assert_eq!(lineage.edges.len(), 3);

        // Exactly 2 warnings: one upstream, one downstream.
        assert_eq!(lineage.warnings.len(), 2);
        assert!(lineage.warnings[0].contains("nonexistent_input"));
        assert!(lineage.warnings[0].contains("upstream"));
        assert!(lineage.warnings[1].contains("bogus_output"));
        assert!(lineage.warnings[1].contains("downstream"));
    }

    #[test]
    fn test_annotation_validation_no_warnings_when_valid() {
        use flux_engine::column_lineage::{LineageAnnotationEdge, LineageAnnotations};

        let node_id = NodeId::from("xform");
        let inputs = vec![(NodeId::from("src"), vec!["a".to_string()])];
        let output_cols = vec!["b".to_string()];

        let annotations = LineageAnnotations {
            edges: vec![LineageAnnotationEdge {
                upstream_column: "a".to_string(),
                downstream_column: "b".to_string(),
                relationship: RelationshipKind::Direct,
            }],
        };

        let lineage =
            derive_annotation_lineage(&node_id, &annotations, &inputs, Some(&output_cols));

        assert_eq!(lineage.edges.len(), 1);
        assert!(lineage.warnings.is_empty());
    }
}
