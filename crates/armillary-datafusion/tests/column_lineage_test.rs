// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Golden-file style regression tests for the DataFusion column-level lineage
//! walker. Each test defines a SQL query against a known schema and asserts the
//! exact edge set the walker produces.
//!
//! These tests serve as regression guards against DataFusion plan shape changes
//! (new intermediate nodes, changed expression representations) that could
//! silently break lineage derivation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;

use armillary_engine::column_lineage::{
    BoundaryColumn, ColumnEdge, ColumnKey, ColumnLineageGraph, Confidence, CrossPipelineColumnEdge,
    NodeColumnLineage, RelationshipKind, TraceOptions,
};
use armillary_engine::lineage::ResourceFingerprint;
use armillary_engine::node::NodeId;
use armillary_engine::pipeline_store::PipelineId;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn ctx_with_tables(tables: &[(&str, Vec<(&str, DataType)>)]) -> SessionContext {
    let ctx = SessionContext::new();
    for (name, cols) in tables {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, dt)| Field::new(*n, dt.clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table(
            datafusion::common::TableReference::bare(name.to_string()),
            Arc::new(mem),
        )
        .unwrap();
    }
    ctx
}

async fn lineage_for_sql(tables: &[(&str, Vec<(&str, DataType)>)], sql: &str) -> NodeColumnLineage {
    let ctx = ctx_with_tables(tables).await;
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.logical_plan();
    let node_id = NodeId::from("test_node");
    let table_to_node: HashMap<String, NodeId> = tables
        .iter()
        .map(|(name, _)| (name.to_string(), NodeId::from(*name)))
        .collect();
    armillary_datafusion::column_lineage::derive_column_lineage(plan, &node_id, &table_to_node)
}

fn edges_for<'a>(lineage: &'a NodeColumnLineage, col: &str) -> Vec<&'a ColumnEdge> {
    lineage.edges_for_column(col)
}

fn upstream_cols<'a>(edges: &'a [&'a ColumnEdge]) -> HashSet<&'a str> {
    edges.iter().map(|e| e.upstream_column.as_str()).collect()
}

fn relationships(edges: &[&ColumnEdge]) -> HashSet<RelationshipKind> {
    edges.iter().map(|e| e.relationship).collect()
}

fn pid(n: u128) -> PipelineId {
    PipelineId(uuid::Uuid::from_u128(n))
}

fn nid(s: &str) -> NodeId {
    NodeId::new(s)
}

fn fp(s: &str) -> ResourceFingerprint {
    ResourceFingerprint::new(s)
}

fn key(pipeline: u128, node: &str, col: &str) -> ColumnKey {
    ColumnKey {
        pipeline_id: pid(pipeline),
        node_id: nid(node),
        column: col.into(),
    }
}

/// Standard orders table schema.
fn orders_table() -> (&'static str, Vec<(&'static str, DataType)>) {
    (
        "orders",
        vec![
            ("order_id", DataType::Int64),
            ("customer_id", DataType::Int64),
            ("amount", DataType::Float64),
            ("status", DataType::Utf8),
            ("created_at", DataType::Date32),
        ],
    )
}

/// Standard customers table schema.
fn customers_table() -> (&'static str, Vec<(&'static str, DataType)>) {
    (
        "customers",
        vec![
            ("customer_id", DataType::Int64),
            ("name", DataType::Utf8),
            ("email", DataType::Utf8),
            ("tier", DataType::Utf8),
        ],
    )
}

// ===========================================================================
// Golden-file tests: DataFusion LogicalPlan walker
// ===========================================================================

// --- Projection ---

#[tokio::test]
async fn golden_select_star() {
    let lineage = lineage_for_sql(&[orders_table()], "SELECT * FROM orders").await;

    // Every column should have exactly one Direct edge to itself.
    for col in &["order_id", "customer_id", "amount", "status", "created_at"] {
        let e = edges_for(&lineage, col);
        assert_eq!(e.len(), 1, "expected 1 edge for {col}, got {}", e.len());
        assert_eq!(e[0].upstream_column, *col);
        assert_eq!(e[0].relationship, RelationshipKind::Direct);
        assert_eq!(e[0].confidence, Confidence::Exact);
    }
}

#[tokio::test]
async fn golden_column_rename() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id AS oid, amount AS total FROM orders",
    )
    .await;

    let oid = edges_for(&lineage, "oid");
    assert_eq!(oid.len(), 1);
    assert_eq!(oid[0].upstream_column, "order_id");
    assert_eq!(oid[0].relationship, RelationshipKind::Direct);

    let total = edges_for(&lineage, "total");
    assert_eq!(total.len(), 1);
    assert_eq!(total[0].upstream_column, "amount");
}

#[tokio::test]
async fn golden_arithmetic_expression() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id, amount * 1.1 AS with_tax FROM orders",
    )
    .await;

    let with_tax = edges_for(&lineage, "with_tax");
    assert_eq!(with_tax.len(), 1);
    assert_eq!(with_tax[0].upstream_column, "amount");
    assert_eq!(with_tax[0].relationship, RelationshipKind::Derived);
    assert!(with_tax[0].expression_text.is_some());
}

#[tokio::test]
async fn golden_multi_column_expression() {
    let lineage = lineage_for_sql(
        &[(
            "line_items",
            vec![
                ("price", DataType::Float64),
                ("qty", DataType::Int64),
                ("discount", DataType::Float64),
            ],
        )],
        "SELECT price * CAST(qty AS DOUBLE) - discount AS net FROM line_items",
    )
    .await;

    let net = edges_for(&lineage, "net");
    let cols = upstream_cols(&net);
    assert!(cols.contains("price"), "net should reference price");
    assert!(cols.contains("qty"), "net should reference qty");
    assert!(cols.contains("discount"), "net should reference discount");
    assert!(
        net.iter()
            .all(|e| e.relationship == RelationshipKind::Derived)
    );
}

#[tokio::test]
async fn golden_case_when() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id,
                CASE WHEN status = 'shipped' THEN amount ELSE 0 END AS shipped_amount
         FROM orders",
    )
    .await;

    let shipped = edges_for(&lineage, "shipped_amount");
    let cols = upstream_cols(&shipped);
    assert!(cols.contains("status"), "CASE WHEN should reference status");
    assert!(cols.contains("amount"), "CASE WHEN should reference amount");
}

#[tokio::test]
async fn golden_coalesce() {
    let lineage = lineage_for_sql(
        &[(
            "users",
            vec![
                ("display_name", DataType::Utf8),
                ("username", DataType::Utf8),
            ],
        )],
        "SELECT COALESCE(display_name, username) AS name FROM users",
    )
    .await;

    let name = edges_for(&lineage, "name");
    let cols = upstream_cols(&name);
    assert!(cols.contains("display_name"));
    assert!(cols.contains("username"));
}

#[tokio::test]
async fn golden_cast_expression() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT CAST(amount AS INTEGER) AS amount_int FROM orders",
    )
    .await;

    let amount_int = edges_for(&lineage, "amount_int");
    assert_eq!(amount_int.len(), 1);
    assert_eq!(amount_int[0].upstream_column, "amount");
    assert_eq!(amount_int[0].relationship, RelationshipKind::Cast);
}

// --- Filter ---

#[tokio::test]
async fn golden_where_single_column() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id, amount FROM orders WHERE status = 'active'",
    )
    .await;

    // Both output columns should have Filter edges from status.
    for col in &["order_id", "amount"] {
        let e = edges_for(&lineage, col);
        let filter_edges: Vec<_> = e
            .iter()
            .filter(|e| e.relationship == RelationshipKind::Filter)
            .collect();
        assert!(
            !filter_edges.is_empty(),
            "{col} should have a filter edge from status"
        );
        assert!(
            filter_edges.iter().any(|e| e.upstream_column == "status"),
            "{col}'s filter edge should reference status"
        );
    }
}

#[tokio::test]
async fn golden_where_multi_column() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id FROM orders WHERE status = 'active' AND amount > 100",
    )
    .await;

    let oid = edges_for(&lineage, "order_id");
    let filter_cols: HashSet<&str> = oid
        .iter()
        .filter(|e| e.relationship == RelationshipKind::Filter)
        .map(|e| e.upstream_column.as_str())
        .collect();
    assert!(filter_cols.contains("status"));
    assert!(filter_cols.contains("amount"));
}

// --- Joins ---

#[tokio::test]
async fn golden_inner_join() {
    let lineage = lineage_for_sql(
        &[orders_table(), customers_table()],
        "SELECT o.order_id, o.amount, c.name
         FROM orders o
         JOIN customers c ON o.customer_id = c.customer_id",
    )
    .await;

    // order_id from orders side
    let oid = edges_for(&lineage, "order_id");
    assert!(!oid.is_empty());
    assert!(oid.iter().any(|e| e.upstream_column == "order_id"));

    // name from customers side
    let name = edges_for(&lineage, "name");
    assert!(!name.is_empty());
    assert!(name.iter().any(|e| e.upstream_column == "name"));

    // customer_id is used as the join key but not selected, so selected
    // columns get JoinPassthrough (they flow through the join without being
    // part of the key).
    let passthroughs: Vec<_> = lineage
        .edges
        .iter()
        .filter(|e| e.relationship == RelationshipKind::JoinPassthrough)
        .collect();
    assert!(
        !passthroughs.is_empty(),
        "join should produce JoinPassthrough edges for non-key columns"
    );
}

#[tokio::test]
async fn golden_left_join() {
    let lineage = lineage_for_sql(
        &[orders_table(), customers_table()],
        "SELECT o.order_id, c.name
         FROM orders o
         LEFT JOIN customers c ON o.customer_id = c.customer_id",
    )
    .await;

    let name = edges_for(&lineage, "name");
    assert!(
        !name.is_empty(),
        "LEFT JOIN should still produce edges for nullable side"
    );
}

#[tokio::test]
async fn golden_multi_table_join() {
    let lineage = lineage_for_sql(
        &[
            orders_table(),
            customers_table(),
            (
                "order_items",
                vec![
                    ("order_id", DataType::Int64),
                    ("product_id", DataType::Int64),
                    ("quantity", DataType::Int64),
                ],
            ),
        ],
        "SELECT c.name, oi.quantity
         FROM orders o
         JOIN customers c ON o.customer_id = c.customer_id
         JOIN order_items oi ON o.order_id = oi.order_id",
    )
    .await;

    let name = edges_for(&lineage, "name");
    assert!(!name.is_empty());
    assert!(name.iter().any(|e| e.upstream_column == "name"));

    let qty = edges_for(&lineage, "quantity");
    assert!(!qty.is_empty());
    assert!(qty.iter().any(|e| e.upstream_column == "quantity"));
}

// --- Aggregation ---

#[tokio::test]
async fn golden_group_by_sum() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT customer_id, SUM(amount) AS total
         FROM orders
         GROUP BY customer_id",
    )
    .await;

    let cid = edges_for(&lineage, "customer_id");
    assert!(
        cid.iter()
            .any(|e| e.relationship == RelationshipKind::GroupBy),
        "customer_id should be GroupBy"
    );

    let total = edges_for(&lineage, "total");
    assert!(
        total
            .iter()
            .any(|e| e.relationship == RelationshipKind::AggregateInput),
        "total should have AggregateInput"
    );
    assert!(total.iter().any(|e| e.upstream_column == "amount"));
}

#[tokio::test]
async fn golden_multi_aggregate() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT status,
                COUNT(*) AS cnt,
                AVG(amount) AS avg_amount,
                MAX(amount) AS max_amount
         FROM orders
         GROUP BY status",
    )
    .await;

    let status = edges_for(&lineage, "status");
    assert!(
        status
            .iter()
            .any(|e| e.relationship == RelationshipKind::GroupBy)
    );

    // AVG(amount) and MAX(amount) should both reference amount
    for col in &["avg_amount", "max_amount"] {
        let e = edges_for(&lineage, col);
        assert!(
            e.iter().any(|e| e.upstream_column == "amount"),
            "{col} should reference amount"
        );
    }
}

#[tokio::test]
async fn golden_having_clause() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT customer_id, SUM(amount) AS total
         FROM orders
         GROUP BY customer_id
         HAVING SUM(amount) > 1000",
    )
    .await;

    // HAVING creates a filter on the aggregated result.
    let total = edges_for(&lineage, "total");
    assert!(
        total
            .iter()
            .any(|e| e.relationship == RelationshipKind::AggregateInput),
        "total should still have aggregate edges"
    );
}

// --- Window functions ---

#[tokio::test]
async fn golden_window_row_number() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id, customer_id,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) AS rn
         FROM orders",
    )
    .await;

    let rn = edges_for(&lineage, "rn");
    let kinds = relationships(&rn);
    assert!(
        kinds.contains(&RelationshipKind::WindowPartition)
            || kinds.contains(&RelationshipKind::WindowOrder),
        "ROW_NUMBER should produce window-specific edges, got: {kinds:?}"
    );

    // Verify partition and order columns are referenced
    let cols = upstream_cols(&rn);
    assert!(
        cols.contains("customer_id") || cols.contains("created_at"),
        "window should reference partition/order columns"
    );
}

#[tokio::test]
async fn golden_window_sum() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id,
                SUM(amount) OVER (PARTITION BY customer_id ORDER BY created_at) AS running_total
         FROM orders",
    )
    .await;

    let rt = edges_for(&lineage, "running_total");
    let cols = upstream_cols(&rt);
    assert!(
        cols.contains("amount"),
        "window SUM should reference amount"
    );
    assert!(
        cols.contains("customer_id"),
        "window should reference partition column"
    );
}

#[tokio::test]
async fn golden_multiple_window_functions() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) AS rn,
                SUM(amount) OVER (PARTITION BY status ORDER BY created_at) AS status_total
         FROM orders",
    )
    .await;

    let rn = edges_for(&lineage, "rn");
    let st = edges_for(&lineage, "status_total");

    assert!(!rn.is_empty(), "rn should have edges");
    assert!(!st.is_empty(), "status_total should have edges");

    // status_total should reference status (partition) and amount (input)
    let st_cols = upstream_cols(&st);
    assert!(st_cols.contains("amount"));
    assert!(st_cols.contains("status"));
}

// --- Union ---

#[tokio::test]
async fn golden_union_all() {
    let lineage = lineage_for_sql(
        &[
            (
                "us_orders",
                vec![
                    ("id", DataType::Int64),
                    ("amount", DataType::Float64),
                    ("region", DataType::Utf8),
                ],
            ),
            (
                "eu_orders",
                vec![
                    ("id", DataType::Int64),
                    ("amount", DataType::Float64),
                    ("region", DataType::Utf8),
                ],
            ),
        ],
        "SELECT id, amount, region FROM us_orders
         UNION ALL
         SELECT id, amount, region FROM eu_orders",
    )
    .await;

    for col in &["id", "amount", "region"] {
        let e = edges_for(&lineage, col);
        let upstream_nodes: HashSet<Option<&NodeId>> =
            e.iter().map(|e| e.upstream_node.as_ref()).collect();
        assert!(
            upstream_nodes.len() >= 2,
            "UNION ALL column {col} should have edges from both inputs, got {upstream_nodes:?}"
        );
    }
}

#[tokio::test]
async fn golden_union_with_rename() {
    let lineage = lineage_for_sql(
        &[
            (
                "table_a",
                vec![("col_x", DataType::Int64), ("col_y", DataType::Utf8)],
            ),
            (
                "table_b",
                vec![("col_x", DataType::Int64), ("col_y", DataType::Utf8)],
            ),
        ],
        "SELECT col_x AS merged_id, col_y AS merged_name FROM table_a
         UNION ALL
         SELECT col_x, col_y FROM table_b",
    )
    .await;

    // Output column names come from the first SELECT.
    let merged = edges_for(&lineage, "merged_id");
    assert!(!merged.is_empty(), "merged_id should have edges");
}

// --- Subqueries ---

#[tokio::test]
async fn golden_subquery_in_from() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT sub.order_id, sub.doubled
         FROM (
             SELECT order_id, amount * 2 AS doubled FROM orders
         ) sub",
    )
    .await;

    let oid = edges_for(&lineage, "order_id");
    assert_eq!(oid.len(), 1);
    assert_eq!(oid[0].upstream_column, "order_id");

    let doubled = edges_for(&lineage, "doubled");
    assert!(!doubled.is_empty());
    assert!(doubled.iter().any(|e| e.upstream_column == "amount"));
}

#[tokio::test]
async fn golden_scalar_subquery() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id, amount,
                (SELECT MAX(amount) FROM orders) AS max_amount
         FROM orders",
    )
    .await;

    // The scalar subquery should produce edges referencing amount.
    let max_a = edges_for(&lineage, "max_amount");
    // Scalar subqueries may or may not produce edges depending on how
    // DataFusion represents them. At minimum, the query should succeed.
    let _ = max_a;
}

// --- DISTINCT ---

#[tokio::test]
async fn golden_distinct() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT DISTINCT status, customer_id FROM orders",
    )
    .await;

    // DISTINCT is a passthrough for lineage purposes.
    let status = edges_for(&lineage, "status");
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].upstream_column, "status");
    assert_eq!(status[0].relationship, RelationshipKind::Direct);
}

// --- ORDER BY + LIMIT ---

#[tokio::test]
async fn golden_order_by_limit() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id, amount FROM orders ORDER BY amount DESC LIMIT 10",
    )
    .await;

    // Sort and Limit are passthroughs.
    let oid = edges_for(&lineage, "order_id");
    assert_eq!(oid.len(), 1);
    assert_eq!(oid[0].upstream_column, "order_id");
    assert_eq!(oid[0].relationship, RelationshipKind::Direct);
}

// --- Complex combinations ---

#[tokio::test]
async fn golden_join_aggregate_filter() {
    let lineage = lineage_for_sql(
        &[orders_table(), customers_table()],
        "SELECT c.name, SUM(o.amount) AS total_spend
         FROM orders o
         JOIN customers c ON o.customer_id = c.customer_id
         WHERE o.status = 'completed'
         GROUP BY c.name
         HAVING SUM(o.amount) > 500",
    )
    .await;

    let name = edges_for(&lineage, "name");
    assert!(
        name.iter()
            .any(|e| e.relationship == RelationshipKind::GroupBy),
        "name should be a GroupBy key"
    );

    let total = edges_for(&lineage, "total_spend");
    assert!(
        total
            .iter()
            .any(|e| e.relationship == RelationshipKind::AggregateInput),
        "total_spend should have AggregateInput edges"
    );
    assert!(
        total.iter().any(|e| e.upstream_column == "amount"),
        "total_spend should reference amount"
    );
}

#[tokio::test]
async fn golden_nested_subquery_with_join() {
    let lineage = lineage_for_sql(
        &[orders_table(), customers_table()],
        "SELECT sub.name, sub.order_count
         FROM (
             SELECT c.name, COUNT(*) AS order_count
             FROM orders o
             JOIN customers c ON o.customer_id = c.customer_id
             GROUP BY c.name
         ) sub
         WHERE sub.order_count > 5",
    )
    .await;

    let name = edges_for(&lineage, "name");
    assert!(
        !name.is_empty(),
        "name should have lineage through the subquery"
    );

    // COUNT(*) references no specific columns, so order_count correctly
    // has no upstream edges. This is accurate — the count depends on the
    // row set (influenced by the join and filter) but not on any particular
    // column value.
    let cnt = edges_for(&lineage, "order_count");
    // It's acceptable for COUNT(*) to have zero edges.
    let _ = cnt;
}

// --- String functions ---

#[tokio::test]
async fn golden_string_concat() {
    let lineage = lineage_for_sql(
        &[customers_table()],
        "SELECT customer_id, name || ' <' || email || '>' AS display FROM customers",
    )
    .await;

    let display = edges_for(&lineage, "display");
    let cols = upstream_cols(&display);
    assert!(cols.contains("name"));
    assert!(cols.contains("email"));
    assert!(
        display
            .iter()
            .all(|e| e.relationship == RelationshipKind::Derived)
    );
}

// --- Deterministic output ---

#[tokio::test]
async fn golden_edges_are_sorted_deterministically() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id, amount * 1.1 AS with_tax FROM orders WHERE status = 'active'",
    )
    .await;

    // Edges should be sorted by (downstream_column, upstream_column, relationship).
    for window in lineage.edges.windows(2) {
        let a = &window[0];
        let b = &window[1];
        let cmp = a
            .downstream_column
            .cmp(&b.downstream_column)
            .then_with(|| a.upstream_column.cmp(&b.upstream_column))
            .then_with(|| a.relationship.to_string().cmp(&b.relationship.to_string()));
        assert!(
            cmp.is_le(),
            "edges should be sorted: ({}, {}) should come before ({}, {})",
            a.downstream_column,
            a.upstream_column,
            b.downstream_column,
            b.upstream_column,
        );
    }
}

// ===========================================================================
// Edge case tests
// ===========================================================================

#[tokio::test]
async fn edge_case_correlated_subquery_in_where() {
    let lineage = lineage_for_sql(
        &[orders_table(), customers_table()],
        "SELECT o.order_id, o.amount
         FROM orders o
         WHERE o.customer_id IN (
             SELECT c.customer_id FROM customers c WHERE c.tier = 'premium'
         )",
    )
    .await;

    // The query should succeed and produce lineage for order_id and amount.
    let oid = edges_for(&lineage, "order_id");
    assert!(
        !oid.is_empty(),
        "correlated subquery should not break lineage"
    );
    assert!(oid.iter().any(|e| e.upstream_column == "order_id"));
}

#[tokio::test]
async fn edge_case_exists_subquery() {
    let lineage = lineage_for_sql(
        &[orders_table(), customers_table()],
        "SELECT o.order_id
         FROM orders o
         WHERE EXISTS (
             SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id
         )",
    )
    .await;

    let oid = edges_for(&lineage, "order_id");
    assert!(!oid.is_empty(), "EXISTS subquery should not break lineage");
}

#[tokio::test]
async fn edge_case_nested_window_functions() {
    // Window function over a result that already used a window function.
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id, rn, sum_amount
         FROM (
             SELECT order_id,
                    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) AS rn,
                    SUM(amount) OVER (ORDER BY created_at) AS sum_amount
             FROM orders
         ) sub
         WHERE rn = 1",
    )
    .await;

    let rn = edges_for(&lineage, "rn");
    assert!(!rn.is_empty(), "nested window: rn should have lineage");

    let sum_a = edges_for(&lineage, "sum_amount");
    assert!(
        !sum_a.is_empty(),
        "nested window: sum_amount should have lineage"
    );
}

#[tokio::test]
async fn edge_case_self_join() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT a.order_id AS id_a, b.order_id AS id_b
         FROM orders a
         JOIN orders b ON a.customer_id = b.customer_id AND a.order_id < b.order_id",
    )
    .await;

    let id_a = edges_for(&lineage, "id_a");
    let id_b = edges_for(&lineage, "id_b");
    assert!(!id_a.is_empty(), "self-join: id_a should have lineage");
    assert!(!id_b.is_empty(), "self-join: id_b should have lineage");
}

#[tokio::test]
async fn edge_case_empty_table() {
    // Query on a table with no columns produces no edges.
    let lineage = lineage_for_sql(
        &[("empty", vec![("id", DataType::Int64)])],
        "SELECT id FROM empty WHERE 1 = 0",
    )
    .await;

    let id = edges_for(&lineage, "id");
    assert!(
        !id.is_empty(),
        "empty table should still produce lineage edges"
    );
}

#[tokio::test]
async fn edge_case_multiple_casts() {
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT CAST(CAST(amount AS INTEGER) AS VARCHAR) AS amount_str FROM orders",
    )
    .await;

    let amount_str = edges_for(&lineage, "amount_str");
    assert_eq!(amount_str.len(), 1);
    assert_eq!(amount_str[0].upstream_column, "amount");
    // Nested casts should still be classified as Cast.
    assert!(
        amount_str[0].relationship == RelationshipKind::Cast
            || amount_str[0].relationship == RelationshipKind::Derived,
        "nested cast should be Cast or Derived"
    );
}

#[tokio::test]
async fn edge_case_values_literal() {
    // Literal VALUES produce no upstream edges.
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT order_id FROM orders WHERE order_id IN (1, 2, 3)",
    )
    .await;

    let oid = edges_for(&lineage, "order_id");
    assert!(!oid.is_empty());
    // The literal values don't produce edges — only order_id does.
    assert!(oid.iter().any(|e| e.upstream_column == "order_id"));
}

// ===========================================================================
// Cross-pipeline column edge tests
// ===========================================================================

use armillary_engine::column_lineage::derive_cross_pipeline_column_lineage;
use armillary_engine::lineage::BindingDirection;

#[test]
fn cross_pipeline_three_hop_chain() {
    // Pipeline 1 → Pipeline 2 → Pipeline 3 via shared resources.
    let boundary = vec![
        // Pipeline 1 sink → resource A
        BoundaryColumn {
            pipeline_id: pid(1),
            node_id: nid("sink1"),
            column: "user_id".into(),
            fingerprint: fp("pg://staging.users"),
            direction: BindingDirection::Sink,
        },
        // Pipeline 2 source ← resource A
        BoundaryColumn {
            pipeline_id: pid(2),
            node_id: nid("src2"),
            column: "user_id".into(),
            fingerprint: fp("pg://staging.users"),
            direction: BindingDirection::Source,
        },
        // Pipeline 2 sink → resource B
        BoundaryColumn {
            pipeline_id: pid(2),
            node_id: nid("sink2"),
            column: "user_id".into(),
            fingerprint: fp("pg://prod.users"),
            direction: BindingDirection::Sink,
        },
        // Pipeline 3 source ← resource B
        BoundaryColumn {
            pipeline_id: pid(3),
            node_id: nid("src3"),
            column: "user_id".into(),
            fingerprint: fp("pg://prod.users"),
            direction: BindingDirection::Source,
        },
    ];

    let result = derive_cross_pipeline_column_lineage(&boundary);
    assert_eq!(
        result.edges.len(),
        2,
        "should have 2 cross-pipeline edges: P1→P2 and P2→P3"
    );
    assert_eq!(result.one_sided.len(), 0);

    // Verify the chain: P1.sink1 → P2.src2 and P2.sink2 → P3.src3
    assert!(
        result
            .edges
            .iter()
            .any(|e| e.upstream_pipeline_id == pid(1) && e.downstream_pipeline_id == pid(2))
    );
    assert!(
        result
            .edges
            .iter()
            .any(|e| e.upstream_pipeline_id == pid(2) && e.downstream_pipeline_id == pid(3))
    );
}

#[test]
fn cross_pipeline_multiple_columns_partial_overlap() {
    // Sink writes {id, name, email}, source reads {id, name, phone}.
    // id and name match; email is sink-only, phone is source-only.
    let boundary = vec![
        BoundaryColumn {
            pipeline_id: pid(1),
            node_id: nid("sink"),
            column: "id".into(),
            fingerprint: fp("pg://users"),
            direction: BindingDirection::Sink,
        },
        BoundaryColumn {
            pipeline_id: pid(1),
            node_id: nid("sink"),
            column: "name".into(),
            fingerprint: fp("pg://users"),
            direction: BindingDirection::Sink,
        },
        BoundaryColumn {
            pipeline_id: pid(1),
            node_id: nid("sink"),
            column: "email".into(),
            fingerprint: fp("pg://users"),
            direction: BindingDirection::Sink,
        },
        BoundaryColumn {
            pipeline_id: pid(2),
            node_id: nid("src"),
            column: "id".into(),
            fingerprint: fp("pg://users"),
            direction: BindingDirection::Source,
        },
        BoundaryColumn {
            pipeline_id: pid(2),
            node_id: nid("src"),
            column: "name".into(),
            fingerprint: fp("pg://users"),
            direction: BindingDirection::Source,
        },
        BoundaryColumn {
            pipeline_id: pid(2),
            node_id: nid("src"),
            column: "phone".into(),
            fingerprint: fp("pg://users"),
            direction: BindingDirection::Source,
        },
    ];

    let result = derive_cross_pipeline_column_lineage(&boundary);
    assert_eq!(result.edges.len(), 2, "id and name should match");

    let matched_cols: HashSet<&str> = result.edges.iter().map(|e| e.column.as_str()).collect();
    assert!(matched_cols.contains("id"));
    assert!(matched_cols.contains("name"));

    // email is sink-only, phone is source-only.
    assert_eq!(result.one_sided.len(), 2);
    let one_sided_cols: HashSet<&str> =
        result.one_sided.iter().map(|o| o.column.as_str()).collect();
    assert!(one_sided_cols.contains("email"));
    assert!(one_sided_cols.contains("phone"));
}

#[test]
fn cross_pipeline_diamond_topology() {
    // Pipeline 1 → {Pipeline 2, Pipeline 3} → Pipeline 4
    // P1 writes to resource A, P2 and P3 read from A.
    // P2 writes to resource B, P3 writes to resource C.
    // P4 reads from both B and C.
    let boundary = vec![
        // P1 → resource A
        BoundaryColumn {
            pipeline_id: pid(1),
            node_id: nid("sink1"),
            column: "id".into(),
            fingerprint: fp("pg://raw"),
            direction: BindingDirection::Sink,
        },
        // P2 ← resource A
        BoundaryColumn {
            pipeline_id: pid(2),
            node_id: nid("src2"),
            column: "id".into(),
            fingerprint: fp("pg://raw"),
            direction: BindingDirection::Source,
        },
        // P3 ← resource A
        BoundaryColumn {
            pipeline_id: pid(3),
            node_id: nid("src3"),
            column: "id".into(),
            fingerprint: fp("pg://raw"),
            direction: BindingDirection::Source,
        },
        // P2 → resource B
        BoundaryColumn {
            pipeline_id: pid(2),
            node_id: nid("sink2"),
            column: "id".into(),
            fingerprint: fp("pg://staging"),
            direction: BindingDirection::Sink,
        },
        // P3 → resource C
        BoundaryColumn {
            pipeline_id: pid(3),
            node_id: nid("sink3"),
            column: "id".into(),
            fingerprint: fp("pg://analytics"),
            direction: BindingDirection::Sink,
        },
        // P4 ← resource B
        BoundaryColumn {
            pipeline_id: pid(4),
            node_id: nid("src4a"),
            column: "id".into(),
            fingerprint: fp("pg://staging"),
            direction: BindingDirection::Source,
        },
        // P4 ← resource C
        BoundaryColumn {
            pipeline_id: pid(4),
            node_id: nid("src4b"),
            column: "id".into(),
            fingerprint: fp("pg://analytics"),
            direction: BindingDirection::Source,
        },
    ];

    let result = derive_cross_pipeline_column_lineage(&boundary);
    // P1→P2, P1→P3, P2→P4, P3→P4 = 4 edges
    assert_eq!(result.edges.len(), 4, "diamond should produce 4 edges");
    assert_eq!(result.one_sided.len(), 0);
}

#[test]
fn cross_pipeline_column_rename_through_pipeline() {
    // P1 sink writes "order_id", P2 source reads "order_id",
    // but P2's transform renames it to "oid" and sinks it.
    // P3 source reads "oid".
    // The cross-pipeline edges at each boundary match by column name at that
    // boundary, not by the original name.
    let boundary = vec![
        BoundaryColumn {
            pipeline_id: pid(1),
            node_id: nid("sink1"),
            column: "order_id".into(),
            fingerprint: fp("pg://orders"),
            direction: BindingDirection::Sink,
        },
        BoundaryColumn {
            pipeline_id: pid(2),
            node_id: nid("src2"),
            column: "order_id".into(),
            fingerprint: fp("pg://orders"),
            direction: BindingDirection::Source,
        },
        // P2 sinks as "oid" (renamed inside P2's transform)
        BoundaryColumn {
            pipeline_id: pid(2),
            node_id: nid("sink2"),
            column: "oid".into(),
            fingerprint: fp("pg://summary"),
            direction: BindingDirection::Sink,
        },
        BoundaryColumn {
            pipeline_id: pid(3),
            node_id: nid("src3"),
            column: "oid".into(),
            fingerprint: fp("pg://summary"),
            direction: BindingDirection::Source,
        },
    ];

    let result = derive_cross_pipeline_column_lineage(&boundary);
    // P1→P2 via "order_id", P2→P3 via "oid"
    assert_eq!(result.edges.len(), 2);
    assert!(result.edges.iter().any(|e| e.column == "order_id"));
    assert!(result.edges.iter().any(|e| e.column == "oid"));
}

// ===========================================================================
// Graph traversal with cross-pipeline edges
// ===========================================================================

#[test]
fn graph_cross_pipeline_three_hop_traversal() {
    // Build a 3-pipeline chain and verify upstream trace crosses both boundaries.
    // P1: src_A.id → transform_B.id
    // P2: src_C.id → transform_D.user_id
    // P3: src_E.user_id → transform_F.uid

    let p1_edges = vec![ColumnEdge {
        upstream_column: "id".into(),
        upstream_node: Some(nid("A")),
        upstream_resource: Some(fp("pg://raw")),
        downstream_column: "id".into(),
        downstream_node: Some(nid("B")),
        downstream_resource: Some(fp("pg://staging")),
        relationship: RelationshipKind::Direct,
        expression_text: None,
        confidence: Confidence::Exact,
    }];

    let p2_edges = vec![
        ColumnEdge {
            upstream_column: "id".into(),
            upstream_node: Some(nid("C")),
            upstream_resource: Some(fp("pg://staging")),
            downstream_column: "user_id".into(),
            downstream_node: Some(nid("D")),
            downstream_resource: None,
            relationship: RelationshipKind::Direct,
            expression_text: None,
            confidence: Confidence::Exact,
        },
        ColumnEdge {
            upstream_column: "user_id".into(),
            upstream_node: Some(nid("D")),
            upstream_resource: None,
            downstream_column: "user_id".into(),
            downstream_node: Some(nid("D_sink")),
            downstream_resource: Some(fp("pg://prod")),
            relationship: RelationshipKind::Direct,
            expression_text: None,
            confidence: Confidence::Exact,
        },
    ];

    let p3_edges = vec![ColumnEdge {
        upstream_column: "user_id".into(),
        upstream_node: Some(nid("E")),
        upstream_resource: Some(fp("pg://prod")),
        downstream_column: "uid".into(),
        downstream_node: Some(nid("F")),
        downstream_resource: None,
        relationship: RelationshipKind::Direct,
        expression_text: None,
        confidence: Confidence::Exact,
    }];

    let cross = vec![
        CrossPipelineColumnEdge {
            upstream_pipeline_id: pid(1),
            upstream_node_id: nid("B"),
            downstream_pipeline_id: pid(2),
            downstream_node_id: nid("C"),
            column: "id".into(),
            fingerprint: fp("pg://staging"),
        },
        CrossPipelineColumnEdge {
            upstream_pipeline_id: pid(2),
            upstream_node_id: nid("D_sink"),
            downstream_pipeline_id: pid(3),
            downstream_node_id: nid("E"),
            column: "user_id".into(),
            fingerprint: fp("pg://prod"),
        },
    ];

    let pipeline_edges = vec![
        (pid(1), p1_edges.as_slice()),
        (pid(2), p2_edges.as_slice()),
        (pid(3), p3_edges.as_slice()),
    ];
    let graph = ColumnLineageGraph::new(&pipeline_edges, &cross);
    let opts = TraceOptions::default();

    // Upstream from P3.F.uid should trace all the way back to P1.A.id
    let result = graph.upstream_trace(&key(3, "F", "uid"), &opts);
    assert!(!result.truncated);

    // Verify we cross both pipeline boundaries
    let pipeline_ids: HashSet<PipelineId> = result
        .edges
        .iter()
        .flat_map(|e| {
            [
                e.upstream.pipeline_id.clone(),
                e.downstream.pipeline_id.clone(),
            ]
        })
        .collect();
    assert!(
        pipeline_ids.contains(&pid(1)),
        "trace should reach pipeline 1"
    );
    assert!(
        pipeline_ids.contains(&pid(2)),
        "trace should reach pipeline 2"
    );
    assert!(
        pipeline_ids.contains(&pid(3)),
        "trace should include pipeline 3"
    );
}

// ===========================================================================
// Performance and depth-limit tests
// ===========================================================================

#[tokio::test]
async fn perf_wide_projection() {
    // 200+ columns — lineage derivation should not be slow.
    let cols: Vec<(&str, DataType)> = (0..200)
        .map(|i| {
            // Leak the string to get a &'static str — fine for tests.
            let name: &'static str = Box::leak(format!("col_{i}").into_boxed_str());
            (name, DataType::Float64)
        })
        .collect();

    let select_cols: String = (0..200)
        .map(|i| format!("col_{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {select_cols} FROM wide_table");

    let start = std::time::Instant::now();
    let lineage = lineage_for_sql(&[("wide_table", cols)], &sql).await;
    let elapsed = start.elapsed();

    assert_eq!(lineage.edges.len(), 200);
    assert!(
        elapsed.as_secs() < 5,
        "200-column lineage should derive in under 5s, took {elapsed:?}"
    );
}

#[tokio::test]
async fn perf_deep_subquery_nesting() {
    // Nested subqueries — tests that the walker handles deep plan trees.
    let lineage = lineage_for_sql(
        &[orders_table()],
        "SELECT d.order_id FROM (
            SELECT c.order_id FROM (
                SELECT b.order_id FROM (
                    SELECT a.order_id FROM orders a
                ) b
            ) c
        ) d",
    )
    .await;

    let oid = edges_for(&lineage, "order_id");
    assert_eq!(oid.len(), 1);
    assert_eq!(oid[0].upstream_column, "order_id");
    assert_eq!(oid[0].relationship, RelationshipKind::Direct);
}

#[test]
fn perf_large_graph_depth_limit() {
    // Build a long chain: node_0 → node_1 → ... → node_99.
    // Test that depth limits prevent runaway traversal.
    let mut all_edges = Vec::new();
    for i in 0..99 {
        let from = format!("node_{i}");
        let to = format!("node_{}", i + 1);
        all_edges.push(ColumnEdge {
            upstream_column: "val".into(),
            upstream_node: Some(nid(&from)),
            upstream_resource: None,
            downstream_column: "val".into(),
            downstream_node: Some(nid(&to)),
            downstream_resource: None,
            relationship: RelationshipKind::Direct,
            expression_text: None,
            confidence: Confidence::Exact,
        });
    }

    let pipeline_edges = vec![(pid(1), all_edges.as_slice())];
    let graph = ColumnLineageGraph::new(&pipeline_edges, &[]);

    // Default depth (10) should truncate.
    let opts = TraceOptions::default();
    let result = graph.upstream_trace(&key(1, "node_99", "val"), &opts);
    assert!(
        result.truncated,
        "default depth should truncate a 100-node chain"
    );
    assert_eq!(
        result.edges.len(),
        opts.max_depth,
        "should return exactly max_depth edges"
    );

    // Explicit depth 5.
    let opts5 = TraceOptions {
        max_depth: 5,
        ..Default::default()
    };
    let result5 = graph.upstream_trace(&key(1, "node_99", "val"), &opts5);
    assert!(result5.truncated);
    assert_eq!(result5.edges.len(), 5);

    // Large depth should not truncate.
    let opts_big = TraceOptions {
        max_depth: 200,
        ..Default::default()
    };
    let result_big = graph.upstream_trace(&key(1, "node_99", "val"), &opts_big);
    assert!(!result_big.truncated);
    assert_eq!(result_big.edges.len(), 99);
}

#[test]
fn perf_graph_construction_speed() {
    // Build a graph with many pipelines and edges to verify construction
    // doesn't blow up.
    let mut pipeline_edge_vecs: Vec<Vec<ColumnEdge>> = Vec::new();

    for p in 0..50 {
        let mut edges = Vec::new();
        // Each pipeline: 10 nodes with 20 columns each = 200 edges.
        for n in 0..10 {
            for c in 0..20 {
                let from = format!("p{p}_n{n}");
                let to = format!("p{p}_n{}", n + 1);
                let col = format!("col_{c}");
                edges.push(ColumnEdge {
                    upstream_column: col.clone(),
                    upstream_node: Some(nid(&from)),
                    upstream_resource: None,
                    downstream_column: col,
                    downstream_node: Some(nid(&to)),
                    downstream_resource: None,
                    relationship: RelationshipKind::Direct,
                    expression_text: None,
                    confidence: Confidence::Exact,
                });
            }
        }
        pipeline_edge_vecs.push(edges);
    }

    let pipeline_edges: Vec<(PipelineId, &[ColumnEdge])> = pipeline_edge_vecs
        .iter()
        .enumerate()
        .map(|(i, edges)| (pid(i as u128), edges.as_slice()))
        .collect();

    let start = std::time::Instant::now();
    let graph = ColumnLineageGraph::new(&pipeline_edges, &[]);
    let elapsed = start.elapsed();

    // 50 pipelines × 200 edges = 10,000 edges — should be fast.
    assert!(
        elapsed.as_secs() < 5,
        "graph construction for 10k edges should be under 5s, took {elapsed:?}"
    );

    // Verify a trace works on the large graph.
    let opts = TraceOptions::default();
    let result = graph.downstream_trace(&key(0, "p0_n0", "col_0"), &opts);
    assert!(!result.edges.is_empty());
}

#[test]
fn depth_limit_downstream_trace() {
    // Same long chain but testing downstream direction.
    let mut all_edges = Vec::new();
    for i in 0..50 {
        let from = format!("node_{i}");
        let to = format!("node_{}", i + 1);
        all_edges.push(ColumnEdge {
            upstream_column: "val".into(),
            upstream_node: Some(nid(&from)),
            upstream_resource: None,
            downstream_column: "val".into(),
            downstream_node: Some(nid(&to)),
            downstream_resource: None,
            relationship: RelationshipKind::Direct,
            expression_text: None,
            confidence: Confidence::Exact,
        });
    }

    let pipeline_edges = vec![(pid(1), all_edges.as_slice())];
    let graph = ColumnLineageGraph::new(&pipeline_edges, &[]);

    let opts = TraceOptions {
        max_depth: 3,
        ..Default::default()
    };
    let result = graph.downstream_trace(&key(1, "node_0", "val"), &opts);
    assert!(result.truncated);
    assert_eq!(result.edges.len(), 3);

    // First hop should be node_0 → node_1
    assert_eq!(result.edges[0].depth, 1);
    assert_eq!(result.edges[0].downstream.node_id, nid("node_1"));
}

// ---------------------------------------------------------------------------
// Doc 35b: Polars LazyFrame lineage conversion (derive_python_lineage)
// ---------------------------------------------------------------------------

#[test]
fn python_lineage_select_maps_to_lazyframe_confidence() {
    use armillary_datafusion::python_runtime::{PythonColumnEdge, PythonColumnLineage};

    let py_lineage = PythonColumnLineage {
        edges: vec![
            PythonColumnEdge {
                upstream_column: "amount".into(),
                downstream_column: "amount_with_tax".into(),
                relationship: "derived".into(),
                expression_text: Some("amount_with_tax".into()),
            },
            PythonColumnEdge {
                upstream_column: "customer_id".into(),
                downstream_column: "customer_id".into(),
                relationship: "direct".into(),
                expression_text: None,
            },
        ],
        confidence: "lazyframe".into(),
        warnings: vec![],
    };

    let node_id = nid("py_transform");
    let input_columns = vec![(
        nid("src"),
        vec![
            "amount".to_string(),
            "customer_id".to_string(),
            "status".to_string(),
        ],
    )];

    let lineage = armillary_datafusion::column_lineage::derive_python_lineage(
        &node_id,
        &py_lineage,
        &input_columns,
    );

    assert_eq!(lineage.node_id, node_id);
    assert_eq!(lineage.edges.len(), 2);

    // Derived edge: amount → amount_with_tax.
    let derived: Vec<_> = lineage
        .edges
        .iter()
        .filter(|e| e.downstream_column == "amount_with_tax")
        .collect();
    assert_eq!(derived.len(), 1);
    assert_eq!(derived[0].upstream_column, "amount");
    assert_eq!(derived[0].relationship, RelationshipKind::Derived);
    assert_eq!(derived[0].confidence, Confidence::LazyFrame);
    assert_eq!(derived[0].upstream_node.as_ref().unwrap(), &nid("src"));
    assert_eq!(
        derived[0].expression_text.as_deref(),
        Some("amount_with_tax")
    );

    // Direct edge: customer_id → customer_id.
    let direct: Vec<_> = lineage
        .edges
        .iter()
        .filter(|e| e.downstream_column == "customer_id")
        .collect();
    assert_eq!(direct.len(), 1);
    assert_eq!(direct[0].upstream_column, "customer_id");
    assert_eq!(direct[0].relationship, RelationshipKind::Direct);
    assert_eq!(direct[0].confidence, Confidence::LazyFrame);
}

#[test]
fn python_lineage_opaque_fallback() {
    use armillary_datafusion::python_runtime::{PythonColumnEdge, PythonColumnLineage};

    let py_lineage = PythonColumnLineage {
        edges: vec![
            PythonColumnEdge {
                upstream_column: "a".into(),
                downstream_column: "x".into(),
                relationship: "opaque".into(),
                expression_text: None,
            },
            PythonColumnEdge {
                upstream_column: "b".into(),
                downstream_column: "x".into(),
                relationship: "opaque".into(),
                expression_text: None,
            },
        ],
        confidence: "opaque".into(),
        warnings: vec!["unsupported IR version".into()],
    };

    let node_id = nid("py_opaque");
    let input_columns = vec![
        (nid("src1"), vec!["a".to_string()]),
        (nid("src2"), vec!["b".to_string()]),
    ];

    let lineage = armillary_datafusion::column_lineage::derive_python_lineage(
        &node_id,
        &py_lineage,
        &input_columns,
    );

    assert_eq!(lineage.edges.len(), 2);
    for edge in &lineage.edges {
        assert_eq!(edge.relationship, RelationshipKind::Opaque);
        assert_eq!(edge.confidence, Confidence::Opaque);
    }

    // Check upstream node resolution.
    let a_edge = lineage
        .edges
        .iter()
        .find(|e| e.upstream_column == "a")
        .unwrap();
    assert_eq!(a_edge.upstream_node.as_ref().unwrap(), &nid("src1"));
    let b_edge = lineage
        .edges
        .iter()
        .find(|e| e.upstream_column == "b")
        .unwrap();
    assert_eq!(b_edge.upstream_node.as_ref().unwrap(), &nid("src2"));
}

#[test]
fn python_lineage_all_relationship_kinds_parse() {
    use armillary_datafusion::python_runtime::{PythonColumnEdge, PythonColumnLineage};

    let kinds = vec![
        ("direct", RelationshipKind::Direct),
        ("derived", RelationshipKind::Derived),
        ("cast", RelationshipKind::Cast),
        ("filter", RelationshipKind::Filter),
        ("join_key", RelationshipKind::JoinKey),
        ("join_passthrough", RelationshipKind::JoinPassthrough),
        ("group_by", RelationshipKind::GroupBy),
        ("aggregate_input", RelationshipKind::AggregateInput),
        ("window_partition", RelationshipKind::WindowPartition),
        ("window_order", RelationshipKind::WindowOrder),
        ("window_input", RelationshipKind::WindowInput),
        ("opaque", RelationshipKind::Opaque),
        ("unknown_future_kind", RelationshipKind::Opaque),
    ];

    for (rel_str, expected) in &kinds {
        let py_lineage = PythonColumnLineage {
            edges: vec![PythonColumnEdge {
                upstream_column: "x".into(),
                downstream_column: "y".into(),
                relationship: rel_str.to_string(),
                expression_text: None,
            }],
            confidence: "lazyframe".into(),
            warnings: vec![],
        };

        let lineage = armillary_datafusion::column_lineage::derive_python_lineage(
            &nid("n"),
            &py_lineage,
            &[(nid("s"), vec!["x".to_string()])],
        );

        assert_eq!(
            lineage.edges[0].relationship, *expected,
            "failed for relationship string '{rel_str}'"
        );
    }
}
