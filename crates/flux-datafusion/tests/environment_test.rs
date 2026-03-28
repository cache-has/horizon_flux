// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Tests for the environment data model, SQLite store, and catalog resolver.

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use flux_datafusion::environment::EnvironmentStore;
use flux_datafusion::resolver::EnvironmentResolver;
use std::sync::Arc;

// ── EnvironmentStore tests ───────────────────────────────────────────────────

#[test]
fn default_environments_exist() {
    let store = EnvironmentStore::open_in_memory().unwrap();
    let envs = store.list().unwrap();
    let names: Vec<&str> = envs.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"prod"));
    assert!(names.contains(&"dev"));

    let prod = store.get("prod").unwrap().unwrap();
    assert_eq!(prod.fallback, None);

    let dev = store.get("dev").unwrap().unwrap();
    assert_eq!(dev.fallback, Some("prod".to_string()));
}

#[test]
fn create_and_delete_environment() {
    let store = EnvironmentStore::open_in_memory().unwrap();
    store.create("staging", Some("prod")).unwrap();

    let staging = store.get("staging").unwrap().unwrap();
    assert_eq!(staging.fallback, Some("prod".to_string()));

    store.delete("staging").unwrap();
    assert!(store.get("staging").unwrap().is_none());
}

#[test]
fn cannot_delete_prod() {
    let store = EnvironmentStore::open_in_memory().unwrap();
    let err = store.delete("prod").unwrap_err();
    assert!(err.to_string().contains("prod"));
}

#[test]
fn cannot_create_duplicate() {
    let store = EnvironmentStore::open_in_memory().unwrap();
    let err = store.create("prod", None).unwrap_err();
    assert!(err.to_string().contains("already exists"));
}

#[test]
fn fallback_must_exist() {
    let store = EnvironmentStore::open_in_memory().unwrap();
    let err = store.create("staging", Some("nonexistent")).unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[test]
fn fallback_chain() {
    let store = EnvironmentStore::open_in_memory().unwrap();
    store.create("staging", Some("prod")).unwrap();
    store.create("feature", Some("staging")).unwrap();

    let chain = store.fallback_chain("feature").unwrap();
    assert_eq!(chain, vec!["feature", "staging", "prod"]);
}

#[test]
fn delete_repoints_dependents() {
    let store = EnvironmentStore::open_in_memory().unwrap();
    store.create("staging", Some("prod")).unwrap();
    store.create("feature", Some("staging")).unwrap();

    store.delete("staging").unwrap();

    // feature should now fall back to prod directly
    let feature = store.get("feature").unwrap().unwrap();
    assert_eq!(feature.fallback, Some("prod".to_string()));
}

#[test]
fn update_fallback() {
    let store = EnvironmentStore::open_in_memory().unwrap();
    store.create("staging", Some("prod")).unwrap();

    store.update_fallback("dev", Some("staging")).unwrap();
    let chain = store.fallback_chain("dev").unwrap();
    assert_eq!(chain, vec!["dev", "staging", "prod"]);
}

#[test]
fn table_override_crud() {
    let store = EnvironmentStore::open_in_memory().unwrap();

    store
        .register_table_override("dev", "public", "orders")
        .unwrap();
    store
        .register_table_override("dev", "public", "users")
        .unwrap();

    let overrides = store.list_table_overrides("dev").unwrap();
    assert_eq!(overrides.len(), 2);

    let removed = store
        .deregister_table_override("dev", "public", "orders")
        .unwrap();
    assert!(removed);

    let overrides = store.list_table_overrides("dev").unwrap();
    assert_eq!(overrides.len(), 1);
    assert_eq!(overrides[0].table_name, "users");
}

// ── EnvironmentResolver tests ────────────────────────────────────────────────

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn make_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                names.iter().map(|s| *s).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn resolver_fallback_table_resolution() {
    // Set up: prod has "orders", dev has "users", dev falls back to prod.
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);

    let orders_batch = make_batch(&[1, 2], &["order_a", "order_b"]);
    let users_batch = make_batch(&[10, 20], &["alice", "bob"]);

    // Register "orders" in prod
    let orders_table = MemTable::try_new(test_schema(), vec![vec![orders_batch]]).unwrap();
    resolver
        .register_table("prod", "public", "orders", Arc::new(orders_table))
        .unwrap();

    // Register "users" in dev
    let users_table = MemTable::try_new(test_schema(), vec![vec![users_batch]]).unwrap();
    resolver
        .register_table("dev", "public", "users", Arc::new(users_table))
        .unwrap();

    // Create a session context with the resolver
    let ctx = SessionContext::new();
    ctx.register_catalog_list(Arc::new(resolver));

    // Query "orders" — should resolve from prod via fallback
    let df = ctx.sql("SELECT * FROM orders ORDER BY id").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    // Query "users" — should resolve from dev directly
    let df = ctx.sql("SELECT * FROM users ORDER BY id").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn resolver_dev_override_shadows_prod() {
    // Both prod and dev have "orders", dev version should win.
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);

    let prod_batch = make_batch(&[1, 2], &["prod_a", "prod_b"]);
    let dev_batch = make_batch(&[99], &["dev_override"]);

    let prod_table = MemTable::try_new(test_schema(), vec![vec![prod_batch]]).unwrap();
    resolver
        .register_table("prod", "public", "orders", Arc::new(prod_table))
        .unwrap();

    let dev_table = MemTable::try_new(test_schema(), vec![vec![dev_batch]]).unwrap();
    resolver
        .register_table("dev", "public", "orders", Arc::new(dev_table))
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog_list(Arc::new(resolver));

    let df = ctx.sql("SELECT name FROM orders").await.unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "dev_override");
}

#[tokio::test]
async fn resolver_prod_only() {
    // When active env is prod, no fallback — just prod tables.
    let resolver = EnvironmentResolver::new("prod".into(), vec!["prod".into()]);

    let batch = make_batch(&[1], &["only_in_prod"]);
    let table = MemTable::try_new(test_schema(), vec![vec![batch]]).unwrap();
    resolver
        .register_table("prod", "public", "data", Arc::new(table))
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog_list(Arc::new(resolver));

    let df = ctx.sql("SELECT * FROM data").await.unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn resolver_three_level_chain() {
    // feature -> staging -> prod
    let resolver = EnvironmentResolver::new(
        "feature".into(),
        vec!["feature".into(), "staging".into(), "prod".into()],
    );

    // Only register in prod
    let batch = make_batch(&[1], &["from_prod"]);
    let table = MemTable::try_new(test_schema(), vec![vec![batch]]).unwrap();
    resolver
        .register_table("prod", "public", "deep_table", Arc::new(table))
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog_list(Arc::new(resolver));

    // Should resolve through feature -> staging -> prod
    let df = ctx.sql("SELECT name FROM deep_table").await.unwrap();
    let batches = df.collect().await.unwrap();
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "from_prod");
}
