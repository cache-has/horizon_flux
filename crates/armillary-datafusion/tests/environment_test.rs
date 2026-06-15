// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Tests for the environment data model, SQLite store, and catalog resolver.

use armillary_datafusion::EnvironmentStorage;
use armillary_datafusion::environment::SqliteEnvironmentStore;
use armillary_datafusion::resolver::EnvironmentResolver;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

// ── SqliteEnvironmentStore tests ───────────────────────────────────────────────────

#[test]
fn default_environments_exist() {
    let store = SqliteEnvironmentStore::open_in_memory().unwrap();
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
    let store = SqliteEnvironmentStore::open_in_memory().unwrap();
    store.create("staging", Some("prod")).unwrap();

    let staging = store.get("staging").unwrap().unwrap();
    assert_eq!(staging.fallback, Some("prod".to_string()));

    store.delete("staging").unwrap();
    assert!(store.get("staging").unwrap().is_none());
}

#[test]
fn cannot_delete_prod() {
    let store = SqliteEnvironmentStore::open_in_memory().unwrap();
    let err = store.delete("prod").unwrap_err();
    assert!(err.to_string().contains("prod"));
}

#[test]
fn cannot_create_duplicate() {
    let store = SqliteEnvironmentStore::open_in_memory().unwrap();
    let err = store.create("prod", None).unwrap_err();
    assert!(err.to_string().contains("already exists"));
}

#[test]
fn fallback_must_exist() {
    let store = SqliteEnvironmentStore::open_in_memory().unwrap();
    let err = store.create("staging", Some("nonexistent")).unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[test]
fn fallback_chain() {
    let store = SqliteEnvironmentStore::open_in_memory().unwrap();
    store.create("staging", Some("prod")).unwrap();
    store.create("feature", Some("staging")).unwrap();

    let chain = store.fallback_chain("feature").unwrap();
    assert_eq!(chain, vec!["feature", "staging", "prod"]);
}

#[test]
fn delete_repoints_dependents() {
    let store = SqliteEnvironmentStore::open_in_memory().unwrap();
    store.create("staging", Some("prod")).unwrap();
    store.create("feature", Some("staging")).unwrap();

    store.delete("staging").unwrap();

    // feature should now fall back to prod directly
    let feature = store.get("feature").unwrap().unwrap();
    assert_eq!(feature.fallback, Some("prod".to_string()));
}

#[test]
fn update_fallback() {
    let store = SqliteEnvironmentStore::open_in_memory().unwrap();
    store.create("staging", Some("prod")).unwrap();

    store.update_fallback("dev", Some("staging")).unwrap();
    let chain = store.fallback_chain("dev").unwrap();
    assert_eq!(chain, vec!["dev", "staging", "prod"]);
}

#[test]
fn table_override_crud() {
    let store = SqliteEnvironmentStore::open_in_memory().unwrap();

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
            Arc::new(StringArray::from(names.to_vec())),
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

// ── Resolver unit-level coverage tests ──────────────────────────────────────

#[test]
fn resolver_accessors() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);
    assert_eq!(resolver.active_environment(), "dev");
    assert_eq!(resolver.fallback_chain(), &["dev", "prod"]);
}

#[test]
fn resolver_debug_format() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);
    let dbg = format!("{resolver:?}");
    assert!(dbg.contains("dev"));
    assert!(dbg.contains("EnvironmentResolver"));
}

#[test]
fn resolver_environment_catalog_exists() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);
    assert!(resolver.environment_catalog("dev").is_some());
    assert!(resolver.environment_catalog("prod").is_some());
    assert!(resolver.environment_catalog("nonexistent").is_none());
}

#[test]
fn resolver_register_table_unknown_env_fails() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into()]);
    let batch = make_batch(&[1], &["a"]);
    let table = MemTable::try_new(test_schema(), vec![vec![batch]]).unwrap();
    let err = resolver
        .register_table("unknown", "public", "t", Arc::new(table))
        .unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[test]
fn resolver_catalog_names() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);
    let names = resolver.catalog_names();
    assert!(names.contains(&"datafusion".to_string()));
}

#[test]
fn resolver_catalog_returns_merged_for_datafusion() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);
    let catalog = resolver.catalog("datafusion");
    assert!(catalog.is_some());
}

#[test]
fn resolver_catalog_returns_specific_env() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);
    let catalog = resolver.catalog("dev");
    assert!(catalog.is_some());
    let catalog = resolver.catalog("nonexistent");
    assert!(catalog.is_none());
}

#[test]
fn resolver_register_catalog_non_env() {
    use datafusion::catalog::MemoryCatalogProvider;

    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into()]);
    // Register a non-EnvironmentCatalog — should return None.
    let mem_catalog = MemoryCatalogProvider::new();
    let result = resolver.register_catalog(
        "other".to_string(),
        Arc::new(mem_catalog) as Arc<dyn CatalogProvider>,
    );
    assert!(result.is_none());
}

#[test]
fn resolver_register_catalog_env() {
    use armillary_datafusion::resolver::EnvironmentCatalog;

    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into()]);
    let env_cat = EnvironmentCatalog::new("dev".into());
    let result = resolver.register_catalog(
        "dev".to_string(),
        Arc::new(env_cat) as Arc<dyn CatalogProvider>,
    );
    // Replaces existing dev catalog, returns the old one.
    assert!(result.is_some());
}

#[test]
fn environment_catalog_schema_names() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into()]);
    let cat = resolver.environment_catalog("dev").unwrap();
    let names = cat.schema_names();
    assert!(names.contains(&"public".to_string()));
}

#[test]
fn environment_catalog_schema_lookup() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into()]);
    let cat = resolver.environment_catalog("dev").unwrap();
    assert!(cat.schema("public").is_some());
    assert!(cat.schema("nonexistent").is_none());
}

#[test]
fn environment_catalog_get_or_create_schema() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into()]);
    let cat = resolver.environment_catalog("dev").unwrap();
    let schema = cat.get_or_create_schema("custom");
    assert_eq!(schema.table_names().len(), 0);
    // Second call returns same schema.
    let schema2 = cat.get_or_create_schema("custom");
    assert_eq!(schema.table_names().len(), schema2.table_names().len());
}

#[test]
fn environment_catalog_register_schema() {
    use armillary_datafusion::resolver::EnvironmentCatalog;

    let cat = EnvironmentCatalog::new("dev".into());
    let new_schema = Arc::new(armillary_datafusion::resolver::EnvironmentSchema::new(
        "test".into(),
    ));
    let prev = cat.register_schema("test", new_schema).unwrap();
    assert!(prev.is_none());
    assert!(cat.schema("test").is_some());
}

#[test]
fn environment_catalog_debug() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into()]);
    let cat = resolver.environment_catalog("dev").unwrap();
    let dbg = format!("{cat:?}");
    assert!(dbg.contains("EnvironmentCatalog"));
}

#[tokio::test]
async fn merged_catalog_schema_names_deduplicates() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);
    // Both envs have "public" — merged should list it once.
    let merged = resolver.catalog("datafusion").unwrap();
    let names = merged.schema_names();
    assert_eq!(names.iter().filter(|n| *n == "public").count(), 1);
}

#[tokio::test]
async fn merged_catalog_schema_returns_none_for_unknown() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into()]);
    let merged = resolver.catalog("datafusion").unwrap();
    assert!(merged.schema("nonexistent").is_none());
}

#[tokio::test]
async fn fallback_schema_table_names_merges() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);

    let batch1 = make_batch(&[1], &["a"]);
    let batch2 = make_batch(&[2], &["b"]);

    let t1 = MemTable::try_new(test_schema(), vec![vec![batch1]]).unwrap();
    resolver
        .register_table("prod", "public", "t_prod", Arc::new(t1))
        .unwrap();

    let t2 = MemTable::try_new(test_schema(), vec![vec![batch2]]).unwrap();
    resolver
        .register_table("dev", "public", "t_dev", Arc::new(t2))
        .unwrap();

    let merged = resolver.catalog("datafusion").unwrap();
    let schema = merged.schema("public").unwrap();
    let names = schema.table_names();
    assert!(names.contains(&"t_prod".to_string()));
    assert!(names.contains(&"t_dev".to_string()));
}

#[tokio::test]
async fn fallback_schema_table_exist() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);

    let batch = make_batch(&[1], &["a"]);
    let table = MemTable::try_new(test_schema(), vec![vec![batch]]).unwrap();
    resolver
        .register_table("prod", "public", "prod_only", Arc::new(table))
        .unwrap();

    let merged = resolver.catalog("datafusion").unwrap();
    let schema = merged.schema("public").unwrap();
    assert!(schema.table_exist("prod_only"));
    assert!(!schema.table_exist("nonexistent"));
}

#[tokio::test]
async fn fallback_schema_register_table_goes_to_active() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);
    let merged = resolver.catalog("datafusion").unwrap();
    let schema = merged.schema("public").unwrap();

    let batch = make_batch(&[1], &["registered"]);
    let table = MemTable::try_new(test_schema(), vec![vec![batch]]).unwrap();
    schema
        .register_table("new_table".to_string(), Arc::new(table))
        .unwrap();

    // Should be visible via the fallback schema.
    assert!(schema.table_exist("new_table"));

    // Should be in dev's catalog directly.
    let dev_cat = resolver.environment_catalog("dev").unwrap();
    let dev_schema = dev_cat.schema("public").unwrap();
    assert!(dev_schema.table_exist("new_table"));
}

#[tokio::test]
async fn fallback_schema_deregister_table() {
    let resolver = EnvironmentResolver::new("dev".into(), vec!["dev".into(), "prod".into()]);

    let batch = make_batch(&[1], &["a"]);
    let table = MemTable::try_new(test_schema(), vec![vec![batch]]).unwrap();
    resolver
        .register_table("dev", "public", "removeme", Arc::new(table))
        .unwrap();

    let merged = resolver.catalog("datafusion").unwrap();
    let schema = merged.schema("public").unwrap();
    assert!(schema.table_exist("removeme"));

    let removed = schema.deregister_table("removeme").unwrap();
    assert!(removed.is_some());
    assert!(!schema.table_exist("removeme"));
}

#[tokio::test]
async fn environment_schema_deregister() {
    use armillary_datafusion::resolver::EnvironmentSchema;

    let schema = EnvironmentSchema::new("test".into());
    let batch = make_batch(&[1], &["a"]);
    let table = MemTable::try_new(test_schema(), vec![vec![batch]]).unwrap();
    schema
        .register_table("t".to_string(), Arc::new(table))
        .unwrap();
    assert!(schema.table_exist("t"));

    let removed = schema.deregister_table("t").unwrap();
    assert!(removed.is_some());
    assert!(!schema.table_exist("t"));
}

#[tokio::test]
async fn environment_schema_table_async() {
    use armillary_datafusion::resolver::EnvironmentSchema;

    let schema = EnvironmentSchema::new("test".into());
    let batch = make_batch(&[1], &["a"]);
    let table = MemTable::try_new(test_schema(), vec![vec![batch]]).unwrap();
    schema
        .register_table("t".to_string(), Arc::new(table))
        .unwrap();

    let result = schema.table("t").await.unwrap();
    assert!(result.is_some());
    let result = schema.table("nonexistent").await.unwrap();
    assert!(result.is_none());
}

#[test]
fn environment_schema_debug() {
    use armillary_datafusion::resolver::EnvironmentSchema;
    let schema = EnvironmentSchema::new("test".into());
    let dbg = format!("{schema:?}");
    assert!(dbg.contains("EnvironmentSchema"));
    assert!(dbg.contains("test"));
}
