// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the encrypted secret store.

use flux_secrets::{SecretStore, resolve_secrets};
use tempfile::TempDir;

fn temp_store(password: &[u8]) -> (TempDir, SecretStore) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("secrets.db");
    let store = SecretStore::init(&path, password).unwrap();
    (dir, store)
}

#[test]
fn init_and_open() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("secrets.db");
    let password = b"test-password";

    SecretStore::init(&path, password).unwrap();
    assert!(SecretStore::is_initialized(&path));

    // Re-open with correct password succeeds.
    SecretStore::open(&path, password).unwrap();

    // Wrong password fails.
    assert!(SecretStore::open(&path, b"wrong").is_err());
}

#[test]
fn set_get_delete() {
    let (_dir, store) = temp_store(b"pw");

    store
        .set("db_url", b"postgres://localhost/mydb", None)
        .unwrap();

    let value = store.get("db_url", None).unwrap();
    assert_eq!(value, b"postgres://localhost/mydb");

    store.delete("db_url", None).unwrap();
    assert!(store.get("db_url", None).is_err());
}

#[test]
fn environment_scoping_and_fallback() {
    let (_dir, store) = temp_store(b"pw");

    // Default secret.
    store.set("api_key", b"default-key", None).unwrap();
    // Prod override.
    store.set("api_key", b"prod-key", Some("prod")).unwrap();

    // Resolve with prod environment → gets prod-specific.
    let val = store.resolve("api_key", Some("prod")).unwrap();
    assert_eq!(val, b"prod-key");

    // Resolve with dev environment → falls back to default.
    let val = store.resolve("api_key", Some("dev")).unwrap();
    assert_eq!(val, b"default-key");

    // Resolve with no environment → gets default.
    let val = store.resolve("api_key", None).unwrap();
    assert_eq!(val, b"default-key");
}

#[test]
fn list_secrets() {
    let (_dir, store) = temp_store(b"pw");

    store.set("alpha", b"a", None).unwrap();
    store.set("alpha", b"a-prod", Some("prod")).unwrap();
    store.set("beta", b"b", None).unwrap();

    let list = store.list().unwrap();
    assert_eq!(list.len(), 3);
    assert_eq!(list[0].name, "alpha");
    assert!(list[0].environment.is_none());
    assert_eq!(list[1].name, "alpha");
    assert_eq!(list[1].environment.as_deref(), Some("prod"));
    assert_eq!(list[2].name, "beta");
}

#[test]
fn update_overwrites() {
    let (_dir, store) = temp_store(b"pw");

    store.set("key", b"v1", None).unwrap();
    store.set("key", b"v2", None).unwrap();

    let val = store.get("key", None).unwrap();
    assert_eq!(val, b"v2");
}

#[test]
fn resolve_secret_references() {
    let (_dir, store) = temp_store(b"pw");

    store.set("db_host", b"db.example.com", None).unwrap();
    store.set("db_pass", b"s3cret", None).unwrap();
    store.set("db_pass", b"pr0d-s3cret", Some("prod")).unwrap();

    // Default environment.
    let resolved = resolve_secrets(
        "postgres://user:{{ secret:db_pass }}@{{ secret:db_host }}/app",
        &store,
        None,
    )
    .unwrap();
    assert_eq!(resolved, "postgres://user:s3cret@db.example.com/app");

    // Prod environment.
    let resolved = resolve_secrets(
        "postgres://user:{{ secret:db_pass }}@{{ secret:db_host }}/app",
        &store,
        Some("prod"),
    )
    .unwrap();
    assert_eq!(resolved, "postgres://user:pr0d-s3cret@db.example.com/app");
}

#[test]
fn non_secret_templates_preserved() {
    let (_dir, store) = temp_store(b"pw");

    let resolved = resolve_secrets("host={{ var:hostname }}", &store, None).unwrap();
    assert_eq!(resolved, "host={{ var:hostname }}");
}

#[test]
fn open_or_init_creates_new() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("secrets.db");

    let store = SecretStore::open_or_init(&path, b"pw").unwrap();
    store.set("test", b"val", None).unwrap();

    // Re-open via open_or_init uses existing.
    let store = SecretStore::open_or_init(&path, b"pw").unwrap();
    let val = store.get("test", None).unwrap();
    assert_eq!(val, b"val");
}
