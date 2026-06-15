// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cross-layer integration tests: pipeline snippets composed with SQL UDFs.
//!
//! Verifies that the two reuse primitives from planning/29 cooperate end to
//! end: a snippet's inner SQL transform can call a UDF registered from
//! `udfs_dir`, and this composition still works when snippets are nested.

use armillary_connectors::default_registry;
use armillary_datafusion::{ExecutionOptions, PipelineExecutor, RunStatus};
use armillary_engine::pipeline::Pipeline;
use std::io::Write;
use tempfile::TempDir;

fn write_file(dir: &std::path::Path, name: &str, contents: &str) {
    let mut f = std::fs::File::create(dir.join(name)).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
}

fn write_csv(dir: &TempDir, filename: &str, content: &str) -> String {
    let path = dir.path().join(filename);
    std::fs::File::create(&path)
        .unwrap()
        .write_all(content.as_bytes())
        .unwrap();
    path.to_str().unwrap().to_string()
}

fn normalize_name_udf() -> &'static str {
    "CREATE OR REPLACE FUNCTION normalize_name(s VARCHAR) RETURNS VARCHAR \
     AS $$ LOWER(TRIM(s)) $$ LANGUAGE SQL IMMUTABLE;"
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snippet_inner_sql_calls_udf() {
    // A snippet whose internal SQL transform calls a UDF declared in udfs_dir.
    // Validates: snippets_dir + udfs_dir cooperate, snippet expansion runs
    // before UDF inlining, and the rewritten SQL produces normalized output.
    let dir = TempDir::new().unwrap();

    // UDFs.
    let udfs_dir = dir.path().join("udfs");
    std::fs::create_dir(&udfs_dir).unwrap();
    write_file(&udfs_dir, "normalize_name.sql", normalize_name_udf());

    // Snippets.
    let snippets_dir = dir.path().join("snippets");
    std::fs::create_dir(&snippets_dir).unwrap();
    let snippet = serde_json::json!({
        "name": "ingest_clean",
        "snippet": "ingest_clean",
        "params": { "src_path": "string" },
        "outputs": ["cleansed"],
        "nodes": [
            { "id": "raw", "name": "raw", "type": "source", "connector": "csv",
              "config": { "path": "{{ src_path }}", "format": "csv" } },
            { "id": "cleansed", "name": "cleansed", "type": "transform", "mode": "sql",
              "code": "SELECT id, normalize_name(name) AS name FROM raw" }
        ],
        "edges": [{ "from": "raw", "to": "cleansed" }]
    });
    write_file(&snippets_dir, "ingest_clean.json", &snippet.to_string());

    // Inputs / outputs.
    let input = write_csv(
        &dir,
        "input.csv",
        "id,name\n1,  Alice  \n2, BOB \n3,Carol\n",
    );
    let output = dir.path().join("out.csv");

    // Parent pipeline JSON (uses absolute paths so from_json_at_path resolves).
    let parent = serde_json::json!({
        "name": "parent",
        "version": 1,
        "default_environment": "dev",
        "udfs_dir": udfs_dir.to_string_lossy(),
        "snippets_dir": snippets_dir.to_string_lossy(),
        "nodes": [
            { "id": "ing", "name": "ing", "snippet": "ingest_clean",
              "params": { "src_path": input } },
            { "id": "sink", "name": "sink", "type": "sink", "connector": "csv",
              "config": { "path": output.to_string_lossy(), "format": "csv" } }
        ],
        "edges": [{ "from": "ing.cleansed", "to": "sink" }]
    });
    let p = Pipeline::from_json_at_path(&parent.to_string(), dir.path())
        .expect("parent pipeline should load + expand");

    // Sanity: snippet was expanded into namespaced inner nodes.
    let ids: Vec<&str> = p.nodes.iter().map(|n| n.id.0.as_str()).collect();
    assert!(ids.contains(&"ing.raw"), "ids: {ids:?}");
    assert!(ids.contains(&"ing.cleansed"), "ids: {ids:?}");

    let registry = default_registry().into_provider_registry();
    let (_result, run) = PipelineExecutor::execute(&p, &registry, &ExecutionOptions::default())
        .await
        .expect("pipeline should succeed");
    assert_eq!(run.status, RunStatus::Success);

    let content = std::fs::read_to_string(&output).unwrap();
    assert!(content.contains("alice"), "got: {content}");
    assert!(content.contains("bob"), "got: {content}");
    assert!(content.contains("carol"), "got: {content}");
    assert!(
        !content.contains("Alice"),
        "Alice should be lowercased: {content}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_snippet_inner_sql_calls_udf() {
    // outer snippet wraps inner snippet; inner SQL calls a UDF. Verifies the
    // full Layer 1 + Layer 2 + nested-snippet composition.
    let dir = TempDir::new().unwrap();

    let udfs_dir = dir.path().join("udfs");
    std::fs::create_dir(&udfs_dir).unwrap();
    write_file(&udfs_dir, "normalize_name.sql", normalize_name_udf());

    let snippets_dir = dir.path().join("snippets");
    std::fs::create_dir(&snippets_dir).unwrap();

    let inner = serde_json::json!({
        "name": "clean",
        "snippet": "clean",
        "params": { "src_path": "string" },
        "outputs": ["cleansed"],
        "nodes": [
            { "id": "raw", "name": "raw", "type": "source", "connector": "csv",
              "config": { "path": "{{ src_path }}", "format": "csv" } },
            { "id": "cleansed", "name": "cleansed", "type": "transform", "mode": "sql",
              "code": "SELECT id, normalize_name(name) AS name FROM raw" }
        ],
        "edges": [{ "from": "raw", "to": "cleansed" }]
    });
    let outer = serde_json::json!({
        "name": "wrap",
        "snippet": "wrap",
        "params": { "src_path": "string" },
        "outputs": ["inner.cleansed"],
        "nodes": [
            { "id": "inner", "name": "inner", "snippet": "clean",
              "params": { "src_path": "{{ src_path }}" } }
        ],
        "edges": []
    });
    write_file(&snippets_dir, "clean.json", &inner.to_string());
    write_file(&snippets_dir, "wrap.json", &outer.to_string());

    let input = write_csv(&dir, "input.csv", "id,name\n1, Dee \n2,EVE\n");
    let output = dir.path().join("out.csv");

    let parent = serde_json::json!({
        "name": "parent",
        "version": 1,
        "default_environment": "dev",
        "udfs_dir": udfs_dir.to_string_lossy(),
        "snippets_dir": snippets_dir.to_string_lossy(),
        "nodes": [
            { "id": "w", "name": "w", "snippet": "wrap",
              "params": { "src_path": input } },
            { "id": "sink", "name": "sink", "type": "sink", "connector": "csv",
              "config": { "path": output.to_string_lossy(), "format": "csv" } }
        ],
        "edges": [{ "from": "w.inner.cleansed", "to": "sink" }]
    });
    let p = Pipeline::from_json_at_path(&parent.to_string(), dir.path())
        .expect("parent pipeline should load + expand nested snippets");

    let ids: Vec<&str> = p.nodes.iter().map(|n| n.id.0.as_str()).collect();
    assert!(ids.contains(&"w.inner.raw"), "ids: {ids:?}");
    assert!(ids.contains(&"w.inner.cleansed"), "ids: {ids:?}");

    let registry = default_registry().into_provider_registry();
    let (_result, run) = PipelineExecutor::execute(&p, &registry, &ExecutionOptions::default())
        .await
        .expect("pipeline should succeed");
    assert_eq!(run.status, RunStatus::Success);

    let content = std::fs::read_to_string(&output).unwrap();
    assert!(content.contains("dee"), "got: {content}");
    assert!(content.contains("eve"), "got: {content}");
    assert!(!content.contains("EVE"), "should be lowercased: {content}");
}
