// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Layer-2 snippet integration tests. Tests #13 and #14 from planning doc 29a.

use armillary_engine::Pipeline;

/// Test #13: snippet whose inner SQL transform references a registered UDF.
/// Without a running executor we verify the post-expansion pipeline contains
/// the UDF call in the expected node, i.e. substitution + namespacing didn't
/// corrupt the SQL.
#[test]
fn snippet_with_udf_preserves_function_call() {
    let dir = tempfile::tempdir().unwrap();
    let snippets_dir = dir.path().join("snippets");
    std::fs::create_dir_all(&snippets_dir).unwrap();
    let snippet_json = serde_json::json!({
        "name": "clean",
        "snippet": "clean",
        "params": { "col": "column" },
        "outputs": ["cleaned"],
        "nodes": [
            { "id": "src", "name": "src", "type": "source", "connector": "csv",
              "config": { "path": "data.csv" } },
            { "id": "cleaned", "name": "cleaned", "type": "transform", "mode": "sql",
              "code": "SELECT my_udf({{ col }}) AS x FROM src" }
        ],
        "edges": [{ "from": "src", "to": "cleaned" }]
    });
    std::fs::write(
        snippets_dir.join("clean.json"),
        serde_json::to_string_pretty(&snippet_json).unwrap(),
    )
    .unwrap();

    let pipeline_json = serde_json::json!({
        "name": "parent",
        "snippets_dir": "snippets",
        "nodes": [
            { "id": "c", "name": "c", "snippet": "clean", "params": { "col": "name" } },
            { "id": "sink", "name": "sink", "type": "sink", "connector": "stdout" }
        ],
        "edges": [{ "from": "c.cleaned", "to": "sink" }]
    });
    let path = dir.path().join("parent.json");
    std::fs::write(&path, serde_json::to_string_pretty(&pipeline_json).unwrap()).unwrap();

    let raw = std::fs::read_to_string(&path).unwrap();
    let p = Pipeline::from_json_at_path(&raw, dir.path()).expect("load + expand");
    let cleaned = p
        .nodes
        .iter()
        .find(|n| n.id.0 == "c.cleaned")
        .expect("namespaced node present");
    let armillary_engine::NodeKind::Transform(t) = &cleaned.kind else {
        panic!("expected transform");
    };
    assert!(t.code.contains("my_udf(name)"), "code: {}", t.code);
}

/// Test #14: end-to-end execution of a pipeline that uses a snippet.
///
/// Wiring a live DataFusion executor from a plain armillary-engine integration
/// test would require pulling in armillary-datafusion + armillary-connectors, which
/// blows scope for this layer. The load-and-expand path is proven by the
/// unit tests; treat this as a stub until a thin in-process executor
/// helper exists.
#[test]
#[ignore = "requires armillary-datafusion executor harness — see doc 29a test #14"]
fn end_to_end_snippet_pipeline() {
    // TODO: once a shared executor test-helper exists (used elsewhere in
    // the workspace), instantiate it here, run a CSV-source → snippet →
    // stdout-sink pipeline and assert the rows.
}
