// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Host-side integration test against the **real** bundled OpenBoard plugin.
//!
//! Mirrors `openboard/plugins/armillary/test/integration.test.ts` but drives the
//! plugin from the host's own framing/transport code, so format drift between
//! the Rust host and the TypeScript plugin gets caught in armillary's CI.
//!
//! The OpenBoard plugin lives in a sibling repo and is not always present
//! (CI checkouts of just armillary, contributors without the openboard
//! tree, etc.). The test therefore **skips** rather than fails when:
//!   - the sibling `openboard/plugins/armillary` directory is missing
//!   - `dist/openboard-plugin.js` hasn't been built
//!   - `node` is not on `PATH`
//!
//! Run a fresh build of the plugin (`npm run build` in `openboard/plugins/armillary`)
//! before relying on this test locally.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use armillary_plugin_host::manifest::Manifest;
use armillary_plugin_host::process::{PluginProcess, SpawnOptions};
use armillary_plugin_host::session::PluginSession;
use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::json;
use tempfile::tempdir;

fn spawn_session(plugin_dir: &std::path::Path) -> PluginSession<PluginProcess> {
    let manifest_path = plugin_dir.join("plugin.toml");
    let manifest = Manifest::from_path(&manifest_path).expect("parse plugin.toml");
    let proc = PluginProcess::spawn_with_manifest(
        "openboard",
        plugin_dir,
        &manifest,
        SpawnOptions::default(),
    )
    .expect("spawn openboard plugin");
    let mut session = PluginSession::new(proc, 1, "0.0.0-test");
    let ack = session.handshake().expect("handshake");
    assert_eq!(ack.plugin_name, "openboard");
    session
}

fn locate_openboard_plugin() -> Option<PathBuf> {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)?
        .to_path_buf();
    let candidate = workspace_root
        .parent()?
        .join("openboard")
        .join("plugins")
        .join("armillary");
    if candidate.join("plugin.toml").is_file()
        && candidate.join("dist").join("openboard-plugin.js").is_file()
    {
        Some(candidate)
    } else {
        None
    }
}

fn node_on_path() -> bool {
    Command::new("node")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[test]
fn openboard_plugin_full_lifecycle() {
    let Some(plugin_dir) = locate_openboard_plugin() else {
        eprintln!("skipping: openboard plugin not found at sibling repo");
        return;
    };
    if !node_on_path() {
        eprintln!("skipping: `node` not on PATH");
        return;
    }

    let manifest_path = plugin_dir.join("plugin.toml");
    let manifest = Manifest::from_path(&manifest_path).expect("parse plugin.toml");

    let proc = PluginProcess::spawn_with_manifest(
        "openboard",
        &plugin_dir,
        &manifest,
        SpawnOptions::default(),
    )
    .expect("spawn openboard plugin");

    let mut session = PluginSession::new(proc, 1, "0.0.0-test");
    let ack = session.handshake().expect("handshake");
    assert_eq!(ack.plugin_name, "openboard");

    // Stage a fresh OpenBoard project directory so the plugin can write
    // connections/, datasets/, and the DuckDB file into a clean tree.
    let project = tempdir().unwrap();
    let project_path = project.path().to_path_buf();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    session
        .configure(
            "openboard_duckdb",
            json!({
                "openboard_project": project_path.to_str().unwrap(),
                "connection_name": "armillary_pipelines",
                "database_file": "data/armillary.duckdb",
                "table_name": "host_harness_rows",
                "write_mode": "replace",
                "write_dataset_metadata": true,
            }),
            &schema,
            None,
        )
        .expect("configure");

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let ack = session.send_batch(&batch).expect("send batch");
    assert_eq!(ack.rows_accepted, 3);

    let commit = session.commit().expect("commit");
    assert_eq!(commit.rows, 3);
    session.shutdown().expect("shutdown");

    // The plugin promises: target file present, connection + dataset YAML emitted.
    assert!(
        project_path.join("data").join("armillary.duckdb").is_file(),
        "expected DuckDB target file to exist after commit"
    );
    assert!(
        project_path
            .join("connections")
            .join("armillary_pipelines.yaml")
            .is_file(),
        "expected connection yaml to exist after commit"
    );
    assert!(
        project_path
            .join("datasets")
            .join("host_harness_rows.yaml")
            .is_file(),
        "expected dataset metadata yaml to exist after commit"
    );
}

/// End-to-end snapshot (SCD2) lifecycle through the host → plugin → DuckDB
/// path. Each cycle is a fresh spawn (one configure/commit per session). Per
/// cycle we assert the `CommitAck.rows` (which the plugin populates from
/// `SnapshotMergeStats.rows_inserted`, i.e. new versions opened) matches the
/// stage-diff-merge expectation. Combined with `sink.snapshot.test.ts` on the
/// plugin side — which already inspects the generated `.duckdb` file's SCD2
/// invariants — this proves the host transport faithfully forwards the
/// `materialization` sub-block and round-trips the snapshot receipt counts.
#[test]
fn openboard_plugin_snapshot_lifecycle() {
    let Some(plugin_dir) = locate_openboard_plugin() else {
        eprintln!("skipping: openboard plugin not found at sibling repo");
        return;
    };
    if !node_on_path() {
        eprintln!("skipping: `node` not on PATH");
        return;
    }

    // Sanity check: the bundled manifest must declare snapshot capability.
    // If a contributor downgrades the manifest this test catches it before
    // we waste time spawning the plugin.
    let manifest = Manifest::from_path(&plugin_dir.join("plugin.toml")).expect("parse plugin.toml");
    let snapshot_capable = manifest.sinks.iter().any(|s| {
        s.capabilities
            .materialization
            .as_ref()
            .is_some_and(|m| m.snapshot)
    });
    assert!(
        snapshot_capable,
        "openboard plugin manifest must declare snapshot capability for this test",
    );

    let project = tempdir().unwrap();
    let project_path = project.path().to_path_buf();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let materialization = json!({
        "read_mode": "full",
        "write_strategy": "snapshot",
        "unique_keys": ["id"],
        "snapshot": {
            "change_detection": "check",
            "check_columns": ["name"],
            "hard_deletes": "invalidate",
        },
    });

    let run = |rows: Vec<(i32, &'static str)>| -> u64 {
        let mut session = spawn_session(&plugin_dir);
        session
            .configure(
                "openboard_duckdb",
                json!({
                    "openboard_project": project_path.to_str().unwrap(),
                    "connection_name": "armillary_pipelines",
                    "database_file": "data/armillary.duckdb",
                    "table_name": "snapshot_rows",
                    "write_dataset_metadata": false,
                }),
                &schema,
                Some(materialization.clone()),
            )
            .expect("configure snapshot");
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(
                    rows.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, n)| *n).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let _ = session.send_batch(&batch).expect("send batch");
        let ack = session.commit().expect("commit");
        session.shutdown().expect("shutdown");
        ack.rows
    };

    // Run 1: 3 brand-new rows → 3 versions opened.
    assert_eq!(run(vec![(1, "alice"), (2, "bob"), (3, "carol")]), 3);

    // Run 2: identical rows → idempotent, no new versions.
    assert_eq!(run(vec![(1, "alice"), (2, "bob"), (3, "carol")]), 0);

    // Run 3: id=2's tracked column changes → exactly one new version opened.
    assert_eq!(run(vec![(1, "alice"), (2, "BOB"), (3, "carol")]), 1);

    // Run 4: id=1 vanishes; hard_deletes=invalidate closes its current row
    // but opens no new version.
    assert_eq!(run(vec![(2, "BOB"), (3, "carol")]), 0);

    // The DuckDB target file must exist after every commit cycle.
    assert!(
        project_path.join("data").join("armillary.duckdb").is_file(),
        "expected DuckDB target file to exist after snapshot commits",
    );
}
