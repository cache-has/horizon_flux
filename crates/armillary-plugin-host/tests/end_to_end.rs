// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end smoke test: spawn the in-tree `mock-plugin` example over a real
//! subprocess and drive the full sink lifecycle through it.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use armillary_plugin_host::manifest::Manifest;
use armillary_plugin_host::process::{PluginProcess, SpawnOptions};
use armillary_plugin_host::session::PluginSession;
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::json;
use tempfile::tempdir;

fn build_mock_plugin() -> PathBuf {
    // Build the example binary in the same target dir cargo test uses.
    let status = Command::new(env!("CARGO"))
        .args(["build", "--example", "mock-plugin", "--quiet"])
        .status()
        .expect("cargo build mock-plugin");
    assert!(status.success(), "failed to build mock-plugin example");

    // CARGO_TARGET_DIR or default. We resolve via env first.
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            manifest_dir
                .ancestors()
                .nth(2)
                .expect("workspace root")
                .join("target")
        });
    let bin = if cfg!(windows) {
        target_dir
            .join("debug")
            .join("examples")
            .join("mock-plugin.exe")
    } else {
        target_dir
            .join("debug")
            .join("examples")
            .join("mock-plugin")
    };
    assert!(bin.is_file(), "expected built binary at {}", bin.display());
    bin
}

#[test]
fn end_to_end_lifecycle_against_real_subprocess() {
    let bin_path = build_mock_plugin();

    // Stage a plugin directory layout pointing at the built example binary.
    let plugin_dir = tempdir().unwrap();
    let exe_name = bin_path.file_name().unwrap();
    let staged_exe = plugin_dir.path().join(exe_name);
    std::fs::copy(&bin_path, &staged_exe).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&staged_exe).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&staged_exe, perms).unwrap();
    }

    let manifest_toml = format!(
        r#"
name = "mock"
version = "0.1.0"
armillary_plugin_protocol = 1
armillary_min_version = "0.1.0"
executable = "{}"

[[sinks]]
type = "mock_sink"
display_name = "Mock"
config_schema = "schema.json"
"#,
        exe_name.to_string_lossy()
    );
    let manifest_path = plugin_dir.path().join("plugin.toml");
    std::fs::write(&manifest_path, &manifest_toml).unwrap();
    let manifest = Manifest::from_path(&manifest_path).unwrap();

    let proc = PluginProcess::spawn_with_manifest(
        "mock",
        plugin_dir.path(),
        &manifest,
        SpawnOptions::default(),
    )
    .unwrap();

    let mut session = PluginSession::new(proc, 1, "0.0.0-test");
    let ack = session.handshake().unwrap();
    assert_eq!(ack.plugin_name, "mock");

    let schema = Schema::new(vec![Field::new("v", DataType::Int32, false)]);
    session
        .configure("mock_sink", json!({}), &schema, None)
        .unwrap();

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30, 40]))],
    )
    .unwrap();
    let back = session.send_batch(&batch).unwrap();
    assert_eq!(back.rows_accepted, 4);

    let commit = session.commit().unwrap();
    assert_eq!(commit.rows, 4);

    session.shutdown().unwrap();
}
