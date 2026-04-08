// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end smoke test for the parquet reference plugin: spawn the real
//! built binary, drive the full sink lifecycle, then read the parquet file
//! back and assert the data round-trips intact.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use tempfile::tempdir;

use flux_plugin_host::manifest::Manifest;
use flux_plugin_host::process::{PluginProcess, SpawnOptions};
use flux_plugin_host::session::PluginSession;

fn build_plugin_binary() -> PathBuf {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--bin", "flux-parquet-plugin", "--quiet"])
        .status()
        .expect("cargo build flux-parquet-plugin");
    assert!(status.success(), "failed to build flux-parquet-plugin");

    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            manifest_dir
                .ancestors()
                .nth(3)
                .expect("workspace root")
                .join("target")
        });
    let bin = if cfg!(windows) {
        target_dir.join("debug").join("flux-parquet-plugin.exe")
    } else {
        target_dir.join("debug").join("flux-parquet-plugin")
    };
    assert!(bin.is_file(), "expected built binary at {}", bin.display());
    bin
}

fn sample_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
    ])
}

fn sample_batches(schema: &Schema) -> Vec<RecordBatch> {
    let s = Arc::new(schema.clone());
    let b1 = RecordBatch::try_new(
        s.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            Arc::new(Float64Array::from(vec![Some(95.5), Some(87.0), None])),
        ],
    )
    .unwrap();
    let b2 = RecordBatch::try_new(
        s,
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["dan", "eve"])),
            Arc::new(Float64Array::from(vec![Some(72.25), Some(100.0)])),
        ],
    )
    .unwrap();
    vec![b1, b2]
}

#[test]
fn parquet_plugin_round_trips_batches() {
    let bin_path = build_plugin_binary();

    // Stage a plugin directory layout pointing at the built binary.
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

    // Copy the real manifest + schema next to the binary.
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    std::fs::copy(
        crate_root.join("plugin.toml"),
        plugin_dir.path().join("plugin.toml"),
    )
    .unwrap();
    std::fs::copy(
        crate_root.join("config_schema.json"),
        plugin_dir.path().join("config_schema.json"),
    )
    .unwrap();

    // Patch the manifest to point at the staged executable name (which has
    // the platform-correct extension on Windows).
    let manifest_path = plugin_dir.path().join("plugin.toml");
    let manifest_text = std::fs::read_to_string(&manifest_path).unwrap();
    let patched = manifest_text.replace(
        "executable = \"flux-parquet-plugin\"",
        &format!("executable = \"{}\"", exe_name.to_string_lossy()),
    );
    std::fs::write(&manifest_path, patched).unwrap();
    let manifest = Manifest::from_path(&manifest_path).unwrap();

    let out_path = plugin_dir.path().join("out.parquet");

    let proc = PluginProcess::spawn_with_manifest(
        "parquet",
        plugin_dir.path(),
        &manifest,
        SpawnOptions::default(),
    )
    .unwrap();

    let mut session = PluginSession::new(proc, 1, "0.0.0-test");
    let ack = session.handshake().unwrap();
    assert_eq!(ack.plugin_name, "parquet");

    let schema = sample_schema();
    session
        .configure(
            "parquet_plugin",
            json!({ "path": out_path.to_str().unwrap(), "compression": "snappy" }),
            &schema,
        )
        .unwrap();

    let batches = sample_batches(&schema);
    let mut expected_rows: u64 = 0;
    for batch in &batches {
        let ack = session.send_batch(batch).unwrap();
        assert_eq!(ack.rows_accepted, batch.num_rows() as u64);
        expected_rows += batch.num_rows() as u64;
    }
    let commit = session.commit().unwrap();
    assert_eq!(commit.rows, expected_rows);
    session.shutdown().unwrap();
    drop(session);

    // Read the parquet file back and assert the data matches.
    assert!(out_path.is_file(), "plugin did not produce {}", out_path.display());
    let file = std::fs::File::open(&out_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let read_batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
    let read_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(read_rows as u64, expected_rows);

    // Concatenate all read batches into one for value-level comparison.
    let combined = arrow::compute::concat_batches(&Arc::new(schema.clone()), &read_batches).unwrap();
    let original = arrow::compute::concat_batches(&Arc::new(schema), &batches).unwrap();
    assert_eq!(combined, original, "round-tripped batches do not match originals");
}
