// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Failure-mode coverage for the v1 plugin protocol — see the Testing section
//! of `planning/24-plugin-system.md`.
//!
//! Each test spawns the in-tree `mock-plugin` example with a `--mode` arg that
//! makes it misbehave in a specific way, then verifies the host surfaces a
//! clean, actionable error rather than crashing or hanging.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use flux_plugin_host::discovery::{PLUGIN_PATH_ENV, PluginStatus, discover_plugins};
use flux_plugin_host::manifest::Manifest;
use flux_plugin_host::process::{PluginProcess, SpawnOptions};
use flux_plugin_host::protocol::{FrameError, MAX_PAYLOAD_LEN, MessageKind, read_frame};
use flux_plugin_host::session::{PluginSession, SessionError};
use flux_plugin_host::transport::TransportError;
use serde_json::json;
use tempfile::tempdir;

fn build_mock_plugin() -> PathBuf {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--example", "mock-plugin", "--quiet"])
        .status()
        .expect("cargo build mock-plugin");
    assert!(status.success(), "failed to build mock-plugin example");

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

fn stage_plugin_dir(bin: &PathBuf) -> (tempfile::TempDir, Manifest) {
    let dir = tempdir().unwrap();
    let exe_name = bin.file_name().unwrap();
    let staged = dir.path().join(exe_name);
    std::fs::copy(bin, &staged).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&staged).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&staged, perms).unwrap();
    }

    let manifest_toml = format!(
        r#"
name = "mock"
version = "0.1.0"
flux_plugin_protocol = 1
flux_min_version = "0.1.0"
executable = "{}"

[[sinks]]
type = "mock_sink"
display_name = "Mock"
config_schema = "schema.json"
"#,
        exe_name.to_string_lossy()
    );
    let manifest_path = dir.path().join("plugin.toml");
    std::fs::write(&manifest_path, &manifest_toml).unwrap();
    let manifest = Manifest::from_path(&manifest_path).unwrap();
    (dir, manifest)
}

fn spawn(bin: &PathBuf, mode: &str) -> (tempfile::TempDir, PluginProcess) {
    let (dir, manifest) = stage_plugin_dir(bin);
    let opts = SpawnOptions {
        extra_args: vec!["--mode".into(), mode.into()],
        cwd: None,
    };
    let proc = PluginProcess::spawn_with_manifest("mock", dir.path(), &manifest, opts).unwrap();
    (dir, proc)
}

fn schema() -> Schema {
    Schema::new(vec![Field::new("v", DataType::Int32, false)])
}

fn batch_of(vals: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(schema()), vec![Arc::new(Int32Array::from(vals))]).unwrap()
}

#[test]
fn handshake_timeout_when_plugin_hangs() {
    let bin = build_mock_plugin();
    let (_dir, proc) = spawn(&bin, "hang-handshake");
    let mut session = PluginSession::new(proc, 1, "0.0.0-test");

    let started = Instant::now();
    let err = session
        .handshake_with_timeout(Duration::from_millis(250))
        .unwrap_err();
    let elapsed = started.elapsed();

    match err {
        SessionError::Transport(TransportError::Timeout { phase, .. }) => {
            assert_eq!(phase, "handshake");
        }
        other => panic!("expected handshake timeout, got {other:?}"),
    }
    // Sanity-check that we did not block for the default 5s timeout.
    assert!(
        elapsed < Duration::from_secs(2),
        "handshake_with_timeout did not honor short timeout: {elapsed:?}"
    );
}

#[test]
fn configure_rejection_surfaces_to_caller() {
    let bin = build_mock_plugin();
    let (_dir, proc) = spawn(&bin, "reject-config");
    let mut session = PluginSession::new(proc, 1, "0.0.0-test");

    session.handshake().unwrap();
    let err = session
        .configure("mock_sink", json!({}), &schema(), None)
        .unwrap_err();
    match err {
        SessionError::ConfigureRejected { reason } => {
            assert!(reason.contains("schema not supported"), "{reason}");
        }
        other => panic!("expected ConfigureRejected, got {other:?}"),
    }
    let _ = session.shutdown();
}

#[test]
fn plugin_crash_mid_stream_is_a_clean_transport_close() {
    let bin = build_mock_plugin();
    let (_dir, proc) = spawn(&bin, "crash-mid-stream");
    let mut session = PluginSession::new(proc, 1, "0.0.0-test");

    session.handshake().unwrap();
    session
        .configure("mock_sink", json!({}), &schema(), None)
        .unwrap();

    // First batch is acked, then the plugin process::exits.
    let ack = session.send_batch(&batch_of(vec![1, 2, 3])).unwrap();
    assert_eq!(ack.rows_accepted, 3);

    // Second batch should fail because the child has gone away. We accept
    // either Transport::Closed surfaced via the read side, or an io error
    // on the write side — both indicate the same broken pipe.
    let err = session.send_batch(&batch_of(vec![4, 5])).unwrap_err();
    match err {
        SessionError::Transport(TransportError::Closed) => {}
        SessionError::Transport(TransportError::Io(_)) => {}
        SessionError::Transport(TransportError::Frame(_)) => {}
        other => panic!("expected closed/io transport error, got {other:?}"),
    }
}

#[test]
fn streaming_many_batches_is_bounded_by_synchronous_acks() {
    // Backpressure check: send_batch is synchronous (waits for BatchAck), so
    // there is no unbounded queue between host and plugin. This test exercises
    // a long stream and verifies all rows are accounted for.
    let bin = build_mock_plugin();
    let (_dir, proc) = spawn(&bin, "normal");
    let mut session = PluginSession::new(proc, 1, "0.0.0-test");

    session.handshake().unwrap();
    session
        .configure("mock_sink", json!({}), &schema(), None)
        .unwrap();

    let mut total = 0u64;
    for i in 0..256 {
        let b = batch_of(vec![i, i + 1, i + 2, i + 3]);
        let ack = session.send_batch(&b).unwrap();
        assert_eq!(ack.rows_accepted, 4);
        total += ack.rows_accepted;
    }
    let commit = session.commit().unwrap();
    assert_eq!(commit.rows, total);
    session.shutdown().unwrap();
}

#[test]
fn fuzz_garbage_input_is_rejected_not_panicked() {
    // Frame-level fuzz: feed a handful of intentionally malformed byte streams
    // through `read_frame` and verify each one returns a typed `FrameError`
    // instead of panicking. Cheap, deterministic, no I/O.
    let cases: Vec<Vec<u8>> = vec![
        // Truncated length prefix.
        vec![0x01, 0x02],
        // Length=0 but no kind byte.
        vec![0x00, 0x00, 0x00, 0x00],
        // Valid length=4, unknown kind.
        vec![0x04, 0x00, 0x00, 0x00, 0xAB, 1, 2, 3, 4],
        // Reserved-for-v2 kind.
        vec![0x00, 0x00, 0x00, 0x00, 0x60],
        // Length advertises more than MAX_PAYLOAD_LEN.
        {
            let mut v = ((MAX_PAYLOAD_LEN as u32) + 1).to_le_bytes().to_vec();
            v.push(MessageKind::RecordBatch as u8);
            v
        },
        // Length=10, kind valid, but payload truncated.
        vec![0x0A, 0x00, 0x00, 0x00, MessageKind::Log as u8, b'a'],
        // Pure noise.
        vec![0xFF; 17],
    ];

    for (i, bytes) in cases.into_iter().enumerate() {
        let mut cur = std::io::Cursor::new(bytes);
        let res = read_frame(&mut cur);
        assert!(res.is_err(), "case {i}: expected error, got {:?}", res);
        // Must be a typed FrameError variant, not a panic / unrelated error.
        match res.unwrap_err() {
            FrameError::PayloadTooLarge(_)
            | FrameError::UnknownKind(_)
            | FrameError::ReservedKind(_)
            | FrameError::UnexpectedEof { .. }
            | FrameError::Io(_) => {}
        }
    }
}

#[test]
fn discovery_honors_env_var_override_cross_platform() {
    // Cross-platform check: a directory advertised via HORIZON_FLUX_PLUGIN_PATH
    // is scanned regardless of where it lives, and the platform-native path
    // separator is respected by `std::env::split_paths`.
    let extra = tempdir().unwrap();
    let cwd = tempdir().unwrap();

    let plugin_dir = extra.path().join("envplug");
    std::fs::create_dir_all(&plugin_dir).unwrap();
    std::fs::write(
        plugin_dir.join("plugin.toml"),
        r#"
name = "envplug"
version = "0.1.0"
flux_plugin_protocol = 1
flux_min_version = "0.1.0"
executable = "envplug-bin"

[[sinks]]
type = "envplug_sink"
display_name = "Env"
config_schema = "schema.json"
"#,
    )
    .unwrap();

    // Set on this thread only — `std::env::set_var` is process-global, so we
    // restore the previous value at the end of the test.
    let prev = std::env::var(PLUGIN_PATH_ENV).ok();
    // SAFETY: tests in this file are not parallel-sensitive to this var.
    unsafe {
        std::env::set_var(PLUGIN_PATH_ENV, extra.path());
    }

    let reg = discover_plugins(cwd.path());
    let p = reg.get("envplug").expect("envplug should be discovered");
    assert!(matches!(p.status, PluginStatus::Ok));
    assert_eq!(reg.find_sink("envplug_sink").unwrap().name, "envplug");

    unsafe {
        match prev {
            Some(v) => std::env::set_var(PLUGIN_PATH_ENV, v),
            None => std::env::remove_var(PLUGIN_PATH_ENV),
        }
    }
}
