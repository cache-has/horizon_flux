// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! A trivial in-tree plugin used by the host's integration test.
//!
//! It speaks the v1 protocol on stdin/stdout: replies to `Hello`,
//! `ConfigureSink`, every `RecordBatch`, `Commit`, and exits on `Shutdown`.
//! It also emits one `Log` frame so we exercise the host's log forwarding.
//!
//! Failure-mode hooks (driven by `--mode <name>` or `MOCK_PLUGIN_MODE` env):
//!
//! - `normal` (default) — full happy path.
//! - `hang-handshake` — read `Hello` then sleep forever (host should time out).
//! - `reject-config` — handshake OK, then send `ConfigureAck { accepted: false }`.
//! - `crash-mid-stream` — handshake + configure OK, ack the first batch,
//!   then `process::exit(2)` mid-stream so the host sees a closed transport.

use std::io::{self, Write};

use armillary_plugin_host::arrow_ipc::decode_record_batch;
use armillary_plugin_host::protocol::control::{
    BatchAck, CommitAck, ConfigureAck, Hello, HelloAck, Log, LogLevel,
};
use armillary_plugin_host::protocol::{MessageKind, read_frame, write_frame};

fn write_json<W: Write, V: serde::Serialize>(w: &mut W, kind: MessageKind, v: &V) {
    let bytes = serde_json::to_vec(v).unwrap();
    write_frame(w, kind, &bytes).unwrap();
    w.flush().unwrap();
}

fn resolve_mode() -> String {
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--mode" {
            if let Some(v) = args.next() {
                return v;
            }
        } else if let Some(v) = a.strip_prefix("--mode=") {
            return v.to_string();
        }
    }
    std::env::var("MOCK_PLUGIN_MODE").unwrap_or_else(|_| "normal".into())
}

fn main() {
    let mode = resolve_mode();
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut r = stdin.lock();
    let mut w = stdout.lock();

    // 1. Hello
    let frame = read_frame(&mut r).expect("hello frame");
    assert_eq!(frame.kind, MessageKind::Hello);
    let _hello: Hello = serde_json::from_slice(&frame.payload).unwrap();

    if mode == "hang-handshake" {
        // Read the Hello but never reply. The host's handshake timeout fires.
        loop {
            std::thread::sleep(std::time::Duration::from_secs(60));
        }
    }

    write_json(
        &mut w,
        MessageKind::HelloAck,
        &HelloAck {
            protocol: 1,
            plugin_name: "mock".into(),
            plugin_version: "0.1.0".into(),
            capabilities: Default::default(),
        },
    );

    // Demonstrate log forwarding.
    write_json(
        &mut w,
        MessageKind::Log,
        &Log {
            level: LogLevel::Info,
            message: "mock plugin online".into(),
        },
    );

    // 2. ConfigureSink
    let frame = read_frame(&mut r).expect("configure frame");
    assert_eq!(frame.kind, MessageKind::ConfigureSink);

    if mode == "reject-config" {
        write_json(
            &mut w,
            MessageKind::ConfigureAck,
            &ConfigureAck {
                accepted: false,
                reason: Some("schema not supported".into()),
            },
        );
        // Wait for the host to clean up; exit on shutdown or pipe close.
        let _ = read_frame(&mut r);
        return;
    }

    write_json(
        &mut w,
        MessageKind::ConfigureAck,
        &ConfigureAck {
            accepted: true,
            reason: None,
        },
    );

    // 3. Stream
    let mut total_rows: u64 = 0;
    let mut batches_seen: u64 = 0;
    loop {
        let frame = read_frame(&mut r).expect("loop frame");
        match frame.kind {
            MessageKind::RecordBatch => {
                let batch = decode_record_batch(&frame.payload).expect("batch decode");
                total_rows += batch.num_rows() as u64;
                write_json(
                    &mut w,
                    MessageKind::BatchAck,
                    &BatchAck {
                        rows_accepted: batch.num_rows() as u64,
                        warning: None,
                    },
                );
                batches_seen += 1;
                if mode == "crash-mid-stream" && batches_seen == 1 {
                    // Hard exit so the host's reader sees a closed pipe.
                    std::process::exit(2);
                }
            }
            MessageKind::Commit => {
                write_json(
                    &mut w,
                    MessageKind::CommitAck,
                    &CommitAck {
                        rows: total_rows,
                        bytes: 0,
                        duration_ms: 0,
                        rows_updated: 0,
                        rows_deleted: 0,
                    },
                );
            }
            MessageKind::Shutdown => break,
            other => panic!("unexpected frame {other:?}"),
        }
    }
}
