<!--
Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Testing a Plugin

Plugins are processes that speak a stdin/stdout protocol, which makes them
easy to test in two ways:

1. **In-process round-trip** — drive your `Sink` directly without spawning
   anything. Fastest, easiest to debug, runs in milliseconds.
2. **Black-box subprocess** — `cargo build` your binary and pipe real frames
   through it. Slower, but verifies the actual on-disk artifact.

You should write at least one test of each kind. The Rust SDK is built so
both are short.

> A `armillary-plugin-test-harness` crate (and PyPI package) with built-in
> fault injection are on the deferred backlog
> (`planning/22-deferred-backlog.md`). Until those land, the patterns
> below are everything you need.

## In-process: `run_io` against `Cursor` / `Vec<u8>`

[`armillary_plugin_sdk::run_io`](../../crates/armillary-plugin-sdk/src/lib.rs) is the
I/O-generic version of `run`. It takes any `Read` and `Write`, so a test
can build the input frame stream as a `Vec<u8>`, hand it to your sink via a
`Cursor`, and inspect what the SDK wrote back to a second `Vec<u8>`.

The SDK's own unit tests are the canonical example of the pattern — read
[`crates/armillary-plugin-sdk/src/lib.rs`](../../crates/armillary-plugin-sdk/src/lib.rs)'s
`mod tests` for a fully worked happy path, configure-rejection case, and
host-abort case. The shape is:

```rust
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use armillary_plugin_protocol::arrow_ipc::{encode_record_batch, encode_schema_b64};
use armillary_plugin_protocol::{
    ConfigureSink, Hello, MessageKind, PROTOCOL_VERSION, write_frame, write_json_frame,
};
use armillary_plugin_sdk::{PluginInfo, run_io};

#[test]
fn happy_path() {
    // 1. Build a "host" input stream containing the frames the SDK expects.
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();

    let mut input: Vec<u8> = Vec::new();
    write_json_frame(&mut input, MessageKind::Hello, &Hello {
        protocol: PROTOCOL_VERSION, armillary_version: "test".into(),
    }).unwrap();
    write_json_frame(&mut input, MessageKind::ConfigureSink, &ConfigureSink {
        sink_type: "my_sink".into(),
        config: serde_json::json!({ "output": "/tmp/x" }),
        input_schema_ipc_b64: encode_schema_b64(&schema).unwrap(),
    }).unwrap();
    write_frame(&mut input, MessageKind::RecordBatch,
                &encode_record_batch(&batch).unwrap()).unwrap();
    write_json_frame(&mut input, MessageKind::Commit, &serde_json::json!({})).unwrap();
    write_json_frame(&mut input, MessageKind::Shutdown, &serde_json::json!({})).unwrap();

    // 2. Drive the SDK against the input stream and capture its output.
    let mut output: Vec<u8> = Vec::new();
    run_io(
        PluginInfo { name: "my_sink".into(), version: "0.1.0".into() },
        MySink::default(),
        &mut Cursor::new(input),
        &mut output,
    ).unwrap();

    // 3. Inspect `output` to assert the protocol responses match.
    //    See crates/armillary-plugin-sdk/src/lib.rs `mod tests` for the full
    //    pattern using `read_json_frame` to decode the responses.
    assert!(!output.is_empty());
}
```

The dev-dependencies you need:

```toml
[dev-dependencies]
armillary-plugin-protocol = { git = "https://github.com/cache-has/armillary", branch = "main" }
serde_json = "1"
arrow = "55"
```

`armillary-plugin-protocol` is the same shared crate the SDK depends on, so its
encoders (`encode_record_batch`, `encode_schema_b64`, `write_json_frame`)
guarantee the test bytes match what the host actually sends.

### What this catches

- Schema and config rejection paths in your `configure`.
- Bad `WriteFailed` / `Fatal` mappings in `write_batch`.
- Forgetting to call `commit`'s effects (file flush, rename, etc).
- Stat tracking (`WriteStats.rows_written`, `bytes_written`).

### What this does **not** catch

- Stdout buffering bugs (`Vec<u8>` is unbuffered — a real OS pipe is not).
  This is exactly the class of bug that bit the SDK during the parquet
  port: SDK tests passed but the real subprocess deadlocked because
  `stdout` wasn't being flushed. **You must also have a black-box test.**
- Anything to do with process spawning, working directory, or environment.

## Black-box: spawn the real binary

This is the pattern used by
[`examples/plugins/parquet-plugin/tests/lifecycle.rs`](../../examples/plugins/parquet-plugin/tests/lifecycle.rs).
Read it as the worked example; the steps are:

1. Add `armillary-plugin-host` and `tempfile` as **`dev-dependencies`** (they
   should not be runtime deps — `armillary-plugin-host` pulls in armillary internals).
2. In the test, `cargo build --bin <your-binary>` from the test, so the
   binary is fresh on every run.
3. Stage a tempdir containing the built binary, your real `plugin.toml`,
   and your real `config_schema.json`. Patch the manifest's `executable =`
   line so it matches the staged filename (this matters on Windows where
   the suffix is `.exe`).
4. Use [`PluginProcess::spawn_with_manifest`](../../crates/armillary-plugin-host/src/process.rs)
   plus [`PluginSession`](../../crates/armillary-plugin-host/src/session.rs) to
   drive the lifecycle: `handshake → configure → send_batch → commit → shutdown`.
5. Read the artifact your plugin produced and assert on its contents.

The minimal `Cargo.toml` additions:

```toml
[dev-dependencies]
armillary-plugin-host = { git = "https://github.com/cache-has/armillary", branch = "main" }
tempfile = "3"
```

This catches everything the in-process test misses: real OS pipes, real
buffering, real process exit codes, the real on-disk artifact.

### Note on armillary-plugin-host as a dev-dep

Pulling `armillary-plugin-host` in for tests adds a non-trivial dependency tree.
That is the price of asserting on the spawned binary. If you only want a
smoke test that the binary launches and completes a handshake, you can
also write a tiny shell script using `armillary plugin check
<plugin-name>` after staging the plugin into a directory listed in
`ARMILLARY_PLUGIN_PATH`.

## Testing a non-Rust plugin

The same two patterns work; you write the framing helpers in your own
language. The Python tutorial in
[`your-first-plugin-direct.md`](./your-first-plugin-direct.md) already
contains a full `read_frame` / `write_frame` / `write_json` set you can
copy into a `pytest` or `unittest` file.

A subprocess test in any language is a six-step recipe:

1. Build (or symlink) the plugin's executable into a tempdir.
2. Spawn it with `stdin` and `stdout` as pipes.
3. Write the `Hello`, `ConfigureSink`, `RecordBatch*`, `Commit`, `Shutdown`
   frames to stdin (flush after each!).
4. Read frames from stdout and assert the kinds and JSON shapes.
5. Wait for the process to exit cleanly.
6. Read the plugin's output artifact and assert its contents.

For a real-world TypeScript example see
`openboard/plugins/armillary/test/` in the openboard repo, which exercises the
OpenBoard plugin's full lifecycle this way.

## CI

For Rust plugins, both kinds of tests run under `cargo test`. Wire it up
the same way the parquet plugin does — its `tests/lifecycle.rs` runs in
armillary's main CI on every PR (success criterion §5 in `planning/26`), so
protocol regressions are caught at merge time.
