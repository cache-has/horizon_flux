<!--
Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Debugging a Plugin

A plugin is a separate process talking to armillary over a stdio pipe, which
makes most failures fall into one of three buckets: **discovery**,
**handshake / configure**, or **mid-stream**. This page walks each one and
shows how to enable protocol tracing and replay a captured input stream
offline.

## First-line triage

Before opening a debugger, run these three commands in order. They take
seconds and pin down the failure to a phase.

```bash
armillary plugin path                  # Where will armillary look?
armillary plugin list                  # Did it find your plugin?
armillary plugin check <plugin-name>   # Spawn + handshake. Does it run?
```

| Command fails at... | Likely cause |
|---|---|
| `plugin list` doesn't show your plugin | Wrong directory, wrong directory name, or invalid `plugin.toml`. Check the **Plugins panel** in the canvas — invalid manifests are listed with a parse error. |
| `plugin list` shows it as `invalid` | Manifest validation failed. The error is in the panel and in armillary's logs. Common causes below. |
| `plugin check` exits non-zero | Your binary is broken: it didn't spawn, didn't speak v1, or didn't ack the `Hello`. Read its stderr (printed inline by `plugin check`). |
| `plugin check` is fine but the canvas shows nothing | Frontend's plugin cache is stale — click **Rescan** in the Plugins panel (or `POST /api/plugins/reload`) and refresh. |

## Common errors

### "directory name does not match manifest name"

The directory must be named exactly `<name>` from the manifest. Rename one
or the other.

### "config_schema not found"

The `config_schema` field in `plugin.toml` is resolved relative to the
plugin directory, not the working directory. If your manifest says
`config_schema = "config_schema.json"`, that file must sit next to
`plugin.toml`.

### "duplicate sink type"

Your `[[sinks]]` `type` collides with another plugin or with a built-in
connector. Pick a more specific name (e.g. `acme_postgres` instead of
`postgres`).

### "protocol mismatch: host speaks N, plugin speaks M"

Your plugin's `armillary_plugin_protocol` doesn't match this build of armillary.
Either rebuild the plugin against a matching SDK / protocol crate, or
upgrade/downgrade armillary. Until 1.0, protocol versions can change between
armillary minor releases.

### "Configure rejected: ..."

Your `Sink::configure` returned `Err`. The reason is right there in the
message. Common cases:

- **invalid config** — the user-supplied JSON didn't deserialize into your
  `Config` struct. Check the user's input against your `config_schema.json`.
- **schema mismatch** — your sink doesn't accept the upstream's column
  types. Either coerce upstream, or relax the check.

### "transport closed" mid-stream

Your plugin process died. Check its stderr in armillary's logs (see below).
The 95% case is one of:

1. **A panic** in a `Sink` method. Wrap the offending code and return
   `SinkError::Fatal(...)` instead — that surfaces a clean `Error` frame
   the host can report on the failing node.
2. **stdout buffering.** If you wrote a non-Rust plugin and forgot to
   flush after a frame, the host's `read_frame` blocks until your buffer
   fills (often forever). Add an explicit flush after every write. The
   Rust SDK does this for you.
3. **An OS-level kill.** Look for `SIGPIPE`, `SIGKILL`, or an OOM kill in
   your system logs.

### "plugin exited prematurely" before commit

Your plugin returned exit code 0 before sending `CommitAck`. The most
common cause is exiting on EOF instead of waiting for `Shutdown` after
commit. The lifecycle expects you to **hold the pipe open after
`CommitAck` until the host sends `Shutdown` (`0xF0`)**. The Rust SDK does
this in `drain_until_shutdown`; if you wrote your own loop, mirror that
behavior.

## Reading plugin logs

Plugin diagnostics flow into armillary through two channels and both end up in
the same `tracing` stream:

- **`Log` frames (`0x50`)**, sent by the plugin via `armillary_plugin_sdk::log::{trace,debug,info,warn,error}`
  in Rust, or by writing a `{ level, message }` JSON frame in another
  language.
- **`stderr` from the plugin process**, which `armillary-plugin-host` captures
  line-by-line and re-emits via `tracing`. This is where panics, prints,
  and unhandled exceptions go.

Turn on debug logging for plugin host and you will see both, tagged with
the plugin name:

```bash
RUST_LOG=armillary_plugin_host=debug,armillary=debug just dev-backend
```

For full protocol tracing (every frame the host reads and writes), bump
the host crate to `trace`:

```bash
RUST_LOG=armillary_plugin_host=trace just dev-backend
```

## Running a plugin standalone

Sometimes the easiest debugger is just running your plugin by hand and
piping bytes at it. Two patterns:

### Capture a real session

Wrap your plugin's executable in a tiny shim that `tee`s stdin and stdout
to files, then point the manifest at the shim:

```bash
#!/usr/bin/env bash
# capture-shim.sh
LOG_DIR="${HOME}/armillary-plugin-debug"
mkdir -p "$LOG_DIR"
tee "$LOG_DIR/in.bin" | ./armillary-my-plugin | tee "$LOG_DIR/out.bin"
```

Run a pipeline once. Now `in.bin` contains the exact byte stream the host
would send and `out.bin` contains exactly what your plugin replied. Both
are framed bytes you can decode with the same `read_frame` helper from
[`your-first-plugin-direct.md`](./your-first-plugin-direct.md).

### Replay offline

With `in.bin` in hand, you can debug your plugin without armillary at all:

```bash
./armillary-my-plugin < in.bin > /tmp/replay-out.bin
```

This is invaluable for reproducing crashes — attach a debugger
(`rust-lldb`, `gdb`, `pdb`) to the process and you have the same input as
the failing run, on demand.

You can also write a tiny driver in your plugin's language that constructs
the input frames from scratch — see the in-process `run_io` test in
[`testing.md`](./testing.md) for the Rust pattern. Five lines of code,
faster than `tee`-ing real runs.

## Asking for help

When you file an issue, include:

1. The output of `armillary plugin list` and `armillary plugin check
   <name>`.
2. The armillary build version (`armillary --version`).
3. Your plugin's `armillary_plugin_protocol` version from `plugin.toml`.
4. The relevant section of armillary's logs with `RUST_LOG=armillary_plugin_host=debug`
   enabled.
5. If possible, a captured `in.bin` from the shim above (it is just bytes,
   so it is small and self-contained).

That is enough to reproduce almost every plugin issue without access to
your environment.
