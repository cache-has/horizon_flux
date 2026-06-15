# `armillary-plugin-host` Architecture

Internal notes on how the host-side plugin runtime is layered. End-user docs
live in [`using-plugins.md`](./using-plugins.md); the wire format is in
[`protocol-v1.md`](./protocol-v1.md).

## Goals

1. Run plugin executables as isolated subprocesses so a crash never takes down
   armillary.
2. Move Arrow `RecordBatch`es across the boundary without copying or lossy
   re-encoding.
3. Keep the layers small enough that each one is independently testable
   (manifests without spawning, sessions without spawning, transports without
   protocol logic).
4. Stay language-agnostic — the host must not assume the plugin is Rust.

## Module map

```
crates/armillary-plugin-host/src/
  manifest.rs    plugin.toml parser → typed Manifest / SinkDeclaration
  discovery.rs   scan roots → Vec<DiscoveredPlugin> + PluginRegistry
  protocol/      [length][kind][payload] framing + control message types
  arrow_ipc.rs   Arrow IPC schema/batch (de)serialization
  transport.rs   Transport trait — async byte-stream abstraction
  process.rs     PluginProcess: child stdin/stdout as a Transport, stderr→tracing
  session.rs     PluginSession: sink lifecycle state machine over a Transport
  error.rs       crate-wide error types
```

The dependency direction is strictly downward: `session` knows about
`transport` and `protocol` but not `process`; `process` implements `Transport`
and is the only module that touches `tokio::process`. This makes
`PluginSession` testable against an in-memory duplex stream without spawning
real binaries (see `tests/failure_modes.rs`).

## Discovery

`discover_plugins` walks the scan roots returned by `discovery::scan_roots`
(env var override → workspace-local → user-global → legacy fallback), parses
each `plugin.toml`, and produces a `PluginRegistry` keyed by sink type. Invalid
manifests are retained as `DiscoveredPlugin { status: PluginStatus::Invalid,
.. }` so the UI can surface parse errors instead of silently dropping them.

The registry is wrapped in an `RwLock` inside `armillary-server` so
`POST /api/plugins/reload` can atomically swap a freshly scanned registry in
place without restarting armillary.

## Protocol layer

Frames are `[4-byte LE length][1-byte kind][payload]`. Control payloads are
JSON; `RecordBatch` payloads are Arrow IPC stream bytes. JSON was chosen over
Protobuf so plugin SDKs in any language need only a JSON parser, not a
toolchain-specific code generator. See `protocol-v1.md` §1.2 for the rationale.

`MAX_PAYLOAD_LEN` caps individual frame sizes; the framing decoder rejects
oversized headers before allocating, which is what the fuzz tests in
`failure_modes.rs::fuzz_garbage_input_is_rejected_not_panicked` exercise.

## Process layer

`PluginProcess::spawn` launches a plugin executable with the args, env, and
working directory from the manifest. The child's `stdin`/`stdout` are wired
into a `Transport` impl, and `stderr` is drained into `tracing` so plugin
diagnostics interleave with host logs.

Reserved exit handling:
- Clean exit after `Shutdown` → success.
- Premature exit (transport EOF mid-stream) → surfaced as a clean transport
  error to the session, not a panic.
- Hung handshake → `PluginSession::handshake_with_timeout` aborts and the host
  kills the child.

## Session layer

`PluginSession` is a state machine that mirrors the v1 sink lifecycle:

```
Spawned → Hello/HelloAck → Configured → (RecordBatch + BatchAck)* → Commit/CommitAck → Shutdown
                                                       ↘ Abort/AbortAck (on error)
```

`send_batch` is **fully synchronous** — it does not return until the plugin
acknowledges the batch. There is intentionally no host-side queue between
batches; backpressure is enforced by construction. The
`streaming_many_batches_is_bounded_by_synchronous_acks` test pins this as a
regression guard.

## Sink integration

`armillary-connectors::plugin_sink::PluginSink` is the adapter that makes plugin
sinks interchangeable with built-in ones. It implements `PipelineSink` by
delegating each lifecycle method to a `PluginSession`. The
`default_registry_with_plugins` constructor walks a `PluginRegistry` and
registers a shared `Arc<PluginSink>` under each plugin sink type so the
executor's existing registry lookup transparently finds them.

## Error surfacing

Errors land in three places:

1. **Manifest / discovery errors** stay attached to the `DiscoveredPlugin` and
   are exposed via `GET /api/plugins`, where the frontend renders them in red
   in the Plugins panel.
2. **Spawn / handshake errors** propagate as `armillary plugin check` exit codes
   and as pipeline-node errors at runtime.
3. **Mid-stream errors** (Configure rejected, plugin-reported `Error` frames,
   transport closed, host-side timeouts) all funnel through `SessionError` and
   become node-level pipeline failures with clear messages.

## Testing strategy

- **Unit tests** on `protocol/frame.rs` cover encode/decode and edge cases.
- **`failure_modes.rs`** drives a real mock plugin binary through every
  failure path (crash mid-stream, hung handshake, configure rejection, garbage
  bytes, large batch streams, env-var-driven discovery).
- **`examples/plugins/parquet-plugin/tests/lifecycle.rs`** is the end-to-end
  validation: spawn the real binary, run the full lifecycle, read the parquet
  output back, assert exact `RecordBatch` equality.
