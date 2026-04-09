<!--
Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Horizon Flux Plugin Protocol — v1

This document is the normative reference for the wire protocol spoken between
the `flux` host process and a plugin subprocess. It is the contract plugin
authors build against.

> **Stability:** Protocol version `1` is **unstable until flux 1.0**. Breaking
> changes between flux minor versions are allowed and will bump the protocol
> version. After 1.0, version N is supported for at least one major cycle
> after N+1 ships. See `planning/24-plugin-system.md` for the rationale.

## 1. Transport

A plugin is an executable launched by `flux` as a child process. The host
communicates with the plugin over the child's `stdin` (host → plugin) and
`stdout` (plugin → host). The plugin's `stderr` is captured and forwarded into
the host's `tracing` infrastructure as plugin diagnostics; it is **not** part
of the protocol and must never carry framed messages.

Both control messages and Arrow data flow over the same byte stream, tagged by
a one-byte message kind in the frame header.

### 1.1 Framing

Every message on the wire uses the same frame layout:

```
+----------------+--------+----------------+
| length (u32 LE)|  kind  |    payload     |
|     4 bytes    | 1 byte | length bytes   |
+----------------+--------+----------------+
```

- `length` is the byte length of `payload` only. It does **not** include the
  kind byte or itself.
- `length` is unsigned little-endian. Maximum payload size is 64 MiB
  (`0x0400_0000`); larger frames must be rejected as a protocol violation.
- `kind` is one of the values in §2.
- `payload` is interpreted per `kind`. Control payloads are UTF-8 JSON
  (see §3). Data payloads are Arrow IPC stream bytes (see §4).

A reader that encounters EOF mid-frame must treat the connection as broken
and surface a protocol error. A reader that encounters an unknown `kind` byte
in v1 must respond with an `Error` frame (kind `0x51`, plugin → host) or, if
the host is the receiver, log a warning and ignore the frame to preserve
forward compatibility with reserved kind ranges (§2.2).

### 1.2 Why JSON for control, not Protobuf

Control messages are tiny and infrequent (Hello, ConfigureSink, Commit,
Abort, Shutdown, plus per-batch acks). JSON keeps the plugin SDK surface
trivial in every language: no `protoc`, no codegen step, no `prost`/`prost-build`
build dependency, and natural additive evolution. Arrow IPC is still used for
RecordBatch payloads, so zero-copy semantics are unaffected.

This decision is revisited at flux 1.0. If schema evolution proves painful,
control messages can be migrated to a binary format under a new protocol
version without disturbing the framing layer.

## 2. Message Kinds

### 2.1 v1 kinds

| Kind   | Name          | Direction      | Payload format     |
|--------|---------------|----------------|--------------------|
| `0x01` | Hello         | host → plugin  | JSON               |
| `0x02` | HelloAck      | plugin → host  | JSON               |
| `0x10` | ConfigureSink | host → plugin  | JSON               |
| `0x11` | ConfigureAck  | plugin → host  | JSON               |
| `0x15` | DeclareResource | plugin → host | JSON              |
| `0x20` | RecordBatch   | host → plugin  | Arrow IPC stream   |
| `0x21` | BatchAck      | plugin → host  | JSON               |
| `0x30` | Commit        | host → plugin  | JSON (`{}`)        |
| `0x31` | CommitAck     | plugin → host  | JSON               |
| `0x40` | Abort         | host → plugin  | JSON               |
| `0x41` | AbortAck      | plugin → host  | JSON (`{}`)        |
| `0x50` | Log           | plugin → host  | JSON               |
| `0x51` | Error         | plugin → host  | JSON               |
| `0xF0` | Shutdown      | host → plugin  | JSON (`{}`)        |

### 2.2 Reserved ranges (v2+)

These ranges MUST NOT be used by v1 implementations. They are reserved so
source-plugin support in v2 is non-breaking:

| Range         | Reserved for                          |
|---------------|---------------------------------------|
| `0x60`–`0x6F` | Source schema introspection           |
| `0x70`–`0x7F` | Source pushdown negotiation           |
| `0x80`–`0x8F` | Source lifecycle (streaming/bounded)  |

All other kinds are reserved and must be rejected.

## 3. Control Payloads (JSON)

All JSON payloads are UTF-8, with no BOM, no trailing newline required, and
unknown fields ignored by the receiver (additive evolution). All field names
are `snake_case`.

### 3.1 `Hello` (host → plugin)

```json
{
  "protocol": 1,
  "flux_version": "0.5.0"
}
```

### 3.2 `HelloAck` (plugin → host)

```json
{
  "protocol": 1,
  "plugin_name": "openboard",
  "plugin_version": "0.1.0",
  "capabilities": {
    "transactional": true,
    "upsert": false,
    "schema_validation": true
  }
}
```

If the plugin's `protocol` does not equal the host's, the host kills the
subprocess and reports a clear error. There is no negotiation in v1.

### 3.3 `ConfigureSink` (host → plugin)

```json
{
  "sink_type": "openboard_duckdb",
  "config": { /* user-supplied config, secrets already substituted */ },
  "input_schema_ipc_b64": "<base64 of an Arrow IPC Schema message>"
}
```

The upstream Arrow schema is delivered as a base64-encoded Arrow IPC
`Schema` message so it can travel inside the JSON payload without a separate
frame. RecordBatches that follow MUST conform to this schema.

### 3.4 `ConfigureAck` (plugin → host)

```json
{ "accepted": true }
```

or

```json
{ "accepted": false, "reason": "duckdb file is not writable" }
```

A rejection causes the pipeline node to fail fast.

### 3.5 `BatchAck` (plugin → host)

```json
{ "rows_accepted": 1024, "warning": null }
```

`warning` is an optional human-readable string that is logged but does not
fail the node.

### 3.6 `Commit` / `CommitAck`

`Commit` payload is `{}`. `CommitAck`:

```json
{
  "rows": 12345,
  "bytes": 9876543,
  "duration_ms": 412
}
```

### 3.7 `Abort` / `AbortAck`

```json
{ "reason": "upstream node failed" }
```

`AbortAck` payload is `{}`. If the plugin does not respond within the abort
timeout (default 5s), the host kills the subprocess.

### 3.8 `Log` (plugin → host)

```json
{ "level": "info", "message": "wrote partition 2026-04-08" }
```

`level` is one of `trace`, `debug`, `info`, `warn`, `error`. Forwarded into
the host's `tracing` infrastructure tagged with the plugin name.

### 3.9 `Error` (plugin → host)

```json
{ "message": "duckdb commit failed: disk full", "details": null }
```

An `Error` frame is fatal: it causes the pipeline node to fail and the host
to send `Shutdown` (or `SIGKILL` if shutdown also fails).

### 3.10 `Shutdown` (host → plugin)

Payload is `{}`. The plugin must exit with status `0` within the shutdown
timeout (default 5s) or be killed.

### 3.11 `DeclareResource` (plugin → host)

```json
{ "resource_fingerprint": "postgres://db.example.com:5432/analytics/public.orders" }
```

Optional. Sent after configuration succeeds (before `ConfigureAck` or
immediately after) to declare the canonical, secret-free identifier of the
resource this sink writes to. The host uses this fingerprint for cross-pipeline
lineage tracking (see `planning/31-cross-pipeline-lineage.md`).

Plugins that do not send `DeclareResource` do not participate in static
lineage; their cross-pipeline relationships can still be discovered via
runtime-observed lineage.

The fingerprint must follow the same canonicalization rules as built-in
connectors: absolute paths, lowercased hostnames, no credentials. See
`crates/flux-connectors/src/fingerprint.rs` for examples.

## 4. Data Payloads (Arrow IPC)

`RecordBatch` frames carry the bytes of an Arrow IPC **stream-format**
message containing exactly one `RecordBatch`. The schema is the one declared
in `ConfigureSink.input_schema_ipc_b64`. Plugins MAY map these bytes
zero-copy if their Arrow library supports it.

The host MUST NOT send dictionary-batch messages mid-stream in v1; if a
column requires a dictionary, the dictionary is delivered inline with each
batch (Arrow IPC `write_legacy_ipc_format = false`, dictionaries written
unified per batch).

## 5. Lifecycle

```
host                                   plugin
 |  spawn(executable, args, env)         |
 |--------------------------------------▶|
 |  Hello (0x01)                         |
 |--------------------------------------▶|
 |                          HelloAck     |
 |◀--------------------------------------|
 |  ConfigureSink (0x10)                 |
 |--------------------------------------▶|
 |              DeclareResource (0x15)?  |  ← optional
 |◀--------------------------------------|
 |                       ConfigureAck    |
 |◀--------------------------------------|
 |  RecordBatch (0x20)   ─┐              |
 |  RecordBatch (0x20)    │  streaming   |
 |  ...                  ─┘              |
 |              BatchAck / Log / Error   |
 |◀--------------------------------------|
 |  Commit (0x30)                        |
 |--------------------------------------▶|
 |                          CommitAck    |
 |◀--------------------------------------|
 |  Shutdown (0xF0)                      |
 |--------------------------------------▶|
 |                            (exit 0)   |
```

On any error the host sends `Abort` instead of `Commit`, waits up to the
abort timeout for `AbortAck`, then sends `Shutdown` (and kills on timeout).

### 5.1 Timeouts (defaults)

| Phase             | Default |
|-------------------|---------|
| Handshake         | 5 s     |
| ConfigureAck      | 30 s    |
| BatchAck (idle)   | none — backpressure governs |
| CommitAck         | 5 min   |
| Abort / Shutdown  | 5 s     |

All timeouts are host-side and configurable in the plugin manifest in a
later revision; v1 ships the defaults above.

## 6. Errors and Crashes

- **Subprocess exits non-zero before `Shutdown`** → node fails with the exit
  code and the captured tail of `stderr`.
- **Subprocess exits zero before `CommitAck`** → node fails with
  "plugin exited prematurely".
- **Protocol violation** (bad frame, unknown v1 kind, oversized payload) →
  node fails, plugin is killed.
- **Host-side `Error` frame** → node fails with the plugin's message.
- **Timeout** → node fails with the phase name and the configured timeout.

In every case the host process itself remains healthy and the failure is
reported on the offending node only.
