<!--
Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Your First Plugin (No SDK)

This is the parallel version of [your-first-plugin-rust.md](./your-first-plugin-rust.md)
for any language that **isn't** Rust. The armillary plugin protocol is designed
so that any language with an Arrow IPC library and the standard library can
implement a plugin in a single file.

This walkthrough does it twice — once in **Python** and once in
**TypeScript**. They are independent; pick whichever fits your stack.

> **Read this alongside [protocol-v1.md](./protocol-v1.md).** This document
> shows you the lifecycle in code; the protocol doc is the normative
> reference for every field, message kind, and edge case.

## What you are building

The same `csv-line-count` sink as the Rust tutorial: count rows in every
incoming `RecordBatch` and append `<unix_secs> <row_count>` to a file when
the pipeline commits.

## Lifecycle reminder

```
host → plugin                  plugin → host
─────────────                  ─────────────
Hello (0x01, JSON)
                               HelloAck (0x02, JSON)
ConfigureSink (0x10, JSON)
                               ConfigureAck (0x11, JSON)
RecordBatch (0x20, Arrow IPC)
                               BatchAck (0x21, JSON)
RecordBatch (0x20, Arrow IPC)
                               BatchAck (0x21, JSON)
... (repeats) ...
Commit (0x30, JSON {})
                               CommitAck (0x31, JSON)
Shutdown (0xF0, JSON {})       (process exits 0)
```

Every frame on the wire is `length: u32 LE | kind: u8 | payload`. Control
payloads are UTF-8 JSON; `RecordBatch` payloads are Arrow IPC **stream**
bytes carrying exactly one batch. `stderr` is captured by the host and
forwarded into its `tracing` logs — write diagnostics there freely.

---

## Python

Requires Python 3.10+ and `pyarrow`.

```bash
pip install pyarrow   # or: uv pip install pyarrow
```

`armillary_csv_line_count.py`:

```python
#!/usr/bin/env python3
import json
import struct
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

PROTOCOL_VERSION = 1
MAX_PAYLOAD = 0x04000000  # 64 MiB, must match the host

# Message kinds (see docs/plugins/protocol-v1.md §2)
HELLO, HELLO_ACK = 0x01, 0x02
CONFIGURE, CONFIGURE_ACK = 0x10, 0x11
RECORD_BATCH, BATCH_ACK = 0x20, 0x21
COMMIT, COMMIT_ACK = 0x30, 0x31
ABORT, ABORT_ACK = 0x40, 0x41
ERROR, SHUTDOWN = 0x51, 0xF0

# ---- framing ---------------------------------------------------------------

def read_exact(stream, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise EOFError("plugin: host closed stdin")
        buf.extend(chunk)
    return bytes(buf)

def read_frame(stream):
    header = read_exact(stream, 5)
    length = struct.unpack("<I", header[:4])[0]
    kind = header[4]
    if length > MAX_PAYLOAD:
        raise ValueError(f"oversized frame: {length}")
    return kind, read_exact(stream, length)

def write_frame(stream, kind: int, payload: bytes) -> None:
    stream.write(struct.pack("<I", len(payload)))
    stream.write(bytes([kind]))
    stream.write(payload)
    stream.flush()  # the host blocks until each frame arrives

def write_json(stream, kind: int, obj) -> None:
    write_frame(stream, kind, json.dumps(obj).encode("utf-8"))

# ---- main ------------------------------------------------------------------

def main() -> int:
    stdin, stdout = sys.stdin.buffer, sys.stdout.buffer

    # 1. Handshake
    kind, payload = read_frame(stdin)
    assert kind == HELLO, f"expected Hello, got {kind:#x}"
    hello = json.loads(payload)
    if hello["protocol"] != PROTOCOL_VERSION:
        write_json(stdout, ERROR,
                   {"message": f"protocol mismatch: host={hello['protocol']}, plugin={PROTOCOL_VERSION}"})
        return 1
    write_json(stdout, HELLO_ACK, {
        "protocol": PROTOCOL_VERSION,
        "plugin_name": "csv-line-count",
        "plugin_version": "0.1.0",
        "capabilities": {"schema_validation": True},
    })

    # 2. Configure
    kind, payload = read_frame(stdin)
    assert kind == CONFIGURE, f"expected ConfigureSink, got {kind:#x}"
    cfg_msg = json.loads(payload)
    try:
        config = cfg_msg["config"]
        output = Path(config["output"])
        # The schema is delivered base64-encoded inside the configure message.
        import base64
        schema_bytes = base64.b64decode(cfg_msg["input_schema_ipc_b64"])
        schema = ipc.read_schema(pa.BufferReader(schema_bytes))
        if len(schema) == 0:
            raise ValueError("empty input schema")
    except Exception as e:
        write_json(stdout, CONFIGURE_ACK, {"accepted": False, "reason": str(e)})
        return 0
    write_json(stdout, CONFIGURE_ACK, {"accepted": True})

    # 3. Stream / commit / abort
    rows = 0
    started = time.monotonic()
    while True:
        kind, payload = read_frame(stdin)

        if kind == RECORD_BATCH:
            reader = ipc.RecordBatchStreamReader(pa.BufferReader(payload))
            batch = reader.read_next_batch()
            rows += batch.num_rows
            write_json(stdout, BATCH_ACK, {"rows_accepted": batch.num_rows, "warning": None})

        elif kind == COMMIT:
            line = f"{int(time.time())} {rows}\n"
            with output.open("a") as f:
                f.write(line)
            write_json(stdout, COMMIT_ACK, {
                "rows": rows,
                "bytes": len(line),
                "duration_ms": int((time.monotonic() - started) * 1000),
            })
            # Hold the pipe open until Shutdown.
            while True:
                k, _ = read_frame(stdin)
                if k == SHUTDOWN:
                    return 0

        elif kind == ABORT:
            write_frame(stdout, ABORT_ACK, b"")
            return 0

        elif kind == SHUTDOWN:
            return 0

        else:
            print(f"unexpected frame kind {kind:#x}", file=sys.stderr)
            return 1

if __name__ == "__main__":
    sys.exit(main())
```

`plugin.toml` (next to the script):

```toml
name = "csv-line-count"
version = "0.1.0"
armillary_plugin_protocol = 1
armillary_min_version = "0.1.0"

# The executable can be the script itself if it has a shebang and is +x;
# otherwise wrap it in a tiny launcher (`#!/usr/bin/env bash` → `python3 …`).
executable = "armillary-csv-line-count"

[[sinks]]
type = "csv_line_count"
display_name = "CSV Line Count (Python)"
config_schema = "config_schema.json"

[sinks.capabilities]
schema_validation = true
```

Make the script executable, then install it the same way as the Rust
tutorial:

```bash
chmod +x armillary_csv_line_count.py
mkdir -p csv-line-count
cp plugin.toml config_schema.json armillary_csv_line_count.py csv-line-count/
mv csv-line-count/armillary_csv_line_count.py csv-line-count/armillary-csv-line-count
PLUGIN_DIR="$(armillary plugin path | head -1)"
cp -r csv-line-count "$PLUGIN_DIR/"
armillary plugin check csv-line-count
```

(`config_schema.json` is identical to the one in the Rust tutorial.)

---

## TypeScript

The OpenBoard plugin in `openboard/plugins/armillary/` is a complete, real-world
TypeScript implementation of the v1 wire protocol. Rather than reproduce the
whole thing here, the canonical files to read and copy from are:

- [`openboard/plugins/armillary/src/protocol.ts`](https://github.com/horizon-analytic/openboard/blob/main/plugins/armillary/src/protocol.ts)
  — framing, message kind enum, JSON payload type definitions, and a
  `readFrame` / `writeFrame` pair against `node:stream` `Readable`/`Writable`.
  Drop this file into a new TypeScript project as-is and you have the
  protocol layer for free.
- [`openboard/plugins/armillary/src/sink.ts`](https://github.com/horizon-analytic/openboard/blob/main/plugins/armillary/src/sink.ts)
  — the lifecycle loop (`runPluginLoop`) that consumes frames from
  `process.stdin`, dispatches on `MessageKind`, and writes acks back to
  `process.stdout`. Strip out the DuckDB-specific logic and you have a
  ~100-line skeleton you can adapt to your sink.
- [`openboard/plugins/armillary/src/arrow_types.ts`](https://github.com/horizon-analytic/openboard/blob/main/plugins/armillary/src/arrow_types.ts)
  — how the OpenBoard plugin parses base64-encoded Arrow IPC schema bytes
  and `RecordBatch` payloads using `apache-arrow`.

The same protocol shapes from the Python example apply: framed messages
with a 5-byte header, JSON for control payloads, Arrow IPC stream bytes for
`RecordBatch`. The `protocol.ts` file already encodes every detail you need.

To run a TypeScript plugin under armillary it must be a launchable executable.
The OpenBoard plugin's `scripts/bundle.mjs` is the canonical pattern: it
bundles `src/index.ts` with `tsup`, marks the output `+x` with a
`#!/usr/bin/env node` shebang, and assembles a directory containing
`plugin.toml`, the bundled JS, and `node_modules/`. See
[`distribution.md`](./distribution.md) for the full pattern.

---

## What every implementation must get right

These are the rough edges that bite people writing a plugin from scratch.
The Rust SDK handles all of them; if you skip the SDK you have to handle
them yourself.

1. **Flush after every write.** Stdout is block-buffered when redirected.
   Forgetting to flush after each frame deadlocks the host instantly. The
   Rust SDK had this exact bug — it was only caught when the parquet plugin
   was ported to it (see planning/26).
2. **Read frame length as unsigned little-endian u32, payload only.** The
   length excludes both itself and the kind byte.
3. **Reject frames over 64 MiB (`0x04000000`).** Anything larger is a
   protocol violation, not just a config issue.
4. **`Commit` payload is `{}`, not empty.** Same for `Shutdown`. Always
   parse the payload as JSON, even if you don't use it.
5. **After `CommitAck`, hold the pipe open and wait for `Shutdown`.** Do
   not exit immediately — the host has not yet noticed your commit.
6. **`stderr` is for human diagnostics, never for protocol frames.** The
   host captures it as `tracing` events; anything you print there shows up
   in armillary's logs tagged with your plugin name.
7. **Schema is base64-encoded inside the JSON `ConfigureSink`.** Decode it
   as Arrow IPC stream bytes, not file format.
8. **`RecordBatch` payload is Arrow IPC stream format with exactly one
   batch.** Use your library's stream reader, read one batch, ack with
   the row count.

## Verifying

Whichever language you used, verify with the same armillary CLI commands as the
Rust tutorial:

```bash
armillary plugin list                    # see it appear
armillary plugin check csv-line-count    # spawn + handshake
```

Then drop it onto the canvas, connect a source, and run the pipeline.

## Where to go next

- [`testing.md`](./testing.md) — how to write integration tests by piping
  framed bytes into your plugin's stdin from a test harness in your
  language of choice.
- [`debugging.md`](./debugging.md) — how to enable protocol tracing on the
  armillary side and replay a captured input stream offline.
- [`protocol-v1.md`](./protocol-v1.md) — the full normative reference. If
  you got this far, this should now feel like a checklist rather than a
  spec.
