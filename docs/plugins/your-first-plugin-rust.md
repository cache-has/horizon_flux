<!--
Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Your First Plugin (Rust SDK)

This is a hands-on walkthrough for writing a Armillary **sink plugin** in
Rust using [`armillary-plugin-sdk`](../../crates/armillary-plugin-sdk). At the end you
will have a plugin built, installed, discovered by `armillary plugin list`,
visible in the canvas, and writing data to disk.

> Looking to do this without the SDK, in another language?
> See [your-first-plugin-direct.md](./your-first-plugin-direct.md). Looking
> for the wire protocol reference? See [protocol-v1.md](./protocol-v1.md).

> **In a hurry?** The [`armillary-plugin-template`](https://github.com/horizon-analytic/armillary-plugin-template)
> repo is a complete, working JSON Lines sink plugin built on this SDK,
> with cross-platform CI already wired up. Clone it and skip steps 1–3:
>
> ```bash
> gh repo create my-armillary-sink --template horizon-analytic/armillary-plugin-template --public --clone
> cd my-armillary-sink && cargo build --release
> ```
>
> Then jump to step 4 ("Install into armillary") below. The walkthrough that
> follows builds the same plugin from scratch so you understand each piece.

## What you are building

A `csv-line-count` sink: it counts rows in every incoming `RecordBatch` and
appends a one-line `<timestamp> <row_count>` summary to a file when the
pipeline commits. It is intentionally tiny — every other plugin you write
will have the same shape.

## 1. Scaffold the crate

Create a new binary crate **outside the armillary workspace**:

```bash
cargo new --bin csv-line-count
cd csv-line-count
```

Add the SDK and its required runtime dependencies to `Cargo.toml`. Until
`armillary-plugin-sdk` ships on crates.io, point at the in-tree path or a git
revision of `armillary`:

```toml
[package]
name = "csv-line-count"
version = "0.1.0"
edition = "2024"

[dependencies]
arrow = "55"
armillary-plugin-sdk = { git = "https://github.com/horizon-analytic/armillary", branch = "main" }
serde = { version = "1", features = ["derive"] }

[[bin]]
name = "armillary-csv-line-count"   # the executable name your manifest references
path = "src/main.rs"
```

The binary name (`armillary-csv-line-count`) is what `plugin.toml` will look for —
it does not have to match the crate name, and the `armillary-` prefix is just a
convention so the plugin is recognizable on `PATH` if a user installs it
there.

## 2. Implement `Sink`

The whole plugin is one `impl Sink` block plus a `main` that calls
[`run`](https://docs.rs/armillary-plugin-sdk). Replace `src/main.rs`:

```rust
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use serde::Deserialize;

use armillary_plugin_sdk::{PluginInfo, Sink, SinkError, WriteStats, log, run};

#[derive(Debug, Deserialize)]
struct Config {
    /// File the line count is appended to.
    output: PathBuf,
}

#[derive(Default)]
struct LineCountSink {
    config: Option<Config>,
    rows: u64,
    started: Option<Instant>,
}

impl Sink for LineCountSink {
    type Config = Config;

    fn configure(&mut self, config: Config, schema: &Schema) -> Result<(), SinkError> {
        if schema.fields().is_empty() {
            return Err(SinkError::InvalidConfig("input schema is empty".into()));
        }
        log::info(format!("counting rows into {}", config.output.display()));
        self.config = Some(config);
        self.started = Some(Instant::now());
        Ok(())
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SinkError> {
        self.rows += batch.num_rows() as u64;
        Ok(())
    }

    fn commit(&mut self) -> Result<WriteStats, SinkError> {
        let cfg = self
            .config
            .as_ref()
            .ok_or_else(|| SinkError::Fatal("commit before configure".into()))?;
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&cfg.output)
            .map_err(|e| SinkError::WriteFailed(Box::new(e)))?;
        let line = format!("{} {}\n", chrono_like_now(), self.rows);
        f.write_all(line.as_bytes())
            .map_err(|e| SinkError::WriteFailed(Box::new(e)))?;
        Ok(WriteStats {
            rows_written: self.rows,
            bytes_written: line.len() as u64,
            duration: self.started.take().map(|t| t.elapsed()).unwrap_or_default(),
        })
    }
}

fn chrono_like_now() -> String {
    // Avoid pulling in chrono just for the tutorial.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    secs.to_string()
}

fn main() -> ExitCode {
    run(
        PluginInfo {
            name: "csv-line-count".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        },
        LineCountSink::default(),
    )
}
```

That's the whole plugin. The SDK reads the wire protocol from stdin, drives
your `Sink` through `configure → write_batch* → commit | abort`, and writes
acks to stdout. You never touch a frame, a JSON message, or an Arrow IPC
buffer.

### What about errors?

Return `SinkError` from any method:

| Variant | When to use |
|---|---|
| `InvalidConfig(String)` | Bad config or unsupported schema during `configure`. The host turns this into `ConfigureAck { accepted: false }` and the pipeline node fails fast — clean, no crash. |
| `SchemaMismatch { expected, got }` | Same situation, but you specifically rejected the schema. |
| `WriteFailed(Box<dyn Error + Send + Sync>)` | An I/O or library error in `write_batch` / `commit`. Wraps any underlying error type. |
| `Fatal(String)` | Anything else you want to bail on. |

In the streaming and commit phases, any `Err` becomes a protocol `Error`
frame and the host fails the pipeline node — no panicking required.

## 3. Write the manifest and config schema

Armillary loads a plugin from a directory containing a `plugin.toml`, the binary,
and (optionally) a JSON Schema for the config form rendered in the canvas.
See [manifest.md](./manifest.md) for the full reference.

`plugin.toml`:

```toml
name = "csv-line-count"
version = "0.1.0"
description = "Counts rows from a pipeline and appends a summary line."
license = "MIT OR Apache-2.0"

armillary_plugin_protocol = 1
armillary_min_version = "0.1.0"

executable = "armillary-csv-line-count"

[[sinks]]
type = "csv_line_count"
display_name = "CSV Line Count"
description = "Append <timestamp> <row_count> to a file on commit."
config_schema = "config_schema.json"

[sinks.capabilities]
transactional = false
upsert = false
schema_validation = true
```

`config_schema.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "csv-line-count sink config",
  "type": "object",
  "required": ["output"],
  "properties": {
    "output": {
      "type": "string",
      "description": "File the row-count summary is appended to."
    }
  },
  "additionalProperties": false
}
```

The keys in `properties` must match the fields on your `Config` struct
exactly — that is the only contract between the canvas form and your code.

## 4. Build and install

```bash
cargo build --release
```

Stage the directory armillary will load:

```bash
mkdir -p ./csv-line-count
cp plugin.toml config_schema.json ./csv-line-count/
cp target/release/armillary-csv-line-count ./csv-line-count/
```

Copy it into the user-global plugin directory. `armillary plugin path`
prints the directories armillary scans, in priority order:

```bash
PLUGIN_DIR="$(armillary plugin path | head -1)"
mkdir -p "$PLUGIN_DIR"
cp -r ./csv-line-count "$PLUGIN_DIR/csv-line-count"
```

(See [discovery.md](./discovery.md) for the full scan order, including the
`ARMILLARY_PLUGIN_PATH` env var and the workspace-local `./plugins/`
escape hatch — the latter is the most convenient way to iterate during
development.)

## 5. Verify it loads

```bash
armillary plugin list
# → csv-line-count 0.1.0 (sinks: csv_line_count)

armillary plugin check csv-line-count
# Spawns the binary, runs the v1 handshake, exits 0 on success.
```

A non-zero exit from `plugin check` means the plugin is broken — read its
stderr (forwarded into armillary's logs) for the failure.

## 6. Use it in the canvas

1. Start armillary (`just dev-backend` for development, or your installed binary).
2. Open a pipeline. Drag any source onto the canvas, then drag the
   **CSV Line Count** sink from the **Plugins** section of the node palette
   (it has a "plugin" badge and an indigo border).
3. Click the sink to open its editor. Armillary fetches `config_schema.json` and
   renders a form with the `output` field.
4. Connect the source to the sink, fill in `output`, and run the pipeline.

After the run, your output file should have one new line:

```
$ cat /tmp/line-count.log
1712592000 12345
```

## Where to go next

- Read [`examples/plugins/parquet-plugin/src/main.rs`](../../examples/plugins/parquet-plugin/src/main.rs)
  — the canonical SDK example. Same shape as this one, but writes real
  Parquet output and demonstrates `abort` and `log::info` usage in a
  real-world setting.
- [`testing.md`](./testing.md) — how to write unit and integration tests for
  your plugin without spawning a process.
- [`distribution.md`](./distribution.md) — how to package and ship your
  plugin to other users (cross-platform binaries, install layout, what to
  put in your README).
- [`debugging.md`](./debugging.md) — what to do when something goes wrong.
- [`protocol-v1.md`](./protocol-v1.md) — the wire protocol the SDK is hiding
  from you. You only need this if you are writing a plugin in another
  language or debugging the SDK itself.
