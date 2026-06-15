# Using Plugins

Armillary supports **sink plugins** — separate executables that extend the set
of destinations a pipeline can write to without bloating the core binary. This
guide covers installing, configuring, and troubleshooting plugins as an end user.
For protocol and authoring details, see [`protocol-v1.md`](./protocol-v1.md) and
[`manifest.md`](./manifest.md).

## Installing a plugin

A plugin is a directory containing a `plugin.toml` manifest, an executable, and
optionally a JSON Schema describing each sink's configuration:

```
my-plugin/
  plugin.toml
  my-plugin            # or my-plugin.exe on Windows
  config_schema.json   # optional, referenced from plugin.toml
```

To install, copy the directory into one of the locations Armillary scans on startup:

| Scope            | Location                                                |
|------------------|---------------------------------------------------------|
| User-global      | Platform data dir (e.g. `~/Library/Application Support/armillary/plugins` on macOS, `~/.local/share/armillary/plugins` on Linux, `%APPDATA%\armillary\plugins` on Windows) |
| Legacy fallback  | `~/.armillary/plugins/`                              |
| Workspace-local  | `./plugins/` relative to where you launch armillary         |
| Override         | Any directory listed in `ARMILLARY_PLUGIN_PATH`     |

Workspace-local plugins shadow user-global ones with the same name. The
`ARMILLARY_PLUGIN_PATH` environment variable accepts a platform-native
`PATH`-style list (`:`-separated on Unix, `;`-separated on Windows) and is
prepended to the scan list.

For the exact precedence rules, see [`discovery.md`](./discovery.md).

## Verifying installation from the CLI

Armillary ships a `plugin` command group for inspecting installed plugins:

```bash
armillary plugin path                # Print the directories armillary will scan
armillary plugin list                # List discovered plugins and their sinks
armillary plugin info <name>         # Show a plugin's manifest details
armillary plugin check <name>        # Spawn the plugin and run the v1 handshake
```

`armillary plugin check` is the fastest way to confirm a freshly installed plugin
works: it spawns the executable, performs the protocol handshake, then exits.
A non-zero status code means the plugin is broken — read the error message and
the plugin's stderr (forwarded into armillary's logs).

## Using a plugin sink in the canvas

1. Open a pipeline in the canvas.
2. From the **Node Palette**, drag a sink onto the canvas. Plugin-provided sinks
   appear alongside built-in sinks with a **plugin** badge and an indigo border
   so the provenance is clear.
3. Click the new sink node to open its editor. Armillary fetches the plugin's
   `config_schema.json` from the host
   (`GET /api/plugins/:name/sinks/:type/schema`) and renders a form for it. If
   the schema is missing or invalid, the editor falls back to a raw JSON editor.
4. Connect an upstream node and run the pipeline. The plugin is spawned once
   per pipeline run and torn down after the sink commits.

The **Plugins** panel (toolbar button) lists every discovered plugin, its sinks,
and any manifest validation errors. Use the **Rescan** action after copying a
new plugin into place — it triggers `POST /api/plugins/reload` so you don't
need to restart armillary during development.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Plugin doesn't appear in the palette | Manifest invalid, or plugin in wrong directory | `armillary plugin list` / `armillary plugin path`; check the Plugins panel for parse errors |
| "Protocol version mismatch" on startup | Plugin built against a different protocol version than this armillary | Update armillary or downgrade the plugin so `armillary_plugin_protocol` matches |
| Pipeline fails with "Configure rejected" | Plugin refused the upstream Arrow schema or config | Check the plugin's stderr in armillary logs for the rejection reason |
| Pipeline fails with "transport closed" | Plugin crashed mid-stream | Inspect plugin stderr; reproduce manually with `armillary plugin check` |
| Schema form is empty / shows raw JSON | `config_schema.json` missing or unreadable from the plugin directory | Add or fix the schema file referenced by the manifest |
| "node: command not found" or plugin spawn fails immediately | Plugin requires a Node.js runtime that isn't on `PATH` (e.g. the OpenBoard plugin) | Install Node.js 18+ and re-run; verify with `node --version` from the same shell that launches armillary |
| "IO Error: Could not set lock on file" / "database is locked" | Target DuckDB file is held open by another process (commonly an `openboard dev` server) | Stop the dev server (or any other reader), re-run the pipeline, then restart the dev server |
| "Permission denied" when the plugin writes its target file | The plugin process can't write to the configured output path or its parent directory | Ensure the directory exists and is writable by the user running armillary; on macOS, grant Full Disk Access if writing under a protected location |
| Stale `*.staging-*.duckdb` files left behind | Plugin was killed mid-run before commit | Safe to delete manually; the OpenBoard plugin also sweeps orphans on its next run |

Plugin `Log` messages and stderr are both forwarded into armillary's `tracing`
infrastructure, so `RUST_LOG=armillary=debug` will show plugin diagnostics
inline with host logs.

## Reference plugins

Two reference plugins are maintained alongside armillary:

- **`examples/plugins/parquet-plugin/`** (in this repo) — a small standalone
  Rust binary that writes incoming batches to a Parquet file. Use this as the
  minimal "hello world" of the plugin protocol: a single source file, no
  external dependencies beyond Arrow and the protocol crate.
- **OpenBoard plugin** — a Node.js sink that materializes pipeline output into
  a DuckDB file and registers it with an [OpenBoard](https://github.com/horizon-analytic/openboard)
  project so dashboards can query it directly. It lives in the openboard repo
  at `plugins/armillary/` and ships as a separate release. This is the **canonical
  example** for plugin authors: it exercises every part of the v1 protocol
  (handshake, configure, streamed Arrow batches, commit, abort, error frames),
  implements transactional staging-and-rename semantics, validates incoming
  schemas against an existing target, and includes both unit and end-to-end
  subprocess tests. Third-party plugin authors are encouraged to read it
  before starting a new plugin — the file layout, manifest, config schema,
  and test harness are all directly reusable as a template, regardless of
  whether the new plugin is written in Node, Rust, Python, or anything else
  that can speak the wire protocol over stdio.

For the joint Postgres → Armillary → OpenBoard tutorial, see
`openboard/docs/tutorials/armillary-postgres-to-dashboard.md` in the openboard
repo.

### Starting your own plugin

The fastest way to get a working sink plugin in armillary is the
[`armillary-plugin-template`](https://github.com/cache-has/armillary-plugin-template)
repo — a pre-wired Rust scaffold (using `armillary-plugin-sdk`) that ships a
JSON Lines sink, cross-platform CI, and the manifest and config-schema
boilerplate. Clone it as a GitHub template, change a few lines, and you have
a working plugin installed in armillary:

```bash
gh repo create my-armillary-sink --template cache-has/armillary-plugin-template --public --clone
cd my-armillary-sink
cargo build --release
```

For the long-form walkthrough see
[`your-first-plugin-rust.md`](./your-first-plugin-rust.md). Non-Rust authors
should start with [`your-first-plugin-direct.md`](./your-first-plugin-direct.md)
and the OpenBoard plugin as a worked example.
