# Using Plugins

Horizon Flux supports **sink plugins** — separate executables that extend the set
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

To install, copy the directory into one of the locations Flux scans on startup:

| Scope            | Location                                                |
|------------------|---------------------------------------------------------|
| User-global      | Platform data dir (e.g. `~/Library/Application Support/horizon-flux/plugins` on macOS, `~/.local/share/horizon-flux/plugins` on Linux, `%APPDATA%\horizon-flux\plugins` on Windows) |
| Legacy fallback  | `~/.horizon-flux/plugins/`                              |
| Workspace-local  | `./plugins/` relative to where you launch flux         |
| Override         | Any directory listed in `HORIZON_FLUX_PLUGIN_PATH`     |

Workspace-local plugins shadow user-global ones with the same name. The
`HORIZON_FLUX_PLUGIN_PATH` environment variable accepts a platform-native
`PATH`-style list (`:`-separated on Unix, `;`-separated on Windows) and is
prepended to the scan list.

For the exact precedence rules, see [`discovery.md`](./discovery.md).

## Verifying installation from the CLI

Flux ships a `plugin` command group for inspecting installed plugins:

```bash
flux plugin path                # Print the directories flux will scan
flux plugin list                # List discovered plugins and their sinks
flux plugin info <name>         # Show a plugin's manifest details
flux plugin check <name>        # Spawn the plugin and run the v1 handshake
```

`flux plugin check` is the fastest way to confirm a freshly installed plugin
works: it spawns the executable, performs the protocol handshake, then exits.
A non-zero status code means the plugin is broken — read the error message and
the plugin's stderr (forwarded into flux's logs).

## Using a plugin sink in the canvas

1. Open a pipeline in the canvas.
2. From the **Node Palette**, drag a sink onto the canvas. Plugin-provided sinks
   appear alongside built-in sinks with a **plugin** badge and an indigo border
   so the provenance is clear.
3. Click the new sink node to open its editor. Flux fetches the plugin's
   `config_schema.json` from the host
   (`GET /api/plugins/:name/sinks/:type/schema`) and renders a form for it. If
   the schema is missing or invalid, the editor falls back to a raw JSON editor.
4. Connect an upstream node and run the pipeline. The plugin is spawned once
   per pipeline run and torn down after the sink commits.

The **Plugins** panel (toolbar button) lists every discovered plugin, its sinks,
and any manifest validation errors. Use the **Rescan** action after copying a
new plugin into place — it triggers `POST /api/plugins/reload` so you don't
need to restart flux during development.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Plugin doesn't appear in the palette | Manifest invalid, or plugin in wrong directory | `flux plugin list` / `flux plugin path`; check the Plugins panel for parse errors |
| "Protocol version mismatch" on startup | Plugin built against a different protocol version than this flux | Update flux or downgrade the plugin so `flux_plugin_protocol` matches |
| Pipeline fails with "Configure rejected" | Plugin refused the upstream Arrow schema or config | Check the plugin's stderr in flux logs for the rejection reason |
| Pipeline fails with "transport closed" | Plugin crashed mid-stream | Inspect plugin stderr; reproduce manually with `flux plugin check` |
| Schema form is empty / shows raw JSON | `config_schema.json` missing or unreadable from the plugin directory | Add or fix the schema file referenced by the manifest |

Plugin `Log` messages and stderr are both forwarded into flux's `tracing`
infrastructure, so `RUST_LOG=horizon_flux=debug` will show plugin diagnostics
inline with host logs.

## Reference plugin

A working reference plugin lives at `examples/plugins/parquet-plugin/` in the
flux repository. It's a small standalone Rust binary that writes incoming
batches to a Parquet file and is the canonical example to copy when authoring
a new plugin.
