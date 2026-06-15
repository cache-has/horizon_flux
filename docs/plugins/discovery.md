<!--
Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Plugin Discovery and Directory Layout

Armillary discovers plugins at startup (and on-demand via
`POST /api/plugins/reload`) by scanning a fixed, ordered list of directories.
Each immediate subdirectory of a scan root is treated as one plugin if it
contains a `plugin.toml` at its root.

## Scan order

Plugins are loaded in the following order. Later entries **shadow** earlier
ones with the same `name`, so a workspace-local plugin overrides a
user-global one of the same name.

1. **Platform user-data directory** (cross-platform, via the `directories`
   crate's `ProjectDirs::data_dir()`):
   - **Linux:** `$XDG_DATA_HOME/armillary/plugins`, defaulting to
     `~/.local/share/armillary/plugins`.
   - **macOS:** `~/Library/Application Support/com.horizon-analytic.armillary/plugins`.
   - **Windows:** `%APPDATA%\horizon-analytic\armillary\data\plugins`.
2. **Legacy fallback (all platforms):** `~/.armillary/plugins`. Loaded
   only if it exists. Present for compatibility with the directory layout
   shown in `planning/24-plugin-system.md` and early-adopter installs.
3. **`ARMILLARY_PLUGIN_PATH` environment variable**, a
   platform-separator-delimited list of additional directories (`:` on
   Unix, `;` on Windows). Useful for development and CI.
4. **Workspace-local:** `./plugins/` resolved against the current working
   directory of the running armillary process. This is the highest-priority
   source so a checked-in plugin can override a globally installed copy.

The CLI command `armillary plugin path` prints this resolved list in order so
users can debug discovery.

## Why this layout

- **Platform-native primary location.** Aligns with what users expect on
  each OS and avoids cluttering `$HOME` on Linux/macOS. The `directories`
  crate already encapsulates the platform rules; armillary uses
  `ProjectDirs::from("com", "horizon-analytic", "armillary")`.
- **Legacy fallback** keeps the originally documented `~/.armillary/plugins`
  working without requiring a migration step for early adopters.
- **Env-var override** is the standard escape hatch for CI, package
  managers, and developers who want to test a plugin without installing it.
- **Workspace-local last (highest priority)** mirrors how `node_modules`,
  `.venv`, and `target/` work: the project's own copy wins.

## Discovery rules

- A subdirectory without a `plugin.toml` is silently ignored (it might be a
  WIP folder or unrelated content).
- A `plugin.toml` that fails to parse or fails JSON Schema validation
  produces a plugin entry with `status: "invalid"` and a captured error;
  the plugin is not spawnable but is visible to the user via
  `armillary plugin list` and `GET /api/plugins`.
- The `name` field in the manifest must equal the directory name. A
  mismatch is a validation error.
- Duplicate `name` across the same scan root is an error on the second
  occurrence. Duplicate `name` across different scan roots is the
  intentional shadowing case described above and is logged at `info`.
- The connector `type` declared in `[[sinks]]` must be globally unique
  across all loaded plugins **and** across the built-in connector
  registry. A collision marks the colliding plugin `invalid`.

## Hot reload

`POST /api/plugins/reload` re-runs discovery. Plugins that disappeared are
removed from the registry; plugins that appeared are added. Plugins whose
manifest changed are re-validated. In v1, plugins that are currently
running as part of a live pipeline are **not** restarted by reload — the
new manifest takes effect on the next pipeline run.
