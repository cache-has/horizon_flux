<!--
Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Plugin Manifest (`plugin.toml`)

Every Armillary plugin directory contains a `plugin.toml` file at its
root. The manifest declares the plugin's identity, the protocol version it
targets, how to spawn it, and what connectors it provides. Armillary refuses to
load a plugin whose manifest is missing, malformed, or fails JSON Schema
validation against `plugin.schema.json` (next to this document).

## Example

```toml
# Identity
name = "openboard"
version = "0.1.0"
author = "Horizon Analytic Studios, LLC"
description = "Publish pipeline outputs to OpenBoard dashboards"
license = "MIT OR Apache-2.0"
homepage = "https://github.com/horizon-analytic/openboard"

# Protocol compatibility
armillary_plugin_protocol = 1
armillary_min_version = "0.5.0"

# Execution
executable = "openboard-plugin"
# args = ["--mode", "production"]
# [env]
# NODE_ENV = "production"

[[sinks]]
type = "openboard_duckdb"
display_name = "OpenBoard (DuckDB)"
description = "Writes pipeline output to a DuckDB file registered with an OpenBoard project"
config_schema = "config_schema.json"

[sinks.capabilities]
transactional = true
upsert = false
schema_validation = true
```

## Field reference

### Top level

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | yes | Lowercase, `[a-z0-9_-]+`. Used as the registry key. Must match the plugin's directory name. |
| `version` | string | yes | SemVer 2.0. |
| `author` | string | no | |
| `description` | string | no | |
| `license` | string | no | SPDX expression. |
| `homepage` | string | no | URL. |
| `armillary_plugin_protocol` | integer | yes | Protocol major version. v1 plugins must set `1`. |
| `armillary_min_version` | string | yes | Minimum armillary version, SemVer. Armillary refuses to load a plugin if its own version is lower. |
| `executable` | string | yes | Path relative to the plugin directory. On Windows, `.exe` is appended automatically if missing. |
| `args` | string[] | no | Static args prepended to every spawn. |
| `env` | table\<string,string\> | no | Environment variables set on every spawn. Inherited environment is preserved. |
| `sinks` | array of tables | yes (≥1) | At least one sink declaration is required in v1. |

### `[[sinks]]` entries

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `type` | string | yes | Unique connector type identifier, `[a-z0-9_]+`. Globally unique across all installed plugins. |
| `display_name` | string | yes | Shown in the canvas sink picker. |
| `description` | string | no | |
| `config_schema` | string | yes | Path (relative to plugin directory) to a JSON Schema describing this sink's user-supplied config. |
| `capabilities` | table | no | All keys default to `false` if absent. |

### `[sinks.capabilities]`

| Key | Meaning |
|-----|---------|
| `transactional` | Plugin supports atomic commit/rollback semantics. |
| `upsert` | Plugin supports upsert on a key column. |
| `schema_validation` | Plugin validates the incoming Arrow schema before accepting batches. |

## Validation

Armillary validates `plugin.toml` against `plugin.schema.json` at discovery time.
Validation failures are surfaced via:

- `armillary plugin list` — the plugin appears with an `invalid` status and the
  validation error message.
- `armillary plugin check <name>` — exits non-zero with the validation error.
- The `/api/plugins` HTTP endpoint — `status: "invalid"` plus the error.

Unknown fields are **rejected** at the top level and inside `[[sinks]]` so
typos surface immediately. Unknown keys inside `capabilities` are accepted
and ignored to allow forward-compatible capability additions.
