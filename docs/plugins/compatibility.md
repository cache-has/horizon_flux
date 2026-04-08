<!--
Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Plugin Compatibility Matrix

This document tracks the relationship between the **plugin wire protocol**,
the **`flux-plugin-sdk`** Rust crate, the **`flux-plugin-protocol`** crate,
and the **`horizon-flux`** host binary. Plugin authors should consult this
table when picking dependency versions, and again whenever they upgrade.

> **Pre-1.0 stability promise:** while flux is pre-1.0, breaking protocol
> changes are permitted at any minor version boundary and will always bump
> the protocol version (`flux_plugin_protocol = N` in your `plugin.toml`).
> After flux 1.0, protocol version `N` will be supported for at least one
> major cycle after `N+1` ships. See `protocol-v1.md` §Stability for the
> normative wording.

## What governs compatibility

A plugin is compatible with a given flux host iff **all three** of these are
true:

1. The plugin's `plugin.toml` declares a `flux_plugin_protocol` integer that
   the host supports. The host announces its supported protocol versions in
   the `Hello` frame; mismatches abort the handshake immediately.
2. The plugin's `flux_min_version` is `<=` the running flux version (semver
   compare; pre-release identifiers ignored).
3. If the plugin uses `flux-plugin-sdk`, the SDK version it links against
   must speak a protocol version the host supports. In practice this means
   pinning the SDK to the same protocol generation as the host you target.

The wire protocol — not the SDK — is the authoritative contract. A plugin
written in Python, TypeScript, or Go that speaks the protocol directly does
not care about SDK versions at all.

## Version matrix

| flux host    | Protocol | `flux-plugin-protocol` | `flux-plugin-sdk` | Notes                                                |
|--------------|---------:|------------------------|-------------------|------------------------------------------------------|
| 0.1.x (dev)  | 1        | 0.1.x                  | 0.1.x             | Pre-1.0. Breaking changes possible at any minor bump |

When a new row is added to this table, the previous row's protocol version
remains supported per the stability promise above.

## How to pin in your plugin

### Rust (using the SDK)

```toml
# Cargo.toml
[dependencies]
flux-plugin-sdk = "0.1"   # once published; pre-publish, use a git or path dep
```

Pinning to `"0.1"` accepts any 0.1.x release of the SDK. Because SDK 0.1.x
is locked to protocol v1, this is also a pin on protocol v1. When protocol
v2 lands, a new SDK major (`0.2.x` or `1.0.x`) will be released; you opt
into the upgrade by bumping that line.

```toml
# plugin.toml
flux_plugin_protocol = 1
flux_min_version = "0.1.0"
```

### Any other language (speaking the protocol directly)

You don't depend on the SDK — you depend on the protocol document. Pin the
spec version you implemented:

```toml
# plugin.toml
flux_plugin_protocol = 1
flux_min_version = "0.1.0"
```

The OpenBoard plugin (`openboard/plugins/flux/src/protocol.ts`) is a worked
example of a from-scratch v1 implementation.

## What counts as a breaking protocol change

Any of the following bumps the protocol version:

- Removing or renaming a control message kind
- Removing or renaming a field in an existing control message
- Changing the framing format (length prefix, message kind tag, payload
  encoding)
- Changing the order or required-vs-optional status of frames in the
  handshake / configure / stream / shutdown lifecycle
- Tightening a previously documented invariant in a way that would reject
  previously valid plugin behavior

The following are **not** breaking and ship in the same protocol version:

- Adding a new optional field to an existing control message (unknown fields
  must be ignored per `protocol-v1.md` §Forward Compatibility)
- Adding a new capability flag in `plugin.toml`'s `[sinks.capabilities]`
- Adding new message kinds the plugin can ignore

## Reporting incompatibilities

If you hit a handshake failure or a protocol mismatch you believe is a bug
in flux (rather than your plugin), open an issue at
<https://github.com/horizon-analytic/horizon_flux/issues> with:

- The output of `horizon-flux --version`
- Your `plugin.toml`
- The plugin's stderr from `RUST_LOG=flux_plugin_host=trace horizon-flux ...`

See `debugging.md` for the full triage flow.
