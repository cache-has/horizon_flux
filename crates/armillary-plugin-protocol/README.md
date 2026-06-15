<!--
Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# armillary-plugin-protocol

Wire-protocol types, constants, and framing for the
[Armillary](https://github.com/cache-has/armillary) plugin
system. Shared between the host (`armillary-plugin-host`) and the Rust SDK
(`armillary-plugin-sdk`).

Most plugin authors do **not** depend on this crate directly — use
`armillary-plugin-sdk`. Reach for `armillary-plugin-protocol` only if you are:

- Building an alternative SDK or test harness in Rust.
- Writing a plugin that needs lower-level control of the wire protocol than
  the SDK exposes.
- Implementing host-side tooling that needs to parse plugin frames.

For the normative wire-protocol reference, see
[`docs/plugins/protocol-v1.md`](../../docs/plugins/protocol-v1.md). Authors
implementing the protocol from scratch in another language should read that
document — this crate is a Rust convenience, not the spec.

## Stability

> **`armillary-plugin-protocol` is unstable, pre-1.0.** While armillary itself is
> pre-1.0, breaking changes to the wire protocol — and therefore to this
> crate's public types — are permitted at any minor version boundary and
> will always bump the protocol version constant
> (`PROTOCOL_VERSION` / `armillary_plugin_protocol` in `plugin.toml`).
>
> Each 0.x release of this crate pins to exactly one wire-protocol
> generation. After armillary reaches 1.0, protocol version `N` will remain
> supported for at least one major cycle after `N+1` ships.

For the full compatibility matrix and the rules that govern what counts as
a breaking change, see
[`docs/plugins/compatibility.md`](../../docs/plugins/compatibility.md).

## License

Dual-licensed under either of:

- Apache License, Version 2.0
- MIT License

at your option.
