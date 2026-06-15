<!--
Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# armillary-plugin-sdk

Rust SDK for writing [Armillary](https://github.com/horizon-analytic/armillary)
sink plugins. Implement one trait, call one function, done.

```rust,ignore
use armillary_plugin_sdk::{run, PluginInfo, Sink, SinkError, WriteStats};

struct MySink { /* ... */ }

impl Sink for MySink {
    type Config = MyConfig;
    fn configure(&mut self, c: MyConfig, schema: &Schema) -> Result<(), SinkError> { /* ... */ }
    fn write_batch(&mut self, b: &RecordBatch) -> Result<(), SinkError> { /* ... */ }
    fn commit(&mut self) -> Result<WriteStats, SinkError> { /* ... */ }
}

fn main() -> std::process::ExitCode {
    run(PluginInfo { name: "my-sink".into(), version: "0.1.0".into() }, MySink::new())
}
```

The SDK handles the full v1 wire protocol — handshake, configure, batch
streaming, commit/abort, shutdown — so plugin authors only ever deal with
their own typed config and `arrow::record_batch::RecordBatch`.

## Getting started

The fastest path is the [plugin template
repo](https://github.com/horizon-analytic/armillary-plugin-template):

```bash
gh repo create my-armillary-sink --template horizon-analytic/armillary-plugin-template --public --clone
cd my-armillary-sink
cargo build --release
```

For a long-form walkthrough, see
[`docs/plugins/your-first-plugin-rust.md`](../../docs/plugins/your-first-plugin-rust.md)
in the armillary repo. The
[`parquet-plugin`](../../examples/plugins/parquet-plugin/) crate is the
canonical reference plugin built on this SDK.

## Stability

> **`armillary-plugin-sdk` is unstable, pre-1.0.** While armillary itself is pre-1.0,
> breaking changes to the SDK surface and to the underlying wire protocol
> are permitted at any minor version boundary. Each SDK 0.x release pins to
> exactly one wire-protocol generation; bumping the SDK is how you opt into
> a new protocol version.
>
> After armillary reaches 1.0, the SDK will follow semver in the usual way, and
> protocol version `N` will remain supported for at least one major cycle
> after `N+1` ships.

For the full compatibility matrix and the rules that govern what "breaking"
means, see
[`docs/plugins/compatibility.md`](../../docs/plugins/compatibility.md).

## License

Dual-licensed under either of:

- Apache License, Version 2.0
- MIT License

at your option.
