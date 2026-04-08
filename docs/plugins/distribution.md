<!--
Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Distributing a Plugin

This page covers how to package a plugin so other people can install it,
the cross-platform considerations that bite if you don't think about them
up front, and the install layouts flux looks at on each operating system.

## What a "plugin" actually is on disk

A plugin is a directory. That directory must contain:

```
my-plugin/
  plugin.toml             # required: manifest
  my-plugin               # required: the executable (or my-plugin.exe on Windows)
  config_schema.json      # optional: per-sink JSON Schema for the canvas form
```

Anything else (`README.md`, `LICENSE`, `node_modules/`, sidecar `*.so`
files, etc.) is fine — flux only cares that `plugin.toml` exists at the
root and that the `executable` field resolves to a real file in the same
directory. See [`manifest.md`](./manifest.md) for the full schema and
[`discovery.md`](./discovery.md) for how flux finds these directories on
each OS.

The directory name **must equal the `name` field in `plugin.toml`**. A
mismatch is a discovery-time validation error.

## Where users install it

`horizon-flux plugin path` prints the directories flux will scan, in
priority order. The first entry is the user-global location, which is
where most installs go:

| OS      | User-global plugin directory |
|---------|------------------------------|
| macOS   | `~/Library/Application Support/com.horizon-analytic.horizon-flux/plugins` |
| Linux   | `$XDG_DATA_HOME/horizon-flux/plugins` (default `~/.local/share/horizon-flux/plugins`) |
| Windows | `%APPDATA%\horizon-analytic\horizon-flux\data\plugins` |

A user installs a plugin by copying the plugin directory into one of those
locations. The portable one-liner that works on every OS is:

```bash
PLUGIN_DIR="$(horizon-flux plugin path | head -1)"
mkdir -p "$PLUGIN_DIR"
cp -r ./my-plugin "$PLUGIN_DIR/my-plugin"
horizon-flux plugin list
```

For development you can skip this entirely by either dropping the plugin
directory into `./plugins/` next to where you run flux (the highest-priority
scan root), or by setting `HORIZON_FLUX_PLUGIN_PATH` to a directory of your
choice. See [`discovery.md`](./discovery.md) §Scan order for the full
precedence rules.

## Cross-platform binaries

Flux runs on macOS (Intel + Apple Silicon), Linux (x86_64 + aarch64), and
Windows (x86_64). Your plugin should ship binaries for the platforms you
want to support — flux will not cross-compile for users.

A practical baseline release matrix:

| Target triple                   | OS / arch          |
|---------------------------------|---------------------|
| `aarch64-apple-darwin`          | macOS Apple Silicon |
| `x86_64-apple-darwin`           | macOS Intel         |
| `x86_64-unknown-linux-gnu`      | Linux x86_64        |
| `aarch64-unknown-linux-gnu`     | Linux aarch64       |
| `x86_64-pc-windows-msvc`        | Windows x86_64      |

For a Rust plugin, the easiest way to produce all five is a GitHub Actions
matrix workflow that runs `cargo build --release --target $TARGET` on each
of `macos-14`, `macos-13`, `ubuntu-latest`, `ubuntu-24.04-arm`, and
`windows-latest`. Each job uploads its plugin directory as a release asset.

### Things that bite

- **Binary suffix on Windows.** The `executable = "my-plugin"` field in
  `plugin.toml` is matched against `<name>` on Unix and `<name>.exe` on
  Windows. Either ship two separate manifests per OS or use the same
  `my-plugin` name and let flux find `my-plugin.exe` automatically — flux
  appends the suffix when resolving on Windows.
- **Executable bit on Unix.** `cp` from a CI artifact preserves it; `unzip`
  does not. If you ship a `.zip` to Unix users, document a `chmod +x
  my-plugin/my-plugin` step in the install instructions, or use `.tar.gz`
  which preserves the bit.
- **macOS Gatekeeper.** Unsigned binaries downloaded via a browser get
  quarantined and refuse to run. Either codesign + notarize the plugin
  binary, or document `xattr -dr com.apple.quarantine path/to/my-plugin`
  for users.
- **glibc version on Linux.** A plugin built on Ubuntu 24.04 will not run
  on Ubuntu 20.04. Build on the oldest distro you want to support, or use
  `musl` (`x86_64-unknown-linux-musl`) for a fully static binary.
- **Dynamic deps.** If your plugin links against system libraries (libpq,
  unixodbc, etc.) document the install command per OS, or vendor them.
- **stdout buffering.** Not OS-specific, but worth restating: any
  language's stdout is block-buffered when piped. Flush after every frame
  or your plugin deadlocks. The Rust SDK does this for you.

## Bundling runtime dependencies

The hard case is plugins that need a runtime (Node.js, Python, JVM). Flux
does not provide one for you; the plugin process must be a real
executable.

### Pattern 1: native binary (Rust, Go, C++)

Easiest. The compiler emits one file. Ship that file plus the manifest and
schema. The Rust parquet plugin is the canonical example —
`examples/plugins/parquet-plugin/` builds a single `flux-parquet-plugin`
binary and that is the entire deliverable.

### Pattern 2: bundled runtime (Node.js, Python)

The OpenBoard plugin (`openboard/plugins/flux/`) is the worked example
here. Its `scripts/bundle.mjs` produces a self-contained `dist/plugin/`
directory that flux can launch as-is:

```
dist/plugin/
  plugin.toml                  # copied from the project root
  config_schema.json           # copied from the project root
  dist/openboard-plugin.js     # bundled by tsup, marked +x with #!/usr/bin/env node
  node_modules/                # runtime deps only, from `npm install --omit=dev`
```

The `executable` field in `plugin.toml` then points at a tiny launcher
shell script (or directly at `dist/openboard-plugin.js` if the user has
`node` on their `PATH`). The user still needs `node` installed — that
prerequisite goes in your plugin's README.

For Python, the equivalent is shipping the `.py` file, marking it `+x` with
a `#!/usr/bin/env python3` shebang, and listing `pyarrow` as a documented
prerequisite. Or use `pyinstaller` / `shiv` to produce a single-file
executable and treat your plugin as Pattern 1.

### Pattern 3: launcher script

Sometimes the simplest thing is a one-line shell script:

```bash
#!/usr/bin/env bash
exec python3 "$(dirname "$0")/my_plugin.py" "$@"
```

Drop this next to your `plugin.toml` as the `executable`, mark it `+x`,
and you have OS-portable plugin entry that delegates to your real
implementation.

## A README for your plugin

Plugin distribution is a documentation problem more than a packaging
problem. Your plugin's README should answer:

1. **What it does.** One sentence, no jargon.
2. **What it requires.** OS support, runtime prerequisites (Node 18+,
   Python 3.10+, libpq, etc.), the minimum flux version (`flux_min_version`
   in your manifest).
3. **How to install.** A literal copy-paste block using `horizon-flux
   plugin path` like the one above. Include the `chmod +x` step on Unix
   if you ship a `.zip`.
4. **How to verify.** `horizon-flux plugin check <name>` and what success
   looks like.
5. **What its config fields mean.** Even though `config_schema.json` drives
   the canvas form, a prose explanation of each field, with an example,
   saves users a guess-and-check loop.
6. **Where to file bugs.** Your repo's issues link.

## Versioning your release

Keep the `version` field in `plugin.toml` in sync with your release tag
and your binary's `--version` output if you have one. Flux surfaces this
in `horizon-flux plugin list` and the canvas Plugins panel, so users can
tell at a glance which version is installed.

The `flux_plugin_protocol` field is the wire protocol version your plugin
speaks (`1` today). Bump it only if you rebuild against a future protocol
version — see the planned `compatibility.md` for the matrix once it lands.

## Where to go next

- [`debugging.md`](./debugging.md) — what to tell users when their install
  doesn't work.
- [`testing.md`](./testing.md) — make sure your CI matrix actually exercises
  every binary you ship before tagging a release.
- [`manifest.md`](./manifest.md) — the full `plugin.toml` reference.
