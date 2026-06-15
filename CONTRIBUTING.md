# Contributing to Armillary

## Prerequisites

- **Rust** (stable toolchain) — install via [rustup](https://rustup.rs/)
- **Node.js** 22+ and npm — install via [nvm](https://github.com/nvm-sh/nvm) or [nodejs.org](https://nodejs.org/)
- **just** (command runner) — `cargo install just` or `brew install just`

## Getting Started

```bash
# Clone the repository
git clone https://github.com/cache-has/armillary.git
cd armillary

# Install frontend dependencies
cd frontend && npm install && cd ..

# Verify everything builds
just build
```

## Development

### Running Locally

```bash
# Terminal 1: Start the backend
just dev-backend

# Terminal 2: Start the frontend dev server (with hot reload + proxy to backend)
just dev-frontend
```

### Common Commands

| Command | Description |
|---------|-------------|
| `just build` | Build backend and frontend |
| `just test` | Run all tests and lints |
| `just check` | Check formatting and clippy warnings |
| `just fmt` | Auto-format all code |
| `just dev-backend` | Run the Rust backend |
| `just dev-frontend` | Start the Vite dev server |

### Code Quality

Before submitting a PR, run:

```bash
just check
just test
```

CI runs the same checks on Ubuntu, macOS, and Windows.

## Project Structure

```
armillary/
  crates/
    armillary-engine/          # Core pipeline engine, DAG execution
    armillary-datafusion/      # DataFusion integration, environment resolver
    armillary-connectors/      # Source and sink implementations
    armillary-secrets/         # Encrypted secret store
    armillary-server/          # Axum web server, API routes, WebSocket
    armillary-tray/            # System tray icon, desktop notifications
    armillary-cli/             # CLI interface
  frontend/               # React + TypeScript application
  tests/                  # Integration tests
  docs/                   # Documentation
```

## License

Dual-licensed under MIT and Apache 2.0. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
