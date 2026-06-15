# Armillary

Visual data pipeline builder. Construct DAGs of source, transform, and sink nodes on a canvas, write SQL (DataFusion) or Python (Polars) transforms, and see live data previews. Single Rust binary with embedded browser UI.

## Quick Start

```bash
# Build
just build        # or: cargo build --workspace && cd frontend && npm run build

# Run
cargo run         # starts server at http://localhost:8080, opens browser

# Or run headless
cargo run -- start --headless --port 9090
```

## Docker

Prebuilt images are published on each release to Docker Hub and the GitHub Container Registry:

```bash
# Docker Hub
docker pull cachehorizon/armillary:1.0.0        # or :latest

# GitHub Container Registry
docker pull ghcr.io/cache-has/armillary:1.0.0   # or :latest
```

Run the server (headless, listening on port 8080) with a persistent data volume:

```bash
docker run -p 8080:8080 -v armillary-data:/data cachehorizon/armillary:latest
```

Then open http://localhost:8080. Pipelines, secrets, and run history persist in the
`armillary-data` volume, mounted at `/data` inside the container.

## Architecture

```
crates/
  armillary-engine/        Core data model, DAG, pipeline storage
  armillary-datafusion/    Pipeline execution, DataFusion, Arrow data flow, Python runtime
  armillary-connectors/    Source/sink implementations (CSV, PostgreSQL, REST, stdout)
  armillary-secrets/       Encrypted secret store (AES-256-GCM + Argon2)
  armillary-server/        Axum HTTP/WebSocket server
  armillary-tray/          System tray, desktop notifications
  armillary-cli/           CLI interface
frontend/             React + TypeScript (Vite)
test-pipelines/       Example pipelines with sample data
```

Data flows as Arrow `RecordBatch` vectors throughout the Rust side.

## Features

- **Visual DAG canvas** with force-directed layout, drag-and-drop node creation
- **SQL transforms** via DataFusion with friendly SQL syntax (GROUP BY ALL, EXCLUDE, COLUMNS)
- **Python transforms** via Polars with subprocess isolation and Arrow IPC exchange
- **Source connectors:** CSV/Parquet files, PostgreSQL (with filter pushdown), REST APIs
- **Sink connectors:** CSV/Parquet files, PostgreSQL (with auto-create, indexes, upsert), stdout
- **Live data preview** at every node with schema display and column statistics
- **Environment management** (dev/staging/prod) with fallback chains and per-node overrides
- **Encrypted secret store** for database passwords and API keys
- **Pipeline variables** with `{{ variable_name }}` syntax and `{{ env:VAR }}` for environment variables
- **External code files** via `code_dir` and `code_path` for clean project organization
- **Import/export** pipelines as self-contained JSON (code_path references resolved on export)
- **CLI** for headless operation: `armillary run`, `armillary preview`, `armillary secret`, `armillary env`
- **WebSocket** real-time execution status updates in the browser

## Development

| Command | What it does |
|---------|-------------|
| `just build` | Build backend + frontend |
| `just test` | Run all tests and lints |
| `just check` | Format check + clippy + frontend lint |
| `just fmt` | Auto-format all code |
| `just dev-backend` | Run the Rust backend (serves frontend) |
| `just dev-frontend` | Start Vite dev server (hot reload) |

Requirements:
- Rust stable (see `rust-toolchain.toml`)
- Node.js (for frontend)
- Python 3 with Polars (`uv pip install polars`) for Python transforms
- PostgreSQL (for PostgreSQL source/sink connectors)

## Configuration

### Environment Variables

Create a `.env` file in the project root. Values are available in pipeline configs via `{{ env:VAR_NAME }}`:

```bash
PAGILA_CONNECTION=postgresql://user:pass@localhost:5432/pagila
```

### Secrets

```bash
armillary secret init                          # first-time setup
armillary secret set db_password "s3cret"      # store a secret
armillary secret set db_password --env prod "prod_s3cret"  # environment-scoped
```

Reference in connector configs: `{{ secret:db_password }}`

### Pipeline Variables

Declare in the pipeline JSON and override at runtime:

```json
{
  "variables": {
    "min_amount": { "type": "float", "default": 100 }
  }
}
```

```bash
armillary run "My Pipeline" -V "min_amount=500"
```

## CLI

```bash
armillary start                    # start server (default)
armillary start --headless         # no browser
armillary stop                     # stop running server
armillary status                   # show server status

armillary list                     # list all pipelines
armillary run <pipeline>           # execute a pipeline
armillary run <pipeline> --env prod -V "key=value"
armillary preview <pipeline>       # preview with sample data
armillary show <pipeline>          # show pipeline details
armillary history <pipeline>       # show execution history

armillary export <pipeline> -o out.json    # export pipeline
armillary export --all -o ./pipelines/     # export all
armillary import input.json                # import pipeline

armillary secret init / set / list / delete
armillary env list / create / delete / show
```

## Test Pipelines

See [`test-pipelines/README.md`](test-pipelines/README.md) for example pipelines using PostgreSQL, REST APIs, CSV files, and both SQL and Python transforms.

**Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.**
Licensed under MIT OR Apache-2.0.