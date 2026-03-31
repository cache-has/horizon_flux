# Development commands for Horizon Flux

# Start backend in development mode
dev-backend:
    cargo run --bin horizon-flux

# Start frontend dev server with hot reload
dev-frontend:
    cd frontend && npm run dev

# Build everything
build:
    cargo build
    cd frontend && npm run build

# Run all tests
test:
    cargo test --workspace
    cd frontend && npm run lint

# Check formatting and lints
check:
    cargo fmt --all --check
    cargo clippy --workspace -- -D warnings
    cd frontend && npm run lint

# Run code coverage report (requires cargo-llvm-cov)
coverage *ARGS:
    cargo llvm-cov --workspace {{ARGS}}

# Run coverage for a specific package
coverage-package PKG:
    cargo llvm-cov --package {{PKG}}

# Build optimized release binary with embedded frontend
release:
    cd frontend && npm run build
    cargo build --release --bin horizon-flux

# Report release binary size
release-size: release
    ls -lh target/release/horizon-flux

# Package a release archive for the current platform
release-dist: release
    #!/usr/bin/env bash
    set -euo pipefail
    VERSION=$(cargo metadata --format-version=1 --no-deps | python3 -c "import sys,json; print(json.load(sys.stdin)['packages'][0]['version'])")
    TARGET=$(rustc -vV | awk '/^host:/ { print $2 }')
    BINARY="target/release/horizon-flux"
    ARCHIVE="horizon-flux-v${VERSION}-${TARGET}.tar.gz"
    tar -czf "$ARCHIVE" -C target/release horizon-flux
    shasum -a 256 "$ARCHIVE"
    echo "Created $ARCHIVE"
    ls -lh "$ARCHIVE"

# Format all code
fmt:
    cargo fmt --all
    cd frontend && npx prettier --write src/
