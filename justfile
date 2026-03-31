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

# Format all code
fmt:
    cargo fmt --all
    cd frontend && npx prettier --write src/
