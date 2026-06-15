# Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
# SPDX-License-Identifier: MIT OR Apache-2.0

# ── Build stage: Rust binary with embedded frontend ──────────────────
FROM rust:1.85-bookworm AS builder

# Install Node.js 22 for frontend build
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs

WORKDIR /build

# Cache dependency builds: copy manifests first
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/armillary-engine/Cargo.toml crates/armillary-engine/Cargo.toml
COPY crates/armillary-datafusion/Cargo.toml crates/armillary-datafusion/Cargo.toml
COPY crates/armillary-connectors/Cargo.toml crates/armillary-connectors/Cargo.toml
COPY crates/armillary-secrets/Cargo.toml crates/armillary-secrets/Cargo.toml
COPY crates/armillary-server/Cargo.toml crates/armillary-server/Cargo.toml
COPY crates/armillary-tray/Cargo.toml crates/armillary-tray/Cargo.toml
COPY crates/armillary-cli/Cargo.toml crates/armillary-cli/Cargo.toml
COPY crates/armillary-postgres/Cargo.toml crates/armillary-postgres/Cargo.toml
COPY crates/armillary-observability/Cargo.toml crates/armillary-observability/Cargo.toml
COPY crates/armillary-plugin-host/Cargo.toml crates/armillary-plugin-host/Cargo.toml
COPY crates/armillary-plugin-protocol/Cargo.toml crates/armillary-plugin-protocol/Cargo.toml
COPY crates/armillary-plugin-sdk/Cargo.toml crates/armillary-plugin-sdk/Cargo.toml
COPY crates/armillary-scheduler/Cargo.toml crates/armillary-scheduler/Cargo.toml
COPY examples/plugins/parquet-plugin/Cargo.toml examples/plugins/parquet-plugin/Cargo.toml

# Create dummy source files so cargo can resolve the workspace
RUN for crate in armillary-engine armillary-datafusion armillary-connectors armillary-secrets \
        armillary-server armillary-tray armillary-cli armillary-postgres \
        armillary-observability armillary-plugin-host armillary-plugin-protocol \
        armillary-plugin-sdk armillary-scheduler; do \
        mkdir -p "crates/$crate/src" && echo "" > "crates/$crate/src/lib.rs"; \
    done \
    && mkdir -p crates/armillary-cli/src && echo "fn main() {}" > crates/armillary-cli/src/main.rs \
    && mkdir -p examples/plugins/parquet-plugin/src && echo "fn main() {}" > examples/plugins/parquet-plugin/src/main.rs

# Pre-build dependencies (cached unless Cargo.toml/Cargo.lock change)
RUN cargo build --release --bin armillary --no-default-features 2>/dev/null || true

# Copy real source and frontend
COPY crates/ crates/
COPY examples/ examples/
# Invalidate cargo's fingerprint cache so it recompiles with real sources
RUN find crates examples -name '*.rs' -exec touch {} +

# Build frontend (npm workspace: lock file is at root)
COPY package.json package-lock.json ./
COPY frontend/ frontend/
RUN npm ci --workspace=frontend && npm run build --workspace=frontend

# Build the release binary without tray support.
# Override codegen-units to reduce peak memory usage in constrained Docker builds.
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 CARGO_PROFILE_RELEASE_LTO=thin
RUN cargo build --release --bin armillary --no-default-features

# ── Runtime stage ────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd --gid 1000 armillary \
    && useradd --uid 1000 --gid armillary --create-home armillary

COPY --from=builder /build/target/release/armillary /usr/local/bin/armillary

# Data directory for pipelines, secrets, and cache
RUN mkdir -p /data && chown armillary:armillary /data
VOLUME ["/data"]

USER armillary
WORKDIR /data
ENV HOME=/data

EXPOSE 8080

ENTRYPOINT ["armillary"]
CMD ["start", "--host", "0.0.0.0", "--headless"]
