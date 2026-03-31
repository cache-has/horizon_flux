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
COPY crates/flux-engine/Cargo.toml crates/flux-engine/Cargo.toml
COPY crates/flux-datafusion/Cargo.toml crates/flux-datafusion/Cargo.toml
COPY crates/flux-connectors/Cargo.toml crates/flux-connectors/Cargo.toml
COPY crates/flux-secrets/Cargo.toml crates/flux-secrets/Cargo.toml
COPY crates/flux-server/Cargo.toml crates/flux-server/Cargo.toml
COPY crates/flux-tray/Cargo.toml crates/flux-tray/Cargo.toml
COPY crates/flux-cli/Cargo.toml crates/flux-cli/Cargo.toml
COPY crates/flux-postgres/Cargo.toml crates/flux-postgres/Cargo.toml

# Create dummy source files so cargo can resolve the workspace
RUN for crate in flux-engine flux-datafusion flux-connectors flux-secrets \
        flux-server flux-tray flux-cli flux-postgres; do \
        mkdir -p "crates/$crate/src" && echo "" > "crates/$crate/src/lib.rs"; \
    done \
    && mkdir -p crates/flux-cli/src && echo "fn main() {}" > crates/flux-cli/src/main.rs

# Pre-build dependencies (cached unless Cargo.toml/Cargo.lock change)
RUN cargo build --release --bin horizon-flux --no-default-features 2>/dev/null || true

# Copy real source and frontend
COPY crates/ crates/
COPY frontend/ frontend/

# Build frontend
RUN cd frontend && npm ci && npm run build

# Build the release binary without tray support
RUN cargo build --release --bin horizon-flux --no-default-features

# ── Runtime stage ────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd --gid 1000 flux \
    && useradd --uid 1000 --gid flux --create-home flux

COPY --from=builder /build/target/release/horizon-flux /usr/local/bin/horizon-flux

# Data directory for pipelines, secrets, and cache
RUN mkdir -p /data && chown flux:flux /data
VOLUME ["/data"]

USER flux
WORKDIR /data
ENV HOME=/data

EXPOSE 8080

ENTRYPOINT ["horizon-flux"]
CMD ["start", "--host", "0.0.0.0", "--headless"]
