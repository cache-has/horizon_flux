#!/usr/bin/env bash
# Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Armillary — Dev Preview Setup Script for macOS
#
# This script installs all dependencies, builds Armillary from source,
# sets up a local PostgreSQL environment with sample databases, and launches
# the application. It is idempotent — safe to run multiple times.
#
# Usage:
#   chmod +x setup.sh && ./setup.sh
#
# What it installs (via Homebrew):
#   - Rust (via rustup)
#   - Node.js 22
#   - Python 3.12 + uv (Python package manager)
#   - PostgreSQL 17
#   - just (command runner)
#
# Everything is installed using standard package managers. Nothing is
# installed to unusual locations. Uninstall with `brew uninstall <pkg>`.

set -euo pipefail

# ── Colors ──────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

info()  { echo -e "${BLUE}▸${NC} $*"; }
ok()    { echo -e "${GREEN}✓${NC} $*"; }
warn()  { echo -e "${YELLOW}!${NC} $*"; }
fail()  { echo -e "${RED}✗${NC} $*"; exit 1; }
header(){ echo -e "\n${BOLD}═══ $* ═══${NC}\n"; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

header "Armillary — Dev Preview Setup"
echo "Project directory: $PROJECT_DIR"
echo ""

# ── Preflight: macOS check ──────────────────────────────────────────
if [[ "$(uname)" != "Darwin" ]]; then
  fail "This script is for macOS only."
fi

ARCH="$(uname -m)"
info "Detected macOS on $ARCH"

# ── Step 1: Homebrew ────────────────────────────────────────────────
header "Step 1/7: Homebrew"

if command -v brew &>/dev/null; then
  ok "Homebrew already installed"
else
  info "Installing Homebrew..."
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

  # Add Homebrew to PATH for Apple Silicon
  if [[ "$ARCH" == "arm64" ]] && [[ -f /opt/homebrew/bin/brew ]]; then
    eval "$(/opt/homebrew/bin/brew shellenv)"
    # Persist for future shells
    if ! grep -q 'homebrew/bin/brew' ~/.zprofile 2>/dev/null; then
      echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
      warn "Added Homebrew to ~/.zprofile — restart your terminal after setup"
    fi
  fi
  ok "Homebrew installed"
fi

# ── Step 2: System packages ────────────────────────────────────────
header "Step 2/7: System packages"

BREW_PACKAGES=(node@22 just)
for pkg in "${BREW_PACKAGES[@]}"; do
  if brew list "$pkg" &>/dev/null; then
    ok "$pkg already installed"
  else
    info "Installing $pkg..."
    brew install "$pkg"
    ok "$pkg installed"
  fi
done

# Ensure node@22 is on PATH
if ! command -v node &>/dev/null; then
  brew link --overwrite node@22 2>/dev/null || true
  if [[ -d "$(brew --prefix)/opt/node@22/bin" ]]; then
    export PATH="$(brew --prefix)/opt/node@22/bin:$PATH"
  fi
fi
info "Node.js $(node --version)"

# ── Step 3: Rust ────────────────────────────────────────────────────
header "Step 3/7: Rust"

if command -v rustup &>/dev/null; then
  ok "Rust already installed ($(rustc --version))"
  info "Updating Rust toolchain..."
  rustup update stable --no-self-update 2>/dev/null || true
else
  info "Installing Rust via rustup..."
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
  source "$HOME/.cargo/env"
  ok "Rust installed ($(rustc --version))"
fi

# Ensure cargo is available
source "$HOME/.cargo/env" 2>/dev/null || true

# ── Step 4: Python + uv ────────────────────────────────────────────
header "Step 4/7: Python + uv"

if command -v uv &>/dev/null; then
  ok "uv already installed ($(uv --version))"
else
  info "Installing uv..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
  ok "uv installed ($(uv --version))"
fi

# Create managed Python venv for transforms
PYTHON_ENV="$HOME/.armillary/python"
if [[ -f "$PYTHON_ENV/.armillary-ready" ]]; then
  ok "Python environment already set up"
else
  info "Setting up Python 3.12 environment with required packages..."
  uv venv --python 3.12 "$PYTHON_ENV"
  uv pip install --python "$PYTHON_ENV/bin/python" \
    "polars>=1.39.3" numpy scipy requests httpx
  touch "$PYTHON_ENV/.armillary-ready"
  ok "Python environment ready at $PYTHON_ENV"
fi

# ── Step 5: PostgreSQL ──────────────────────────────────────────────
header "Step 5/7: PostgreSQL"

# Detect any existing Homebrew PostgreSQL installation
PG_FORMULA=""
for v in 17 16 15 14; do
  if brew list "postgresql@$v" &>/dev/null; then
    PG_FORMULA="postgresql@$v"
    break
  fi
done
# Also check the unversioned formula
if [[ -z "$PG_FORMULA" ]] && brew list postgresql &>/dev/null; then
  PG_FORMULA="postgresql"
fi

if [[ -n "$PG_FORMULA" ]]; then
  ok "PostgreSQL already installed ($PG_FORMULA)"
else
  PG_FORMULA="postgresql@17"
  info "Installing $PG_FORMULA..."
  brew install "$PG_FORMULA"
  ok "$PG_FORMULA installed"
fi

# Ensure PostgreSQL binaries are on PATH
PG_BIN="$(brew --prefix)/opt/$PG_FORMULA/bin"
if [[ -d "$PG_BIN" ]]; then
  export PATH="$PG_BIN:$PATH"
fi

# Start PostgreSQL if not running
if pg_isready -q 2>/dev/null; then
  ok "PostgreSQL is running"
else
  info "Starting PostgreSQL..."
  brew services start "$PG_FORMULA"
  # Wait for PostgreSQL to be ready
  for i in {1..30}; do
    if pg_isready -q 2>/dev/null; then break; fi
    sleep 1
  done
  if pg_isready -q 2>/dev/null; then
    ok "PostgreSQL started"
  else
    fail "PostgreSQL failed to start. Check: brew services list"
  fi
fi

# Create databases if they don't exist
for db in pagila openboard_examples armillary_output; do
  if psql -lqt 2>/dev/null | cut -d '|' -f 1 | grep -qw "$db"; then
    ok "Database '$db' exists"
  else
    info "Creating database '$db'..."
    createdb "$db"
    ok "Database '$db' created"
  fi
done

# Load pagila sample data if the 'film' table doesn't exist
if psql -d pagila -c "SELECT 1 FROM film LIMIT 1" &>/dev/null; then
  ok "Pagila sample data already loaded"
else
  info "Downloading and loading Pagila sample database..."
  PAGILA_DIR="$(mktemp -d)"
  curl -sL "https://github.com/devrimgunduz/pagila/archive/refs/heads/master.tar.gz" \
    | tar -xz -C "$PAGILA_DIR" --strip-components=1
  psql -d pagila -f "$PAGILA_DIR/pagila-schema.sql" -q
  psql -d pagila -f "$PAGILA_DIR/pagila-data.sql" -q
  rm -rf "$PAGILA_DIR"
  ok "Pagila sample data loaded (16 tables)"
fi

# Load openboard sample data if the 'orders' table doesn't exist
if psql -d openboard_examples -c "SELECT 1 FROM orders LIMIT 1" &>/dev/null; then
  ok "Openboard sample data already loaded"
else
  info "Loading Openboard sample data..."
  OPENBOARD_SQL="$PROJECT_DIR/dev-dist/openboard_seed.sql"
  if [[ -f "$OPENBOARD_SQL" ]]; then
    psql -d openboard_examples -f "$OPENBOARD_SQL" -q
    ok "Openboard sample data loaded"
  else
    warn "Openboard seed file not found at $OPENBOARD_SQL — skipping"
    warn "The 'Openboard: Order Summary' pipeline will not work without it"
  fi
fi

# ── Step 6: Build Armillary ──────────────────────────────────────
header "Step 6/7: Build"

info "Installing frontend dependencies..."
cd "$PROJECT_DIR/frontend"
npm ci --silent
ok "Frontend dependencies installed"

info "Building frontend..."
npm run build --silent
ok "Frontend built"

info "Building Armillary (this takes 5-10 minutes on first run)..."
cd "$PROJECT_DIR"
cargo build --release --bin armillary 2>&1 | tail -1
ok "Armillary built successfully"

BINARY="$PROJECT_DIR/target/release/armillary"
BINARY_SIZE=$(ls -lh "$BINARY" | awk '{print $5}')
info "Binary: $BINARY ($BINARY_SIZE)"

# ── Step 7: Environment setup ──────────────────────────────────────
header "Step 7/7: Environment"

# Create .env if it doesn't exist
if [[ ! -f "$PROJECT_DIR/.env" ]]; then
  info "Creating .env from test-pipelines/.env.example..."
  cp "$PROJECT_DIR/test-pipelines/.env.example" "$PROJECT_DIR/.env"
  ok ".env created"
else
  ok ".env already exists"
fi

# Import test pipelines
info "Importing test pipelines..."
for pipeline in "$PROJECT_DIR"/test-pipelines/*.json; do
  name=$(basename "$pipeline" .json)
  "$BINARY" import "$pipeline" 2>/dev/null && ok "Imported: $name" || warn "Already imported or error: $name"
done

# ── Done ────────────────────────────────────────────────────────────
header "Setup Complete!"

echo -e "${GREEN}Armillary is ready to run.${NC}"
echo ""
echo "To start the application:"
echo ""
echo -e "  ${BOLD}$BINARY${NC}"
echo ""
echo "Or from the project directory:"
echo ""
echo -e "  ${BOLD}cargo run --release --bin armillary${NC}"
echo ""
echo "The app will open in your browser at http://localhost:8080"
echo ""
echo "────────────────────────────────────────────────"
echo "Test pipelines loaded:"
echo "  • Pagila: Revenue by Category (SQL only)"
echo "  • Openboard: Order Summary by Region (SQL only)"
echo "  • Cross-Source Analytics (SQL + Python + REST APIs)"
echo ""
echo "Sample CSV data in: test-pipelines/data/"
echo "  • sales.csv — 25 sample orders"
echo "  • customers.csv — 10 sample customers"
echo "  • country_economics.csv — 31 country records"
echo "────────────────────────────────────────────────"
echo ""

# Ask if they want to launch now
read -p "Launch Armillary now? [Y/n] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Nn]$ ]]; then
  echo ""
  info "Starting Armillary..."
  exec "$BINARY"
fi
