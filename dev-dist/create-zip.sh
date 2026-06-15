#!/usr/bin/env bash
# Creates the dev distribution zip file.
# Run from the project root: bash dev-dist/create-zip.sh

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

VERSION=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/' || echo "0.1.0")
DIST_NAME="armillary-dev-v${VERSION}"
DIST_DIR="/tmp/$DIST_NAME"

echo "Creating dev distribution: $DIST_NAME"

# Clean previous build
rm -rf "$DIST_DIR" "/tmp/${DIST_NAME}.zip"

# Create directory structure
mkdir -p "$DIST_DIR"

# Copy source code (excluding build artifacts, venvs, etc.)
rsync -a \
  --exclude='target/' \
  --exclude='node_modules/' \
  --exclude='.venv/' \
  --exclude='.git/' \
  --exclude='output/' \
  --exclude='dev-dist/create-zip.sh' \
  --exclude='.DS_Store' \
  --exclude='*.pyc' \
  --exclude='__pycache__/' \
  "$PROJECT_DIR/" "$DIST_DIR/"

# Make setup script executable
chmod +x "$DIST_DIR/dev-dist/setup.sh"

# Create the zip
cd /tmp
zip -r "${DIST_NAME}.zip" "$DIST_NAME" -x '*.DS_Store'

SIZE=$(ls -lh "/tmp/${DIST_NAME}.zip" | awk '{print $5}')
echo ""
echo "Created: /tmp/${DIST_NAME}.zip ($SIZE)"
echo ""
echo "Send this to your friends with these instructions:"
echo ""
echo "  1. Unzip the file"
echo "  2. Open Terminal and run:"
echo ""
echo "     cd ${DIST_NAME}"
echo "     chmod +x dev-dist/setup.sh && ./dev-dist/setup.sh"
echo ""

# Clean up the unzipped copy
rm -rf "$DIST_DIR"
