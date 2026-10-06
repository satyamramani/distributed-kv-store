#!/usr/bin/env bash
set -e

PORT=${1:-8080}
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$ROOT_DIR/build/bin/kvstore_server"

# Build server if missing or out of date
echo "Building native C++ kvstore_server..."
cmake --build "$ROOT_DIR/build" --target kvstore_server

echo "======================================================================"
echo " 🚀 Launching Native C++ KV-Store Server with Linked Library"
echo " Binary: $BIN"
echo " Serving Web UI & C++ REST APIs on: http://localhost:$PORT"
echo "======================================================================"

# Automatically open default browser on macOS
if command -v open > /dev/null; then
    (sleep 1 && open "http://localhost:$PORT") &
fi

exec "$BIN" "$PORT"
