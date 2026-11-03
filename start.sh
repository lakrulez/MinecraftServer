#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=================================================================="
echo "  BedrockNexus — Minecraft Bedrock Multi-Server Manager & Proxy   "
echo "=================================================================="

PYTHON_BIN="python3"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    PYTHON_BIN="python"
fi
if [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
fi

echo "[1/2] Checking Python dependencies..."
"$PYTHON_BIN" -m pip install -q -r requirements.txt

PORT="${PORT:-8000}"
echo "[2/2] Starting FastAPI Web GUI on http://0.0.0.0:${PORT} and RakNet UDP Proxy on :19132..."
echo "      Open http://localhost:${PORT} in your browser."

exec "$PYTHON_BIN" -m uvicorn app.main:app --host 0.0.0.0 --port "$PORT"

