#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

nohup uv run python "$ROOT/dev.py" >> "$ROOT/dev.log" 2>&1 &
DEV_PID=$!
disown "$DEV_PID"
echo "$DEV_PID" > "$ROOT/dev.pid"
echo "Dashboard started (PID $DEV_PID). Logs → dev.log"

sleep 2
open "http://localhost:8080"
