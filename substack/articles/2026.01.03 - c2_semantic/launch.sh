#!/usr/bin/env bash
set -euo pipefail

# Start Cube in the background
docker compose up -d

# Run the dev stack (static web + API) in the background
uv run dev.py &
DEV_PID=$!

# Give the servers a moment to start up
sleep 3

# Open the web UI (static site is on 8080; API is on 8000)
open "http://localhost:8080"

# Keep this script attached to the dev process so logs stream here
wait "$DEV_PID"