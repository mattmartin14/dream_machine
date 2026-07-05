#!/usr/bin/env bash
set -euo pipefail

# Start Cube in the background
docker compose up -d

# Run the dev stack (static web + API) as a daemon; logs go to dev.log
nohup uv run dev.py >> dev.log 2>&1 &
DEV_PID=$!
disown "$DEV_PID"

echo "Dev server started (PID $DEV_PID). Logs → dev.log"

# Give the servers a moment to start up
sleep 3

# Open the web UI (static site is on 8080; API is on 8000)
open "http://localhost:8080"