# C2 Metrics Dashboard

A minimal static dashboard rendering four charts from the Cube semantic layer.

## Prerequisites
- Docker running and the `cube` service up (`docker compose up -d`).
- Cube exposed on port 4000 (default in this repo).

## Run locally
From the `web` folder, serve the static page and open it:

```bash
python3 -m http.server 8080
# Open http://localhost:8080 in your browser
```

If your Cube instance requires auth, add a Bearer token in the input at the top of the page. In dev mode (`CUBEJS_DEV_MODE=true`), auth is typically not required.

## Stop locally
In the same terminal where the server is running, press `Ctrl+C` to stop it gracefully.

If you started it in the background or a different terminal, you can terminate it by port (macOS):

```bash
# Stop any process listening on port 8080
lsof -ti:8080 | xargs kill
# If you used a different port, replace 8080 accordingly.
```

Alternatively, if you know the command/port combo you used:

```bash
# Stop python http.server on port 8080
pkill -f "http.server 8080"
```

## Charts
- Avg Strokes per Minute — Daily (line)
- Avg Watts — Daily (line)
- Avg Pace — Daily (line)
- Avg Session Duration — Daily (line)

Charts filter for the selected `machine` and only include sessions with `total_session_minutes >= 10`. Adjust these in `index.html` if needed.

## Filters
- Machine selector allows toggling between `RowErg`, `SkiErg`, and `BikeErg`.
- Changing the selector reloads the charts with the chosen machine.
 - Start Date filter (toggle + date input) applies `workout_date afterOrOnDate` when enabled; defaults to `2025-07-01`.
 - Sessions filter requires `total_session_minutes >= 10`.
