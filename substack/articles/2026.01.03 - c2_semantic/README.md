# C2 Metrics Dashboard

A minimal dashboard for Concept2 workout metrics powered by a Cube semantic layer. It shows daily charts for RowErg and SkiErg, lets you click a data point to load per-session stroke detail, and provides summary KPIs.

## Prerequisites
- Docker installed and running.
- `uv` installed for Python runs.

## Start the Semantic Layer (Cube)
From the project root, bring up Cube with Docker Compose:

```bash
cd "/Users/matthewmartin/dream_machine/substack/articles/2026.01.03 - c2_semantic"
docker compose up -d
```

This starts Cube on `http://localhost:4000` using the cube model in `model/`.

## Start the Web Dashboard
Use the combined dev runner via `uv` to start both the static site and the local API used for session detail:

```bash
uv run python dev.py
# Dashboard: http://localhost:8080
# Session API: http://127.0.0.1:8000
```

Alternatively, you can serve the static page only:

```bash
cd web
python3 -m http.server 8080
# Open http://localhost:8080
```

## Stop Services
- Stop the web/API dev runner: press `Ctrl+C` in the terminal where `uv run python dev.py` is running.
- If a static server was started:

```bash
# macOS: kill anything on port 8080
lsof -ti:8080 | xargs kill
```

- Stop Cube/Docker services:

```bash
docker compose down
```

## Notes
- The dashboard defaults to `http://localhost:4000` for Cube; you can change the URL and provide an auth token at the top of the page.
- Interactive charts: clicking any daily chart point (Watts, Pace, Duration) loads the selected session’s stroke detail on the left and shows session KPIs.
- SPM histogram: clicking a point on the SPM daily chart opens a modal with a histogram of the session’s stroke rate distribution (requires the local API).
- A distance filter at the top lets you switch between “Show All” and “Show >5k” for daily charts.
