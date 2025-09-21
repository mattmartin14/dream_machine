# DuckDB + BigLake Iceberg REST Catalog (Experiment)

## Goal
Attach DuckDB (Iceberg extension) to Google BigLake managed Iceberg REST catalog and perform basic namespace/table operations.

## Current Status
Work-in-progress; Google's preview API and DuckDB's REST attach semantics may not yet align. Scripts provided to iterate quickly and diagnose endpoint issues.

## Layout
- `gcs_auth.py` : OAuth2 token helper via ADC (google-auth)
- `scripts/attach_biglake.py` : Attempts DuckDB ATTACH with several endpoint candidates
- `scripts/probe_endpoints.py` : Raw HTTP probes of potential config endpoints
- `tests/` : Minimal pytest ensuring SQL shape
- `.env.example` : Copy to `.env` and fill values

## Environment Variables
Copy `.env.example` to `.env` and edit:
```
PROJECT_ID=your-gcp-project
BIGLAKE_CATALOG=biglakecat
BIGLAKE_LOCATION=US
BIGLAKE_DATABASE=db1
GCS_BUCKET=your-bucket
ICEBERG_WAREHOUSE_PATH=gs://your-bucket/ice_ice-baby
```
Optional: `OAUTH_TOKEN` to bypass auto token fetch.

## Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run Attach Attempt
```bash
cp .env.example .env  # then edit values
python scripts/attach_biglake.py
```

## Probe Raw Endpoints
```bash
python scripts/probe_endpoints.py | jq '.'
```

## Interpreting Failures
A 404 on `/v1/config` likely means endpoint path mismatch. Try reviewing latest GCP docs or release notes; adjust candidate endpoints in `attach_biglake.py`.

If you see 403, check IAM roles: need `roles/biglake.admin` and `roles/storage.admin` attached to the credentials used.

## Next Steps / Ideas
- Add ability to set custom headers (e.g., `x-goog-user-project`) once DuckDB exposes header injection or by contributing upstream.
- Implement creation of namespaces and tables once attach succeeds.
- Add integration test using a disposable bucket (skipped by default).

## Disclaimer
Preview APIs change; this repo is exploratory. Use non-production data.
