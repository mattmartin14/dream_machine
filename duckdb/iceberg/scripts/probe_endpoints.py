#!/usr/bin/env python3
"""Probe potential BigLake Iceberg REST endpoints.

The BigLake Iceberg REST catalog is in preview; endpoint structure might evolve.
This utility performs raw HTTP HEAD/GET requests with your OAuth token to
identify which endpoint form responds without 404.

NOTE: This doesn't use DuckDB; it just helps narrow correct endpoint.

It tests:
  1. https://biglake.googleapis.com/iceberg/v1beta/restcatalog/v1/config?warehouse=<gs://...>
  2. https://biglake.googleapis.com/iceberg/v1beta/projects/{project}/locations/{location}/catalogs/{catalog}/v1/config?warehouse=<gs://...>
  3. Adds header x-goog-user-project: <project>

Outputs a JSON summary.
"""
from __future__ import annotations
import os
import sys
import urllib.parse
import json
import time
import requests
from dotenv import load_dotenv

CANDIDATE_BASES = [
    "https://biglake.googleapis.com/iceberg/v1beta/restcatalog",
    "https://biglake.googleapis.com/iceberg/v1beta/projects/{project}/locations/{location}/catalogs/{catalog}",
]

CONFIG_SUFFIX = "/v1/config"


def get_token():
    # Reuse existing helper if available; fallback to direct google-auth
    try:
        import duckdb  # noqa
        import gcs_auth  # noqa
        helper = gcs_auth.BigLakeAuthHelper(None)  # type: ignore[arg-type]
        return helper.get_token()
    except Exception:  # noqa
        import google.auth
        import google.auth.transport.requests
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        req = google.auth.transport.requests.Request()
        if not creds.valid:
            creds.refresh(req)
        return creds.token


def probe(url: str, token: str, project: str):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    # include user-project header variant
    headers_with_project = dict(headers)
    headers_with_project["x-goog-user-project"] = project
    results = []
    for hdrs, label in [(headers, "no_user_project"), (headers_with_project, "with_user_project")]:
        start = time.time()
        r = requests.get(url, headers=hdrs)
        elapsed = (time.time() - start) * 1000
        try:
            body = r.json()
        except Exception:
            body = r.text[:500]
        results.append({
            "header_mode": label,
            "status": r.status_code,
            "elapsed_ms": round(elapsed, 1),
            "body": body,
            "content_type": r.headers.get("content-type"),
        })
    return results


def main():
    load_dotenv(override=False)
    project = os.getenv("PROJECT_ID")
    catalog = os.getenv("BIGLAKE_CATALOG")
    location = os.getenv("BIGLAKE_LOCATION", "US")
    warehouse = os.getenv("ICEBERG_WAREHOUSE_PATH")

    for v in [project, catalog, warehouse]:
        if not v:
            print("Missing required env vars; ensure .env is loaded.", file=sys.stderr)
            sys.exit(1)

    token = get_token()
    encoded_wh = urllib.parse.quote(warehouse, safe="")

    summary = []
    for base in CANDIDATE_BASES:
        ep = base.format(project=project, location=location, catalog=catalog)
        url = f"{ep}{CONFIG_SUFFIX}?warehouse={encoded_wh}"
        try:
            res = probe(url, token, project)
        except Exception as e:  # noqa
            res = [{"error": str(e)}]
        summary.append({"base": ep, "config_url": url, "responses": res})

    json.dump(summary, sys.stdout, indent=2)

if __name__ == "__main__":
    main()
