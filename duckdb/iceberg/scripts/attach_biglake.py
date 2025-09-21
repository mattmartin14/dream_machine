#!/usr/bin/env python3
"""Attach DuckDB to a BigLake Iceberg REST catalog.

This script attempts to perform an ATTACH using DuckDB's Iceberg extension
against the Google BigLake managed Iceberg REST catalog.

Current Hypothesis (BigLake REST):
  The URI for Spark examples is a generic endpoint:
    https://biglake.googleapis.com/iceberg/v1beta/restcatalog
  Spark then passes a warehouse (gs://bucket/path) and x-goog-user-project header.

DuckDB ATTACH for Iceberg expects either:
  ATTACH 'warehouse' AS name (
      TYPE ICEBERG,
      ENDPOINT '<host>/<maybe path>',
      AUTHORIZATION_TYPE OAUTH2,
      TOKEN '<token>'
  );

Empirically, DuckDB will call <ENDPOINT>/v1/config?warehouse=<encoded warehouse>
So for BigLake we try endpoint = https://biglake.googleapis.com/iceberg/v1beta/restcatalog

If Google requires the x-goog-user-project header, DuckDB iceberg extension
currently doesn't expose custom headers in ATTACH, so we may need to
set an ICEBERG secret (future enhancement) or rely on billing project
being inferred from the token.

This script:
  1. Loads env vars / .env
  2. Ensures extensions installed
  3. Gets OAuth token via google-auth (ADC)
  4. Attempts ATTACH with several candidate endpoints
  5. Reports first success or detailed failures.
"""
from __future__ import annotations
import os
import sys
import json
import time
import argparse
import threading
import duckdb
from pathlib import Path
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # noqa
    def load_dotenv(*args, **kwargs):  # type: ignore
        # Graceful no-op if python-dotenv not installed
        return False

# Local helper
import gcs_auth

CANDIDATE_ENDPOINTS = [
    # Documented generic REST catalog endpoint (Spark docs)
    "https://biglake.googleapis.com/iceberg/v1beta/restcatalog",
    # Potential fully scoped endpoint style (pattern guessing)
    "https://biglake.googleapis.com/iceberg/v1beta/projects/{project}/locations/{location}/catalogs/{catalog}",
]


def load_env():
    load_dotenv(override=False)
    required = [
        "PROJECT_ID", "BIGLAKE_CATALOG", "BIGLAKE_LOCATION", "GCS_BUCKET", "ICEBERG_WAREHOUSE_PATH"
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"Missing required environment variables: {missing}", file=sys.stderr)
        sys.exit(1)


def ensure_extensions(con: duckdb.DuckDBPyConnection):
    con.execute("INSTALL httpfs; INSTALL iceberg;")
    con.execute("LOAD httpfs; LOAD iceberg;")


def attempt_attach(con: duckdb.DuckDBPyConnection, endpoint: str, token: str, warehouse: str, name: str = "biglake"):
    sql = f"""
    ATTACH '{warehouse}' AS {name} (
        TYPE ICEBERG,
        ENDPOINT '{endpoint}',
        AUTHORIZATION_TYPE OAUTH2,
        TOKEN '{token}'
    );
    """
    try:
        start = time.time()
        con.execute(sql)
        elapsed = (time.time() - start) * 1000
        print(f"SUCCESS: Attached using endpoint={endpoint} in {elapsed:.1f} ms")
        return True, None
    except Exception as e:  # noqa
        return False, str(e)


def main():
    parser = argparse.ArgumentParser(description="Attach DuckDB to BigLake Iceberg REST catalog")
    parser.add_argument("--dry-run", action="store_true", help="Print planned ATTACH attempts without executing")
    parser.add_argument("--embedded-proxy", action="store_true", help="Launch header-injecting proxy thread automatically (implies USE_PROXY=true)")
    parser.add_argument("--proxy-port", type=int, default=int(os.getenv("PROXY_PORT", "8080")), help="Port for proxy if embedded")
    args = parser.parse_args()

    load_env()
    project = os.getenv("PROJECT_ID")
    catalog = os.getenv("BIGLAKE_CATALOG")
    location = os.getenv("BIGLAKE_LOCATION", "US")
    warehouse = os.getenv("ICEBERG_WAREHOUSE_PATH")
    token_override = os.getenv("OAUTH_TOKEN")

    if args.embedded_proxy:
        os.environ["USE_PROXY"] = "true"
        os.environ["PROXY_PORT"] = str(args.proxy_port)

    use_proxy = os.getenv("USE_PROXY", "false").lower() == "true"
    proxy_port = os.getenv("PROXY_PORT", "8080")

    # Optionally start embedded proxy inside this process
    proxy_thread = None
    if args.embedded_proxy:
        try:
            from scripts.proxy_biglake import run as run_proxy  # type: ignore
        except Exception:
            # Fallback relative import when executed from scripts directory
            from proxy_biglake import run as run_proxy  # type: ignore

        def launch_proxy():
            os.environ.setdefault("PORT", str(args.proxy_port))
            run_proxy()

        proxy_thread = threading.Thread(target=launch_proxy, daemon=True)
        proxy_thread.start()
        # Give proxy a moment to bind
        time.sleep(0.8)
        print(f"Embedded proxy started on port {args.proxy_port}")

    formatted_candidates = []
    if use_proxy:
        formatted_candidates = [f"http://localhost:{proxy_port}"]
        print(f"Using local proxy endpoint(s): {formatted_candidates}")
    else:
        formatted_candidates = [c.format(project=project, location=location, catalog=catalog) for c in CANDIDATE_ENDPOINTS]

    # Prepare token only if not dry-run
    token = "(dry-run-token)"
    con = None
    if not args.dry_run:
        con = duckdb.connect()
        ensure_extensions(con)
        helper = gcs_auth.BigLakeAuthHelper(con)
        token = token_override or helper.get_token()

    print("Planned ATTACH attempts:")
    for ep in formatted_candidates:
        print(f"  - ATTACH '{warehouse}' (TYPE ICEBERG, ENDPOINT '{ep}', AUTHORIZATION_TYPE OAUTH2)")

    if args.dry_run:
        print("Dry run complete. No network or DuckDB calls executed.")
        return

    results = []
    assert con is not None
    for ep in formatted_candidates:
        ok, err = attempt_attach(con, ep, token, warehouse)
        results.append({"endpoint": ep, "ok": ok, "error": err})
        if ok:
            break

    success = any(r["ok"] for r in results)
    if not success:
        print("All attempts failed. Detailed errors:")
        for r in results:
            print("-- Endpoint:", r["endpoint"])
            print("   Error:")
            print("   " + r["error"].replace("\n", "\n   "))
        print("\nTroubleshooting suggestions:")
        print("1. Confirm the BigLake REST catalog feature is enabled (preview).")
        print("2. Verify the token has roles biglake.admin + storage.admin on billing project.")
        print("3. Try creating a table with Spark first to ensure catalog operational.")
        print("4. The actual endpoint path shape might differ; consult latest release notes.")
        print("5. Capture HTTP trace by enabling DuckDB logs: SET log_level='debug';")
        sys.exit(2)
    else:
        try:
            print("Listing schemas (SHOW SCHEMAS):")
            rows = con.execute("SHOW SCHEMAS").fetchall()
            for r in rows:
                print("  -", r)
        except Exception as e:  # noqa
            print(f"Warning: attached but listing schemas failed: {e}")

        maybe_create_resources(con)
        print("Done.")

def maybe_create_resources(con: duckdb.DuckDBPyConnection):
    """Optionally create a schema + table + insert sample rows if env flags set.

    Controlled by env vars:
      CREATE_SCHEMA (default: true)
      TARGET_SCHEMA (default: env BIGLAKE_DATABASE or 'db1')
      CREATE_TABLE (default: true)
      TARGET_TABLE (default: 'demo_iceberg')
    """
    create_schema = os.getenv("CREATE_SCHEMA", "true").lower() == "true"
    create_table = os.getenv("CREATE_TABLE", "true").lower() == "true"
    schema_name = os.getenv("TARGET_SCHEMA") or os.getenv("BIGLAKE_DATABASE", "db1")
    table_name = os.getenv("TARGET_TABLE", "demo_iceberg")

    # Switch to attached database context: ATTACH gave it name 'biglake'
    # DuckDB uses qualified form biglake.schema.table
    if create_schema:
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS biglake.{schema_name};")
            print(f"Created/verified schema biglake.{schema_name}")
        except Exception as e:  # noqa
            print(f"Schema creation failed (non-fatal): {e}")

    if create_table:
        try:
            con.execute(f"CREATE TABLE IF NOT EXISTS biglake.{schema_name}.{table_name} (id INT, msg STRING);")
            print(f"Created/verified table biglake.{schema_name}.{table_name}")
            con.execute(f"INSERT INTO biglake.{schema_name}.{table_name} VALUES (1,'hello'),(2,'world');")
            print("Inserted sample rows.")
            out = con.execute(f"SELECT * FROM biglake.{schema_name}.{table_name} ORDER BY id").fetchall()
            print("Sample query result:", out)
        except Exception as e:  # noqa
            print(f"Table create/insert failed (non-fatal): {e}")

if __name__ == "__main__":
    main()
