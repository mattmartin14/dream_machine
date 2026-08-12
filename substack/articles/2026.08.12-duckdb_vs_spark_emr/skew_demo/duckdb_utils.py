from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
from skew_demo.runtime_utils import elapsed_s, now_s, upload_bytes


def _quote(value: str) -> str:
    return value.replace("'", "''")


def configure_duckdb_s3(conn: duckdb.DuckDBPyConnection, region: str = "us-east-1") -> None:
    try:
        conn.execute("LOAD httpfs")
    except Exception:
        # Fallback for local environments where extension cache is empty.
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
    conn.execute(f"SET s3_region='{_quote(region)}'")

    conn.execute("DROP SECRET IF EXISTS aws_creds")
    conn.execute(
        """
        CREATE SECRET aws_creds (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN,
            REGION ?
        )
        """,
        [region],
    )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

