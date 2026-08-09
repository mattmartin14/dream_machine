from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import duckdb


def now_s() -> float:
    return time.perf_counter()


def elapsed_s(start_s: float) -> float:
    return time.perf_counter() - start_s


def _quote(value: str) -> str:
    return value.replace("'", "''")


def configure_duckdb_s3(conn: duckdb.DuckDBPyConnection, region: str = "us-east-1") -> None:
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    conn.execute(f"SET s3_region='{_quote(region)}'")

    # Let DuckDB resolve credentials the same way the AWS SDK/CLI does.
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
