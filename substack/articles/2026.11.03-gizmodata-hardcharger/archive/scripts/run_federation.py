#!/usr/bin/env python3
"""Run the optional, preconfigured Hardcharger federation query."""

from __future__ import annotations

import os
import re
from pathlib import Path

from hardcharger_demo.config import Settings
from hardcharger_demo.connection import connect

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def qualified_identifier(value: str) -> str:
    parts = value.split(".")
    if not parts or any(not IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(f"Invalid qualified SQL identifier: {value!r}")
    return ".".join(parts)


def run(query_path: Path) -> int:
    mode = os.getenv("FEDERATION_MODE", "s3tables")
    query = query_path.read_text()
    if mode == "s3tables":
        ducklake_table = qualified_identifier(os.environ["FEDERATION_DUCKLAKE_TABLE"])
        iceberg_table = qualified_identifier(os.environ["FEDERATION_ICEBERG_TABLE"])
        query = query.replace("{{FEDERATION_DUCKLAKE_TABLE}}", ducklake_table)
        query = query.replace("{{FEDERATION_ICEBERG_TABLE}}", iceberg_table)
    elif mode == "parquet":
        bucket = os.environ["RAW_BUCKET"]
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
            raise ValueError("RAW_BUCKET is not a valid S3 general-purpose bucket name")
        query = query.replace("<RAW_BUCKET>", bucket)
    else:
        raise ValueError("FEDERATION_MODE must be 's3tables' or 'parquet'")
    if "{{" in query or "}}" in query:
        raise ValueError("Federation query has unresolved placeholders")
    settings = Settings.from_env()
    settings.require_etl()
    with connect(settings) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        finally:
            cursor.close()
    if not rows:
        raise ValueError("Federated query returned no rows")
    print(f"Federation query succeeded: {len(rows)} rows")
    return len(rows)


if __name__ == "__main__":
    run(Path(os.getenv("FEDERATION_QUERY_FILE", "sql/federation.sql")))
