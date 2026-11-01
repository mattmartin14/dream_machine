#!/usr/bin/env python3
"""Validate a generated dataset manifest and its local files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

TABLES = {
    "customer",
    "orders",
    "lineitem",
    "part",
    "partsupp",
    "supplier",
    "nation",
    "region",
}


def validate(data_dir: Path) -> None:
    manifest = json.loads((data_dir / "manifest.json").read_text())
    missing = TABLES - manifest["tables"].keys()
    if missing:
        raise ValueError(f"Manifest is missing TPC-H tables: {sorted(missing)}")
    for table in TABLES:
        record = manifest["tables"][table]
        file = data_dir / record["file"]
        if not file.is_file() or file.stat().st_size != record["bytes"]:
            raise ValueError(f"File missing or size mismatch for table {table}")
        if record["rows"] < 1:
            raise ValueError(f"No rows recorded for table {table}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/tpch"))
    args = parser.parse_args()
    validate(args.data_dir)
    print("TPC-H manifest and Parquet files validated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
