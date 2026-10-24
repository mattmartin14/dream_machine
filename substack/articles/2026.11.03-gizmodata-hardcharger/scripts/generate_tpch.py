#!/usr/bin/env python3
"""Generate standard TPC-H tables as compressed Parquet with DuckDB."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

TABLES = (
    "customer",
    "orders",
    "lineitem",
    "part",
    "partsupp",
    "supplier",
    "nation",
    "region",
)


def generate(scale_factor: int, output_dir: Path) -> dict[str, object]:
    if scale_factor < 1:
        raise ValueError("Scale factor must be a positive integer")
    output_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute("INSTALL tpch")
        connection.execute("LOAD tpch")
        connection.execute(f"CALL dbgen(sf={scale_factor})")
        tables: dict[str, dict[str, object]] = {}
        for name in TABLES:
            target = output_dir / name / f"{name}.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            count = connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            connection.execute(
                f"COPY (SELECT * FROM {name}) TO '{target.as_posix()}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
            )
            tables[name] = {
                "rows": count,
                "file": str(target.relative_to(output_dir)),
                "bytes": target.stat().st_size,
            }
    finally:
        connection.close()
    manifest: dict[str, object] = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "scale_factor": scale_factor,
        "dataset": "synthetic TPC-H data, reused as representative business-shaped data",
        "tables": tables,
        "total_bytes": sum(int(entry["bytes"]) for entry in tables.values()),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale-factor", type=int, default=1)
    parser.add_argument("--output-dir", type=Path, default=Path("data/tpch"))
    args = parser.parse_args()
    manifest = generate(args.scale_factor, args.output_dir)
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
