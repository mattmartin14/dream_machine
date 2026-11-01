#!/usr/bin/env python3
"""Generate a tiny synthetic nation-to-segment lookup for optional federation."""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def generate(output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "nationkey": pa.array(range(25), type=pa.int32()),
            "segment": [
                "synthetic_a" if nationkey % 2 == 0 else "synthetic_b"
                for nationkey in range(25)
            ],
        }
    )
    pq.write_table(table, output, compression="zstd")
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/reference/customer_segment.parquet"),
    )
    args = parser.parse_args()
    path = generate(args.output)
    print(f"Created synthetic reference data at {path} ({path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
