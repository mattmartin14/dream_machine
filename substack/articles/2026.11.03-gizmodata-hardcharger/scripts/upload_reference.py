#!/usr/bin/env python3
"""Upload the optional federation reference Parquet file."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file",
        type=Path,
        default=Path("data/reference/customer_segment.parquet"),
    )
    parser.add_argument("--bucket", default=os.getenv("RAW_BUCKET", ""))
    parser.add_argument("--prefix", default=os.getenv("RAW_PREFIX", "tpch"))
    args = parser.parse_args()
    if not args.bucket:
        parser.error("Pass --bucket or set RAW_BUCKET")
    key = f"{args.prefix.strip('/')}/reference/customer_segment/customer_segment.parquet"
    boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2")).upload_file(
        str(args.file), args.bucket, key
    )
    print(f"Uploaded {args.file} to s3://{args.bucket}/{key} ({args.file.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
