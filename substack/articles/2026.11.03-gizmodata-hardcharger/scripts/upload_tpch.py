#!/usr/bin/env python3
"""Upload a generated TPC-H dataset to the project's S3 raw bucket."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import boto3


def upload(
    data_dir: Path,
    bucket: str,
    prefix: str,
    *,
    client: Any | None = None,
) -> dict[str, object]:
    manifest_path = data_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if client is None:
        client = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))
    results: list[dict[str, object]] = []
    for table, details in manifest["tables"].items():
        source = data_dir / details["file"]
        key = f"{prefix.strip('/')}/{table}/{source.name}"
        client.upload_file(str(source), bucket, key)
        results.append({"table": table, "key": key, "bytes": source.stat().st_size})
    manifest_key = f"{prefix.strip('/')}/manifest.json"
    client.upload_file(str(manifest_path), bucket, manifest_key)
    results.append(
        {"table": "manifest", "key": manifest_key, "bytes": manifest_path.stat().st_size}
    )
    result = {
        "bucket": bucket,
        "prefix": prefix.strip("/"),
        "files_uploaded": len(results),
        "bytes_uploaded": sum(int(item["bytes"]) for item in results),
        "uploads": results,
    }
    (data_dir / "upload-manifest.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/tpch"))
    parser.add_argument("--bucket", default=os.getenv("RAW_BUCKET", ""))
    parser.add_argument("--prefix", default=os.getenv("RAW_PREFIX", "tpch"))
    args = parser.parse_args()
    if not args.bucket:
        parser.error("Pass --bucket or set RAW_BUCKET")
    print(json.dumps(upload(args.data_dir, args.bucket, args.prefix), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
