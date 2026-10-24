#!/usr/bin/env python3
"""Check ADBC connectivity and AWS-side raw bucket access without a data scan."""

from __future__ import annotations

import logging

import boto3

from hardcharger_demo.config import Settings
from hardcharger_demo.connection import connect

LOGGER = logging.getLogger(__name__)


def main() -> int:
    settings = Settings.from_env()
    settings.require_etl()
    with connect(settings) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT 42")
            answer = cursor.fetchone()[0]
            if answer != 42:
                raise RuntimeError("Hardcharger connectivity check returned an unexpected result")
            cursor.execute("SELECT current_database()")
            current_catalog = cursor.fetchone()[0]
        finally:
            cursor.close()
    s3 = boto3.client("s3", region_name=settings.aws_region)
    s3.list_objects_v2(
        Bucket=settings.raw_bucket,
        Prefix=f"{settings.raw_prefix}/",
        MaxKeys=1,
    )
    print(
        "Smoke test passed: Hardcharger query and AWS caller's S3 listing succeeded; "
        f"session catalog={current_catalog!r}. "
        "This does not verify the managed service's S3 identity."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
