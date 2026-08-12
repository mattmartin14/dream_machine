from __future__ import annotations

import boto3
import time
from typing import Any


def now_s() -> float:
    return time.perf_counter()


def elapsed_s(start_s: float) -> float:
    return time.perf_counter() - start_s


def upload_bytes(bucket: str, key: str, body: bytes, content_type: str, s3_client: Any | None = None) -> str:
    client = s3_client or boto3.client("s3")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    return f"s3://{bucket}/{key}"