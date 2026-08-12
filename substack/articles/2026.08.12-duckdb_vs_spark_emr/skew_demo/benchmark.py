from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_benchmark_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def upload_json(bucket: str, key: str, payload: dict[str, Any], s3_client: Any | None = None) -> str:
    client = s3_client or boto3.client("s3")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return s3_uri(bucket, key)


def build_benchmark_result(
    *,
    benchmark_id: str,
    engine: str,
    run_date: str,
    bucket: str,
    input_uri: str,
    logical_start_time: str,
    logical_end_time: str,
    elapsed_seconds: float,
    counts: dict[str, int],
    output_uri: str,
    metrics_uri: str,
    stage_timings: dict[str, float],
    status: str = "succeeded",
    error: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "benchmark_id": benchmark_id,
        "engine": engine,
        "status": status,
        "run_date": run_date,
        "bucket": bucket,
        "input_uri": input_uri,
        "logical_start_time": logical_start_time,
        "logical_end_time": logical_end_time,
        "elapsed_seconds": round(elapsed_seconds, 6),
        "counts": counts,
        "stage_timings": {key: round(value, 6) for key, value in stage_timings.items()},
        "output_uri": output_uri,
        "metrics_uri": metrics_uri,
        "emitted_at": utc_now_iso(),
    }
    if error is not None:
        payload["error"] = error
    if extra:
        payload["extra"] = extra
    return payload