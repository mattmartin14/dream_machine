from __future__ import annotations

import json
import statistics
import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
import zstandard as zstd


def now_s() -> float:
    return time.perf_counter()


def elapsed_s(start_s: float) -> float:
    return time.perf_counter() - start_s


def create_spark_session(app_name: str, event_log_dir: str, shuffle_partitions: int = 200) -> SparkSession:
    Path(event_log_dir).mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{Path(event_log_dir).resolve()}")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.780",
        )
    )
    return builder.getOrCreate()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_event_log_metrics(event_log_dir: str) -> dict[str, Any]:
    log_dir = Path(event_log_dir)
    durations_ms: list[int] = []
    stage_durations: dict[int, list[int]] = {}

    def consume_event(event: dict[str, Any]) -> None:
        if event.get("Event") != "SparkListenerTaskEnd":
            return
        task_info = event.get("Task Info", {})
        launch = task_info.get("Launch Time")
        finish = task_info.get("Finish Time")
        stage_id = event.get("Stage ID")
        if launch is None or finish is None or stage_id is None:
            return
        duration = int(finish) - int(launch)
        durations_ms.append(duration)
        stage_durations.setdefault(int(stage_id), []).append(duration)

    for event_file in sorted(log_dir.rglob("*")):
        if event_file.is_dir() or event_file.name.startswith("."):
            continue

        if event_file.suffix == ".zstd":
            with event_file.open("rb") as compressed:
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(compressed) as reader:
                    buffer = b""
                    while True:
                        chunk = reader.read(65536)
                        if not chunk:
                            break
                        buffer += chunk
                        lines = buffer.split(b"\n")
                        buffer = lines.pop() if lines else b""
                        for line in lines:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                consume_event(json.loads(line.decode("utf-8")))
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                continue
                    if buffer.strip():
                        try:
                            consume_event(json.loads(buffer.decode("utf-8")))
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            pass
            continue

        try:
            with event_file.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        consume_event(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except UnicodeDecodeError:
            continue

    if not durations_ms:
        return {
            "task_count": 0,
            "max_task_ms": None,
            "median_task_ms": None,
            "p95_task_ms": None,
            "max_over_median": None,
            "stage_hotspots": [],
        }

    median_ms = statistics.median(durations_ms)
    sorted_durations = sorted(durations_ms)
    p95_idx = max(0, int(len(sorted_durations) * 0.95) - 1)
    p95_ms = sorted_durations[p95_idx]

    hotspots: list[dict[str, Any]] = []
    for stage_id, values in stage_durations.items():
        stage_median = statistics.median(values)
        stage_max = max(values)
        ratio = (stage_max / stage_median) if stage_median else None
        hotspots.append(
            {
                "stage_id": stage_id,
                "task_count": len(values),
                "max_task_ms": stage_max,
                "median_task_ms": stage_median,
                "max_over_median": ratio,
            }
        )

    hotspots.sort(key=lambda row: row["max_over_median"] or 0, reverse=True)

    return {
        "task_count": len(durations_ms),
        "max_task_ms": max(durations_ms),
        "median_task_ms": median_ms,
        "p95_task_ms": p95_ms,
        "max_over_median": (max(durations_ms) / median_ms) if median_ms else None,
        "stage_hotspots": hotspots[:10],
    }
