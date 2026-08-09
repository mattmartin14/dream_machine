from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Any

import boto3
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from skew_demo.config import (
    DEFAULT_BUCKET,
    DEFAULT_DATASET_PREFIX,
    DEFAULT_ROOT_PREFIX,
    DemoPaths,
    default_run_date,
    ensure_local_artifact_dirs,
)
from skew_demo.spark_utils import create_spark_session, elapsed_s, now_s, parse_event_log_metrics, write_json
from skew_demo.transforms import aggregate_returns, flatten_transcript_messages


FLAT_SCHEMA = (
    "transcript_id string, order_id string, customer_id string, store_id string, "
    "created_at string, product_sku string, message_ts string, speaker string, "
    "message_id string, intent string, sentiment_score double, return_reason_code string, "
    "refund_requested boolean, text string, source_key string"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Optimized Spark ETL: size-aware normalization for skew")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--root-prefix", default=DEFAULT_ROOT_PREFIX)
    parser.add_argument("--dataset-prefix", default=DEFAULT_DATASET_PREFIX)
    parser.add_argument("--run-date", default=default_run_date())
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--large-threshold-mb", type=int, default=10)
    parser.add_argument("--chunk-message-count", type=int, default=1500)
    parser.add_argument("--output-prefix", default="etl-skew-demo/output/optimized")
    return parser.parse_args()


def list_objects(bucket: str, prefix: str) -> list[dict[str, Any]]:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    objects: list[dict[str, Any]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                objects.append({"Key": key, "Size": int(obj["Size"])})
    return objects


def normalize_large_object(
    s3_client: Any,
    bucket: str,
    key: str,
    normalized_prefix: str,
    chunk_message_count: int,
) -> list[str]:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    doc = json.loads(body)

    base = {
        "transcript_id": doc.get("transcript_id"),
        "order_id": doc.get("order_id"),
        "customer_id": doc.get("customer_id"),
        "store_id": doc.get("store_id"),
        "created_at": doc.get("created_at"),
        "product_sku": doc.get("product_sku"),
    }

    messages = doc.get("messages", [])
    normalized_keys: list[str] = []

    for i in range(0, len(messages), chunk_message_count):
        chunk = messages[i : i + chunk_message_count]
        lines = []
        for msg in chunk:
            row = {
                **base,
                "message_ts": msg.get("ts"),
                "speaker": msg.get("speaker"),
                "message_id": msg.get("message_id"),
                "intent": msg.get("intent"),
                "sentiment_score": msg.get("sentiment_score"),
                "return_reason_code": msg.get("return_reason_code"),
                "refund_requested": msg.get("refund_requested"),
                "text": msg.get("text"),
                "source_key": key,
            }
            lines.append(json.dumps(row))

        part_key = f"{normalized_prefix}/{doc.get('order_id')}/part-{i // chunk_message_count:05d}.jsonl"
        s3_client.put_object(
            Bucket=bucket,
            Key=part_key,
            Body=("\n".join(lines) + "\n").encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        normalized_keys.append(part_key)

    return normalized_keys


def read_small_files(spark: SparkSession, bucket: str, keys: list[str]) -> DataFrame:
    if not keys:
        return spark.createDataFrame([], FLAT_SCHEMA)
    paths = [f"s3a://{bucket}/{k}" for k in keys]
    raw_small = spark.read.option("multiline", "true").json(paths)
    return flatten_transcript_messages(raw_small).withColumn("source_key", F.input_file_name())


def read_normalized_large_files(spark: SparkSession, bucket: str, normalized_keys: list[str]) -> DataFrame:
    if not normalized_keys:
        return spark.createDataFrame([], FLAT_SCHEMA)
    paths = [f"s3a://{bucket}/{k}" for k in normalized_keys]
    return spark.read.json(paths)


def main() -> None:
    args = parse_args()
    paths = DemoPaths(
        bucket=args.bucket,
        root_prefix=args.root_prefix,
        dataset_prefix=args.dataset_prefix,
        run_date=args.run_date,
    )

    artifacts = ensure_local_artifact_dirs()
    event_log_dir = artifacts / "event_logs" / "optimized"
    shutil.rmtree(event_log_dir, ignore_errors=True)
    event_log_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session(
        app_name=f"json-skew-optimized-{args.run_date}",
        event_log_dir=str(event_log_dir),
        shuffle_partitions=args.shuffle_partitions,
    )

    start_total = now_s()

    source_prefix = f"{paths.raw_prefix}/"
    threshold_bytes = args.large_threshold_mb * 1024 * 1024

    t_list = now_s()
    objects = list_objects(args.bucket, source_prefix)
    large_objects = [obj for obj in objects if obj["Size"] >= threshold_bytes]
    small_objects = [obj for obj in objects if obj["Size"] < threshold_bytes]
    listing_seconds = elapsed_s(t_list)

    t_normalize = now_s()
    s3 = boto3.client("s3")
    normalized_prefix = paths.normalized_prefix
    normalized_keys: list[str] = []
    for obj in large_objects:
        normalized_keys.extend(
            normalize_large_object(
                s3_client=s3,
                bucket=args.bucket,
                key=obj["Key"],
                normalized_prefix=normalized_prefix,
                chunk_message_count=args.chunk_message_count,
            )
        )
    normalize_seconds = elapsed_s(t_normalize)

    t_read = now_s()
    small_flat = read_small_files(spark, args.bucket, [obj["Key"] for obj in small_objects])
    large_flat = read_normalized_large_files(spark, args.bucket, normalized_keys)
    unified = small_flat.unionByName(large_flat, allowMissingColumns=True).persist()
    message_count = unified.count()
    read_seconds = elapsed_s(t_read)

    t_transform = now_s()
    partition_sizes = unified.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    agg_df = aggregate_returns(unified)
    agg_rows = [row.asDict() for row in agg_df.limit(25).collect()]
    transform_seconds = elapsed_s(t_transform)

    output_path = f"s3a://{args.bucket}/{args.output_prefix}/run_date={args.run_date}"
    agg_df.coalesce(1).write.mode("overwrite").json(output_path)

    total_seconds = elapsed_s(start_total)
    event_metrics = parse_event_log_metrics(str(event_log_dir))

    metrics = {
        "mode": "optimized",
        "run_date": args.run_date,
        "bucket": args.bucket,
        "input_prefix": source_prefix,
        "normalized_prefix": normalized_prefix,
        "output_path": output_path,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "object_distribution": {
            "total_files": len(objects),
            "small_files": len(small_objects),
            "large_files": len(large_objects),
            "threshold_bytes": threshold_bytes,
            "normalized_chunk_files": len(normalized_keys),
        },
        "timing_seconds": {
            "listing": listing_seconds,
            "normalize": normalize_seconds,
            "read": read_seconds,
            "transform": transform_seconds,
            "total": total_seconds,
        },
        "counts": {
            "messages": message_count,
            "result_rows": len(agg_rows),
        },
        "partition_row_counts": partition_sizes,
        "partition_stats": {
            "partition_count": len(partition_sizes),
            "largest_partition_rows": max(partition_sizes) if partition_sizes else 0,
            "median_partition_rows": sorted(partition_sizes)[len(partition_sizes) // 2] if partition_sizes else 0,
            "largest_over_median": (
                (max(partition_sizes) / max(1, sorted(partition_sizes)[len(partition_sizes) // 2]))
                if partition_sizes
                else None
            ),
        },
        "task_metrics": event_metrics,
        "top_aggregates_preview": agg_rows,
    }

    metrics_path = Path("artifacts/metrics/optimized_metrics.json")
    write_json(metrics_path, metrics)
    print(json.dumps(metrics["timing_seconds"], indent=2))
    print(f"Optimized metrics written: {metrics_path}")

    spark.stop()


if __name__ == "__main__":
    main()
