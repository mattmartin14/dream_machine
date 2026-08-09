from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import shutil

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baseline Spark ETL: direct multiline JSON read")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--root-prefix", default=DEFAULT_ROOT_PREFIX)
    parser.add_argument("--dataset-prefix", default=DEFAULT_DATASET_PREFIX)
    parser.add_argument("--run-date", default=default_run_date())
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--output-prefix", default="etl-skew-demo/output/baseline")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = DemoPaths(
        bucket=args.bucket,
        root_prefix=args.root_prefix,
        dataset_prefix=args.dataset_prefix,
        run_date=args.run_date,
    )

    artifacts = ensure_local_artifact_dirs()
    event_log_dir = artifacts / "event_logs" / "baseline"
    shutil.rmtree(event_log_dir, ignore_errors=True)
    event_log_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session(
        app_name=f"json-skew-baseline-{args.run_date}",
        event_log_dir=str(event_log_dir),
        shuffle_partitions=args.shuffle_partitions,
    )

    start_total = now_s()

    input_path = f"s3a://{args.bucket}/{paths.raw_prefix}/*/chat_*.json"
    print(f"Reading baseline data from: {input_path}")

    t_read = now_s()
    raw_df = spark.read.option("multiline", "true").json(input_path)
    transcript_count = raw_df.count()
    read_seconds = elapsed_s(t_read)

    t_transform = now_s()
    flat_df = flatten_transcript_messages(raw_df).persist()
    message_count = flat_df.count()

    partition_sizes = flat_df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    agg_df = aggregate_returns(flat_df)
    agg_rows = [row.asDict() for row in agg_df.limit(25).collect()]
    transform_seconds = elapsed_s(t_transform)

    output_path = f"s3a://{args.bucket}/{args.output_prefix}/run_date={args.run_date}"
    agg_df.coalesce(1).write.mode("overwrite").json(output_path)

    total_seconds = elapsed_s(start_total)
    event_metrics = parse_event_log_metrics(str(event_log_dir))

    metrics = {
        "mode": "baseline",
        "run_date": args.run_date,
        "bucket": args.bucket,
        "input_path": input_path,
        "output_path": output_path,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "timing_seconds": {
            "read": read_seconds,
            "transform": transform_seconds,
            "total": total_seconds,
        },
        "counts": {
            "transcripts": transcript_count,
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

    metrics_path = Path("artifacts/metrics/baseline_metrics.json")
    write_json(metrics_path, metrics)
    print(json.dumps(metrics["timing_seconds"], indent=2))
    print(f"Baseline metrics written: {metrics_path}")

    spark.stop()


if __name__ == "__main__":
    main()
