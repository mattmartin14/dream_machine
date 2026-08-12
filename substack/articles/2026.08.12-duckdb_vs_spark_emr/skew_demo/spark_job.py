from __future__ import annotations

import argparse
from typing import Any

import boto3
from pyspark.sql import SparkSession, functions as F

from skew_demo.benchmark import build_benchmark_result, default_benchmark_id, upload_json, utc_now_iso, write_json
from skew_demo.config import DEFAULT_BUCKET, DEFAULT_DATASET_PREFIX, DEFAULT_ROOT_PREFIX, DemoPaths, default_run_date, ensure_local_artifact_dirs
from skew_demo.runtime_utils import elapsed_s, now_s


def build_skew_analysis(raw_df: Any, input_glob: str, benchmark_id: str, run_date: str) -> dict[str, Any]:
    files_df = (
        raw_df.sparkSession.read.format("binaryFile")
        .load(input_glob)
        .select(F.col("path").alias("file_path"), F.col("length").cast("long").alias("size_bytes"))
    )
    transcript_rows_df = (
        raw_df.select(F.input_file_name().alias("file_path"))
        .groupBy("file_path")
        .count()
        .withColumnRenamed("count", "transcript_rows")
    )
    file_stats_df = files_df.join(transcript_rows_df, on="file_path", how="left").fillna(0, subset=["transcript_rows"])

    file_count = int(file_stats_df.count())
    if file_count == 0:
        return {
            "benchmark_id": benchmark_id,
            "engine": "spark",
            "run_date": run_date,
            "input_glob": input_glob,
            "file_count": 0,
            "note": "No input files were discovered for skew analysis.",
        }

    size_summary = file_stats_df.select(
        F.sum("size_bytes").alias("total_bytes"),
        F.avg("size_bytes").alias("avg_bytes"),
        F.min("size_bytes").alias("min_bytes"),
        F.max("size_bytes").alias("max_bytes"),
    ).first()

    size_quantiles = file_stats_df.approxQuantile("size_bytes", [0.5, 0.9, 0.95, 0.99], 0.01)
    row_quantiles = file_stats_df.approxQuantile("transcript_rows", [0.5, 0.95, 0.99], 0.0)

    largest_files = [
        {
            "file_path": row["file_path"],
            "size_bytes": int(row["size_bytes"]),
            "transcript_rows": int(row["transcript_rows"]),
        }
        for row in file_stats_df.orderBy(F.desc("size_bytes")).limit(20).collect()
    ]

    p50_size = float(size_quantiles[0]) if size_quantiles else 0.0
    max_size = int(size_summary["max_bytes"])
    max_to_p50_ratio = round(max_size / p50_size, 3) if p50_size > 0 else None

    return {
        "benchmark_id": benchmark_id,
        "engine": "spark",
        "run_date": run_date,
        "input_glob": input_glob,
        "file_count": file_count,
        "size_bytes": {
            "total": int(size_summary["total_bytes"]),
            "average": round(float(size_summary["avg_bytes"]), 3),
            "min": int(size_summary["min_bytes"]),
            "p50": round(float(size_quantiles[0]), 3),
            "p90": round(float(size_quantiles[1]), 3),
            "p95": round(float(size_quantiles[2]), 3),
            "p99": round(float(size_quantiles[3]), 3),
            "max": max_size,
            "max_to_p50_ratio": max_to_p50_ratio,
        },
        "transcript_rows_per_file": {
            "p50": round(float(row_quantiles[0]), 3),
            "p95": round(float(row_quantiles[1]), 3),
            "p99": round(float(row_quantiles[2]), 3),
        },
        "possible_size_skew": bool(max_to_p50_ratio is not None and max_to_p50_ratio >= 3.0),
        "largest_files": largest_files,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark ETL parity job for EMR Serverless benchmarking")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--root-prefix", default=DEFAULT_ROOT_PREFIX)
    parser.add_argument("--dataset-prefix", default=DEFAULT_DATASET_PREFIX)
    parser.add_argument("--run-date", default=default_run_date())
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--benchmark-id", default=default_benchmark_id())
    parser.add_argument("--app-name", default="duckdb-vs-spark-emr-benchmark")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = DemoPaths(
        bucket=args.bucket,
        root_prefix=args.root_prefix,
        dataset_prefix=args.dataset_prefix,
        run_date=args.run_date,
    )
    s3 = boto3.client("s3", region_name=args.region)
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()

    input_glob = paths.input_glob()
    engine_prefix = paths.engine_prefix(args.benchmark_id, "spark")
    aggregate_output_prefix = f"{engine_prefix}/aggregate_results"
    metrics_key = f"{engine_prefix}/benchmark_result.json"
    local_artifacts = ensure_local_artifact_dirs()

    t_total = now_s()
    logical_start_time = utc_now_iso()

    t_read = now_s()
    raw_df = spark.read.option("multiLine", True).json(input_glob)
    flat_df = raw_df.select(
        "transcript_id",
        "order_id",
        "customer_id",
        "store_id",
        "created_at",
        "product_sku",
        F.explode("messages").alias("msg"),
    ).select(
        "transcript_id",
        "order_id",
        "customer_id",
        "store_id",
        "created_at",
        "product_sku",
        F.col("msg.ts").alias("message_ts"),
        F.col("msg.speaker").alias("speaker"),
        F.col("msg.message_id").alias("message_id"),
        F.col("msg.intent").alias("intent"),
        F.col("msg.sentiment_score").alias("sentiment_score"),
        F.col("msg.return_reason_code").alias("return_reason_code"),
        F.col("msg.refund_requested").alias("refund_requested"),
        F.col("msg.text").alias("text"),
    )
    raw_df.createOrReplaceTempView("raw_transcripts")
    flat_df.createOrReplaceTempView("flat_messages")
    read_seconds = elapsed_s(t_read)

    t_transform = now_s()
    aggregates_df = spark.sql(
        """
        SELECT
            store_id,
            return_reason_code,
            COUNT(*) AS message_count,
            COUNT(DISTINCT order_id) AS orders_touched,
            AVG(sentiment_score) AS avg_sentiment,
            SUM(CASE WHEN refund_requested THEN 1 ELSE 0 END) AS refund_mentions
        FROM flat_messages
        GROUP BY store_id, return_reason_code
        ORDER BY message_count DESC, store_id ASC, return_reason_code ASC
        """
    )
    transform_seconds = elapsed_s(t_transform)

    t_write = now_s()
    output_uri = f"s3://{args.bucket}/{aggregate_output_prefix}/"
    aggregates_df.write.mode("overwrite").json(output_uri)
    write_seconds = elapsed_s(t_write)

    total_seconds = elapsed_s(t_total)
    logical_end_time = utc_now_iso()

    # Collect post-run stats outside the timed benchmark path.
    transcript_count = int(spark.sql("SELECT COUNT(*) AS c FROM raw_transcripts").first()["c"])
    message_count = int(spark.sql("SELECT COUNT(*) AS c FROM flat_messages").first()["c"])
    result_rows = int(aggregates_df.count())
    skew_analysis_key = f"{engine_prefix}/skew_analysis.json"
    skew_analysis = build_skew_analysis(raw_df, input_glob, args.benchmark_id, args.run_date)
    skew_analysis_uri = upload_json(args.bucket, skew_analysis_key, skew_analysis, s3_client=s3)

    metrics_uri = f"s3://{args.bucket}/{metrics_key}"
    metrics = build_benchmark_result(
        benchmark_id=args.benchmark_id,
        engine="spark",
        run_date=args.run_date,
        bucket=args.bucket,
        input_uri=input_glob,
        logical_start_time=logical_start_time,
        logical_end_time=logical_end_time,
        elapsed_seconds=total_seconds,
        counts={
            "transcripts": transcript_count,
            "messages": message_count,
            "result_rows": result_rows,
        },
        output_uri=output_uri,
        metrics_uri=metrics_uri,
        stage_timings={
            "read": read_seconds,
            "transform": transform_seconds,
            "write": write_seconds,
            "total": total_seconds,
        },
        extra={
            "skew_analysis_uri": skew_analysis_uri,
        },
    )
    upload_json(args.bucket, metrics_key, metrics, s3_client=s3)

    out = local_artifacts / "metrics" / "spark_benchmark_result.json"
    write_json(out, metrics)
    skew_out = local_artifacts / "metrics" / "spark_skew_analysis.json"
    write_json(skew_out, skew_analysis)
    print(metrics["stage_timings"])
    print(f"Spark aggregate results written: {output_uri}")
    print(f"Spark skew analysis written: {skew_analysis_uri}")
    print(f"Spark benchmark metrics written: {metrics_uri}")
    spark.stop()


if __name__ == "__main__":
    main()