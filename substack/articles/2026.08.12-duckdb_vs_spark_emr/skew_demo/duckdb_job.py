from __future__ import annotations

import argparse

import boto3
import duckdb

from skew_demo.benchmark import build_benchmark_result, default_benchmark_id, upload_json, utc_now_iso, write_json
from skew_demo.config import DEFAULT_BUCKET, DEFAULT_DATASET_PREFIX, DEFAULT_ROOT_PREFIX, DemoPaths, default_run_date, ensure_local_artifact_dirs
from skew_demo.duckdb_utils import configure_duckdb_s3, elapsed_s, now_s


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DuckDB baseline ETL: direct multiline JSON read from S3")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--root-prefix", default=DEFAULT_ROOT_PREFIX)
    parser.add_argument("--dataset-prefix", default=DEFAULT_DATASET_PREFIX)
    parser.add_argument("--run-date", default=default_run_date())
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--benchmark-id", default=default_benchmark_id())
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

    conn = duckdb.connect()
    configure_duckdb_s3(conn, region=args.region)

    input_glob = paths.input_glob()
    engine_prefix = paths.engine_prefix(args.benchmark_id, "duckdb")
    aggregate_key = f"{engine_prefix}/aggregate_results.json"
    metrics_key = f"{engine_prefix}/benchmark_result.json"
    local_artifacts = ensure_local_artifact_dirs()

    t_total = now_s()
    logical_start_time = utc_now_iso()

    t_read = now_s()
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE raw_transcripts AS
        SELECT *
        FROM read_json_auto(?, maximum_object_size=100000000)
        """,
        [input_glob],
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE flat_messages AS
        SELECT
            transcript_id,
            order_id,
            customer_id,
            store_id,
            created_at,
            product_sku,
            msg.ts AS message_ts,
            msg.speaker AS speaker,
            msg.message_id AS message_id,
            msg.intent AS intent,
            msg.sentiment_score AS sentiment_score,
            msg.return_reason_code AS return_reason_code,
            msg.refund_requested AS refund_requested,
            msg.text AS text
        FROM raw_transcripts, UNNEST(messages) AS t(msg)
        """
    )
    read_seconds = elapsed_s(t_read)

    t_transform = now_s()

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE aggregates AS
        SELECT
            store_id,
            return_reason_code,
            count(*) AS message_count,
            count(DISTINCT order_id) AS orders_touched,
            avg(sentiment_score) AS avg_sentiment,
            sum(CASE WHEN refund_requested THEN 1 ELSE 0 END) AS refund_mentions
        FROM flat_messages
        GROUP BY 1, 2
        ORDER BY message_count DESC, store_id ASC, return_reason_code ASC
        """
    )
    transform_seconds = elapsed_s(t_transform)

    t_write = now_s()
    output_uri = f"s3://{args.bucket}/{aggregate_key}"
    conn.execute(
        f"""
        COPY (
            SELECT
                store_id,
                return_reason_code,
                message_count,
                orders_touched,
                avg_sentiment,
                refund_mentions
            FROM aggregates
            ORDER BY message_count DESC, store_id ASC, return_reason_code ASC
        ) TO '{output_uri}' (FORMAT JSON)
        """
    )
    write_seconds = elapsed_s(t_write)

    total_seconds = elapsed_s(t_total)
    logical_end_time = utc_now_iso()

    # Collect post-run stats outside the timed benchmark path.
    transcript_count = int(conn.execute("SELECT count(*) FROM raw_transcripts").fetchone()[0])
    message_count = int(conn.execute("SELECT count(*) FROM flat_messages").fetchone()[0])
    result_rows = int(conn.execute("SELECT count(*) FROM aggregates").fetchone()[0])

    metrics_uri = f"s3://{args.bucket}/{metrics_key}"
    metrics = build_benchmark_result(
        benchmark_id=args.benchmark_id,
        engine="duckdb",
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
    )
    upload_json(args.bucket, metrics_key, metrics, s3_client=s3)

    out = local_artifacts / "metrics" / "duckdb_benchmark_result.json"
    write_json(out, metrics)
    print(metrics["stage_timings"])
    print(f"DuckDB aggregate results written: {output_uri}")
    print(f"DuckDB benchmark metrics written: {metrics_uri}")


if __name__ == "__main__":
    main()