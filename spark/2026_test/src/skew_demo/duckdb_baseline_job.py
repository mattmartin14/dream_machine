from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from skew_demo.config import DEFAULT_BUCKET, DEFAULT_DATASET_PREFIX, DEFAULT_ROOT_PREFIX, DemoPaths, default_run_date
from skew_demo.duckdb_utils import configure_duckdb_s3, elapsed_s, now_s, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DuckDB baseline ETL: direct multiline JSON read from S3")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--root-prefix", default=DEFAULT_ROOT_PREFIX)
    parser.add_argument("--dataset-prefix", default=DEFAULT_DATASET_PREFIX)
    parser.add_argument("--run-date", default=default_run_date())
    parser.add_argument("--region", default="us-east-1")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = DemoPaths(
        bucket=args.bucket,
        root_prefix=args.root_prefix,
        dataset_prefix=args.dataset_prefix,
        run_date=args.run_date,
    )

    conn = duckdb.connect()
    configure_duckdb_s3(conn, region=args.region)

    input_glob = f"s3://{args.bucket}/{paths.raw_prefix}/*/chat_*.json"

    t_total = now_s()

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
    transcript_count = conn.execute("SELECT count(*) FROM raw_transcripts").fetchone()[0]
    read_seconds = elapsed_s(t_read)

    t_transform = now_s()
    message_count = conn.execute("SELECT count(*) FROM flat_messages").fetchone()[0]

    aggregates = conn.execute(
        """
        SELECT
            store_id,
            return_reason_code,
            count(*) AS message_count,
            count(DISTINCT order_id) AS orders_touched,
            avg(sentiment_score) AS avg_sentiment,
            sum(CASE WHEN refund_requested THEN 1 ELSE 0 END) AS refund_mentions
        FROM flat_messages
        GROUP BY 1, 2
        ORDER BY message_count DESC
        LIMIT 25
        """
    ).fetchall()
    transform_seconds = elapsed_s(t_transform)

    total_seconds = elapsed_s(t_total)

    metrics = {
        "mode": "duckdb_baseline",
        "run_date": args.run_date,
        "bucket": args.bucket,
        "input_glob": input_glob,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "timing_seconds": {
            "read": read_seconds,
            "transform": transform_seconds,
            "total": total_seconds,
        },
        "counts": {
            "transcripts": int(transcript_count),
            "messages": int(message_count),
            "result_rows": len(aggregates),
        },
        "top_aggregates_preview": [
            {
                "store_id": row[0],
                "return_reason_code": row[1],
                "message_count": int(row[2]),
                "orders_touched": int(row[3]),
                "avg_sentiment": float(row[4]) if row[4] is not None else None,
                "refund_mentions": int(row[5]),
            }
            for row in aggregates
        ],
    }

    out = Path("artifacts/metrics/duckdb_baseline_metrics.json")
    write_json(out, metrics)
    print(metrics["timing_seconds"])
    print(f"DuckDB baseline metrics written: {out}")


if __name__ == "__main__":
    main()
