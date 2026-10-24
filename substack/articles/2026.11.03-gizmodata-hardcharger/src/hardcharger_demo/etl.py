"""Date-windowed ETL orchestration for Hardcharger."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from contextlib import nullcontext
from datetime import date
from typing import Any

import boto3

from hardcharger_demo.config import Settings, select_processing_dates
from hardcharger_demo.connection import connect
from hardcharger_demo.locking import EtlLock
from hardcharger_demo.logging_config import configure_logging
from hardcharger_demo.metrics import publish_metric
from hardcharger_demo.validation import DailyResult, DataValidationError, validate_daily_result

LOGGER = logging.getLogger(__name__)


def _identifier(value: str) -> str:
    if not value or not value.replace("_", "").isalnum() or value[0].isdigit():
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _table_name(settings: Settings, table: str) -> str:
    return ".".join(
        _identifier(part) for part in (settings.ducklake_catalog, settings.ducklake_schema, table)
    )


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _daily_sql(
    settings: Settings,
    processing_date: date,
) -> tuple[str, str, str]:
    """Build the daily replacement statements with values bound separately."""
    raw = f"s3://{settings.raw_bucket}/{settings.raw_prefix}"
    orders_path = _sql_string(f"{raw}/orders/*.parquet")
    lineitem_path = _sql_string(f"{raw}/lineitem/*.parquet")
    customer_path = _sql_string(f"{raw}/customer/*.parquet")
    nation_path = _sql_string(f"{raw}/nation/*.parquet")
    sales_table = _table_name(settings, settings.daily_table)
    nation_table = _table_name(settings, settings.nation_table)
    daily_sql = f"""
        INSERT INTO {sales_table}
        WITH orders AS (
            SELECT o_orderkey, o_orderdate, o_custkey
            FROM read_parquet({orders_path})
            WHERE o_orderdate = CAST(? AS DATE)
        ),
        lineitems AS (
            SELECT l_orderkey, l_extendedprice, l_discount
            FROM read_parquet({lineitem_path})
        ),
        daily AS (
            SELECT o.o_orderdate AS order_date,
                   COUNT(DISTINCT o.o_orderkey) AS order_count,
                   COUNT(*) AS line_item_count,
                   SUM(l.l_extendedprice) AS gross_revenue,
                   SUM(l.l_extendedprice * l.l_discount) AS discount_amount,
                   SUM(l.l_extendedprice * (1 - l.l_discount)) AS net_revenue
            FROM orders o JOIN lineitems l ON l.l_orderkey = o.o_orderkey
            GROUP BY o.o_orderdate
        )
        SELECT order_date, order_count, line_item_count, gross_revenue,
               discount_amount, net_revenue,
               CASE WHEN order_count = 0 THEN 0
                    ELSE net_revenue / order_count END
        FROM daily
    """
    nation_sql = f"""
        INSERT INTO {nation_table}
        WITH orders AS (
            SELECT o.o_orderkey, o.o_orderdate, c.c_nationkey,
                   CAST(n.n_regionkey AS VARCHAR) AS regionkey
            FROM read_parquet({orders_path}) o
            JOIN read_parquet({customer_path}) c ON c.c_custkey = o.o_custkey
            JOIN read_parquet({nation_path}) n ON n.n_nationkey = c.c_nationkey
            WHERE o.o_orderdate = CAST(? AS DATE)
        )
        SELECT o.o_orderdate, o.c_nationkey, o.regionkey,
               COUNT(DISTINCT o.o_orderkey), COUNT(*),
               SUM(l.l_extendedprice),
               SUM(l.l_extendedprice * l.l_discount),
               SUM(l.l_extendedprice * (1 - l.l_discount))
        FROM orders o JOIN read_parquet({lineitem_path}) l
          ON l.l_orderkey = o.o_orderkey
        GROUP BY o.o_orderdate, o.c_nationkey, o.regionkey
    """
    return daily_sql, nation_sql, processing_date.isoformat()


def _read_source_manifest(settings: Settings, client: Any | None = None) -> dict[str, Any]:
    if client is None:
        client = boto3.client("s3", region_name=settings.aws_region)
    key = f"{settings.raw_prefix}/manifest.json"
    response = client.get_object(Bucket=settings.raw_bucket, Key=key)
    manifest = json.loads(response["Body"].read())
    if not isinstance(manifest.get("tables"), dict):
        raise ValueError("S3 TPC-H manifest has no tables mapping")
    if not isinstance(manifest.get("total_bytes"), int):
        raise ValueError("S3 TPC-H manifest has no valid total_bytes")
    return manifest


def _run_date(connection: object, settings: Settings, processing_date: date) -> tuple[int, float]:
    sales_table = _table_name(settings, settings.daily_table)
    nation_table = _table_name(settings, settings.nation_table)
    daily_sql, nation_sql, date_text = _daily_sql(settings, processing_date)
    cur = connection.cursor()
    start = time.monotonic()
    try:
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {sales_table} (
                order_date DATE, order_count BIGINT, line_item_count BIGINT,
                gross_revenue DECIMAL(18, 2), discount_amount DECIMAL(18, 2),
                net_revenue DECIMAL(18, 2), average_order_value DECIMAL(18, 2)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {nation_table} (
                order_date DATE, nationkey INTEGER, regionkey VARCHAR,
                order_count BIGINT, line_item_count BIGINT,
                gross_revenue DECIMAL(18, 2), discount_amount DECIMAL(18, 2),
                net_revenue DECIMAL(18, 2)
            )
            """
        )
        # Both replacements run in one ADBC transaction. The deploy smoke test
        # must confirm the configured DuckLake catalog supports these operations.
        try:
            for table in (sales_table, nation_table):
                cur.execute(f"DELETE FROM {table} WHERE order_date = ?", [date_text])
            cur.execute(daily_sql, [date_text])
            cur.execute(nation_sql, [date_text])
            cur.execute(
                f"""
                SELECT CAST(order_date AS VARCHAR), order_count, line_item_count,
                       gross_revenue, discount_amount, net_revenue, average_order_value
                FROM {sales_table} WHERE order_date = ?
                """,
                [date_text],
            )
            daily_record = cur.fetchone()
            cur.execute(
                f"""
                SELECT COALESCE(SUM(order_count), 0),
                       COALESCE(SUM(line_item_count), 0),
                       COALESCE(SUM(gross_revenue), 0),
                       COALESCE(SUM(discount_amount), 0),
                       COALESCE(SUM(net_revenue), 0)
                FROM {nation_table} WHERE order_date = ?
                """,
                [date_text],
            )
            nation_totals = cur.fetchone()
            if daily_record is None:
                if any(float(value) != 0 for value in nation_totals):
                    raise DataValidationError(
                        "Nation aggregation has rows but daily aggregation is empty"
                    )
                output_rows = 0
            else:
                result = DailyResult(
                    order_date=str(daily_record[0])[:10],
                    order_count=int(daily_record[1]),
                    line_item_count=int(daily_record[2]),
                    gross_revenue=float(daily_record[3]),
                    discount_amount=float(daily_record[4]),
                    net_revenue=float(daily_record[5]),
                    average_order_value=float(daily_record[6]),
                )
                validate_daily_result(result)
                expected = (
                    result.order_count,
                    result.line_item_count,
                    result.gross_revenue,
                    result.discount_amount,
                    result.net_revenue,
                )
                for actual, expected_value in zip(nation_totals, expected, strict=True):
                    tolerance = max(0.01, abs(float(expected_value)) * 1e-8)
                    if abs(float(actual) - float(expected_value)) > tolerance:
                        raise DataValidationError(
                            "Nation-level totals do not reconcile to daily sales"
                        )
                output_rows = 1
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return output_rows, time.monotonic() - start
    finally:
        cur.close()


def run(settings: Settings, dates: list[date]) -> None:
    settings.require_etl()
    execution_id = str(uuid.uuid4())
    started = time.monotonic()
    LOGGER.info(
        "ETL started",
        extra={"execution_id": execution_id, "status": "started"},
    )
    try:
        source_manifest = _read_source_manifest(settings)
        source_rows = {
            table: int(values["rows"]) for table, values in source_manifest["tables"].items()
        }
        source_bytes = int(source_manifest["total_bytes"])
        lock = (
            EtlLock(settings.lock_table, execution_id, region=settings.aws_region)
            if settings.lock_table
            else nullcontext()
        )
        if not settings.lock_table:
            LOGGER.warning("DDB_LOCK_TABLE is unset; no distributed execution lock is active")
        with lock:
            with connect(settings) as connection:
                for processing_date in dates:
                    rows, sql_duration = _run_date(connection, settings, processing_date)
                    LOGGER.info(
                        "Processing date completed",
                        extra={
                            "execution_id": execution_id,
                            "processing_date": processing_date.isoformat(),
                            "duration_seconds": round(sql_duration, 3),
                            "rows": rows,
                            "status": "success",
                            "source_rows": source_rows,
                            "source_bytes": source_bytes,
                            "validation_status": "passed",
                        },
                    )
                    publish_metric(
                        "OutputRows",
                        rows,
                        region=settings.aws_region,
                        dimensions={"Environment": os.getenv("ENVIRONMENT", "demo")},
                    )
        duration = time.monotonic() - started
        publish_metric(
            "ETLDuration",
            duration,
            region=settings.aws_region,
            dimensions={"Environment": os.getenv("ENVIRONMENT", "demo")},
        )
        publish_metric(
            "ETLFailures",
            0,
            region=settings.aws_region,
            dimensions={"Environment": os.getenv("ENVIRONMENT", "demo")},
        )
        LOGGER.info(
            "ETL completed",
            extra={
                "execution_id": execution_id,
                "duration_seconds": round(duration, 3),
                "status": "success",
            },
        )
    except Exception as error:
        try:
            publish_metric(
                "ETLFailures",
                1,
                region=settings.aws_region,
                dimensions={"Environment": os.getenv("ENVIRONMENT", "demo")},
            )
            if isinstance(error, DataValidationError):
                publish_metric(
                    "ValidationFailures",
                    1,
                    region=settings.aws_region,
                    dimensions={"Environment": os.getenv("ENVIRONMENT", "demo")},
                )
        except Exception:
            LOGGER.exception("Failed to publish failure metric")
        LOGGER.exception(
            "ETL failed",
            extra={"execution_id": execution_id, "status": "failure"},
        )
        raise


def main() -> int:
    configure_logging(os.getenv("LOG_LEVEL", "INFO"))
    try:
        settings = Settings.from_env()
        dates = select_processing_dates(
            processing_date=os.getenv("PROCESSING_DATE") or None,
            start_date=os.getenv("PROCESSING_START_DATE") or None,
            end_date=os.getenv("PROCESSING_END_DATE") or None,
        )
        run(settings, dates)
    except Exception:
        LOGGER.exception("ETL process terminated")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
