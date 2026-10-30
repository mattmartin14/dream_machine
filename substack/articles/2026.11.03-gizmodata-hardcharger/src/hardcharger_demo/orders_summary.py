"""POC job: Hardcharger reads Parquet from S3, aggregates it, and keeps the result as a table.

Fargate only sends SQL. Hardcharger reads the source through the S3 secret mounted
on the cluster, and the result table lives inside Hardcharger (no S3 output).
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from contextlib import nullcontext

from hardcharger_demo.config import Settings
from hardcharger_demo.connection import connect
from hardcharger_demo.locking import EtlLock
from hardcharger_demo.logging_config import configure_logging

LOGGER = logging.getLogger(__name__)


def _identifier(value: str) -> str:
    if not value or not value.replace("_", "").isalnum() or value[0].isdigit():
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_sql(source_uri: str, target_table: str) -> str:
    if not source_uri.startswith("s3://"):
        raise ValueError("SOURCE_S3_URI must be an s3:// URI")
    table = ".".join(_identifier(part) for part in target_table.split("."))
    return f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT o_orderstatus AS order_status,
               o_orderpriority AS order_priority,
               COUNT(*) AS order_count,
               SUM(o_totalprice) AS total_price,
               AVG(o_totalprice) AS average_price
        FROM read_parquet({_sql_string(source_uri)})
        GROUP BY o_orderstatus, o_orderpriority
    """


def run(settings: Settings, source_uri: str, target_table: str) -> int:
    sql = build_sql(source_uri, target_table)
    table = ".".join(_identifier(part) for part in target_table.split("."))
    execution_id = str(uuid.uuid4())
    lock = (
        EtlLock(settings.lock_table, execution_id, region=settings.aws_region)
        if settings.lock_table
        else nullcontext()
    )
    with lock, connect(settings) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            connection.commit()
            cursor.execute(f"SELECT COUNT(*), COALESCE(SUM(order_count), 0) FROM {table}")
            groups, orders = cursor.fetchone()
        finally:
            cursor.close()
    if int(orders) <= 0:
        raise RuntimeError(f"{table} was created but contains no aggregated orders")
    LOGGER.info(
        "Orders summary table created",
        extra={"execution_id": execution_id, "rows": int(groups), "status": "success"},
    )
    return int(groups)


def main() -> int:
    configure_logging(os.getenv("LOG_LEVEL", "INFO"))
    try:
        source_uri = os.environ["SOURCE_S3_URI"]
        target_table = os.getenv("TARGET_TABLE", "order_stats.main.orders_summary")
        run(Settings.from_env(), source_uri, target_table)
    except Exception:
        LOGGER.exception("Orders summary job failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
