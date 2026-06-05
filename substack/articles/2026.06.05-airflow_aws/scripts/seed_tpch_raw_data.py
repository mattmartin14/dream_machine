import logging
import os
import sys

from helpers.common import copy_local_file_to_s3, establish_duckdb_connection, send_slack_notification


def env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value.strip()


def main() -> int:
    aws_region = env("AWS_REGION", "us-east-1")
    extension_directory = env("DUCKDB_EXTENSION_DIR", "/opt/duckdb/extensions")
    raw_bucket_name = env("RAW_BUCKET_NAME", "s3-sales-raw-test")
    scale_factor = env("SEED_TPCH_SCALE_FACTOR", "0.01")

    orders_local_path = "orders.parquet"
    customer_local_path = "customer.parquet"
    lineitem_local_path = "lineitem.parquet"

    orders_s3_path = f"s3://{raw_bucket_name}/tpch/orders_raw/orders.parquet"
    customer_s3_path = f"s3://{raw_bucket_name}/tpch/customer_raw/customer.parquet"
    lineitem_s3_path = f"s3://{raw_bucket_name}/tpch/lineitem_raw/lineitem.parquet"

    try:
        cn = establish_duckdb_connection(
            aws_region=aws_region,
            extension_directory=extension_directory,
        )

        cn.execute("INSTALL tpch;")
        cn.execute("LOAD tpch;")
        cn.execute(f"CALL dbgen(sf={scale_factor});")

        cn.execute(f"COPY orders TO '{orders_local_path}' (FORMAT PARQUET)")
        cn.execute(f"COPY customer TO '{customer_local_path}' (FORMAT PARQUET)")
        cn.execute(f"COPY lineitem TO '{lineitem_local_path}' (FORMAT PARQUET)")

        orders_row_count = cn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        customer_row_count = cn.execute("SELECT COUNT(*) FROM customer").fetchone()[0]
        lineitem_row_count = cn.execute("SELECT COUNT(*) FROM lineitem").fetchone()[0]

        copy_local_file_to_s3(orders_local_path, orders_s3_path)
        copy_local_file_to_s3(customer_local_path, customer_s3_path)
        copy_local_file_to_s3(lineitem_local_path, lineitem_s3_path)

        send_slack_notification(
            "success",
            "TPCH raw data seed completed.",
            title="TPCH Seed Completed",
            fields={
                "Scale Factor": scale_factor,
                "Orders Output": orders_s3_path,
                "Customer Output": customer_s3_path,
                "Lineitem Output": lineitem_s3_path,
                "Orders Rows": orders_row_count,
                "Customer Rows": customer_row_count,
                "Lineitem Rows": lineitem_row_count,
            },
        )

        logging.info(
            "tpch_seed_complete scale_factor=%s orders_rows=%s customer_rows=%s lineitem_rows=%s",
            scale_factor,
            orders_row_count,
            customer_row_count,
            lineitem_row_count,
        )
        return 0
    except Exception as exc:
        logging.exception("tpch_seed_failed %s", str(exc))
        send_slack_notification(
            "error",
            f"TPCH raw data seed failed: {str(exc)}",
            title="TPCH Seed Failed",
            fields={
                "Scale Factor": scale_factor,
                "Raw Bucket": raw_bucket_name,
                "Error Type": type(exc).__name__,
            },
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
