import logging
import sys

from helpers.common import copy_local_file_to_s3, establish_duckdb_connection, send_slack_notification


def main() -> int:
    try:
        cn = establish_duckdb_connection(
            aws_region="us-east-1",
            extension_directory="/opt/duckdb/extensions",
        )

        logging.info("established duckdb connection with aws/httpfs extensions loaded")

        raw_bucket_name = "s3-sales-raw-test"
        agg_bucket_name = "s3-sales-agg-test"

        s3_raw_path = f"s3://{raw_bucket_name}/tpch/lineitem_raw/lineitem.parquet"
        s3_target_path = f"s3://{agg_bucket_name}/tpch/lineitem_agg/lineitem_agg.parquet"

        cn.execute(
            f"""
            COPY (
                SELECT
                    l_returnflag,
                    l_linestatus,
                    COUNT(*) AS line_count,
                    SUM(l_quantity) AS total_quantity,
                    SUM(l_extendedprice) AS total_extended_price,
                    SUM(l_discount) AS total_discount
                FROM read_parquet('{s3_raw_path}')
                GROUP BY l_returnflag, l_linestatus
            )
            TO 'lineitem.parquet'
            """
        )

        result = cn.execute("SELECT COUNT(*) AS record_count FROM read_parquet('lineitem.parquet')").fetchone()
        record_count = result[0] if result else 0
        if record_count == 0:
            send_slack_notification(
                "error",
                "Validation check failed: no records found in local file lineitem.parquet",
                title="Lineitem ETL Validation Failed",
                fields={
                    "Stage": "QA Gate",
                    "Check": "record_count > 0",
                    "Record Count": record_count,
                    "Local File": "lineitem.parquet",
                },
            )
            raise ValueError("Validation check failed: no records found in local file lineitem.parquet")
        logging.info("validation check - record count in local file: %d", record_count)

        result = cn.execute(
            """
            SELECT COUNT(*) AS null_group_key_count
            FROM read_parquet('lineitem.parquet')
            WHERE l_returnflag IS NULL OR l_linestatus IS NULL
            """
        ).fetchone()
        null_group_key_count = result[0] if result else 0
        if null_group_key_count > 0:
            send_slack_notification(
                "error",
                f"Validation check failed: found {null_group_key_count} records with null group keys in local file lineitem.parquet",
                title="Lineitem ETL Validation Failed",
                fields={
                    "Stage": "QA Gate",
                    "Check": "l_returnflag/l_linestatus IS NOT NULL",
                    "Null Group Keys": null_group_key_count,
                    "Local File": "lineitem.parquet",
                },
            )
            raise ValueError(
                f"Validation check failed: found {null_group_key_count} records with null group keys in local file lineitem.parquet"
            )
        logging.info("validation check - null group key count in local file: %d", null_group_key_count)

        result = cn.execute(
            "SELECT COUNT(*) AS non_positive_quantity_count FROM read_parquet('lineitem.parquet') WHERE total_quantity <= 0"
        ).fetchone()
        non_positive_quantity_count = result[0] if result else 0
        if non_positive_quantity_count > 0:
            send_slack_notification(
                "error",
                f"Validation check failed: found {non_positive_quantity_count} records with total_quantity <= 0 in local file lineitem.parquet",
                title="Lineitem ETL Validation Failed",
                fields={
                    "Stage": "QA Gate",
                    "Check": "total_quantity > 0",
                    "Non-positive Quantity": non_positive_quantity_count,
                    "Local File": "lineitem.parquet",
                },
            )
            raise ValueError(
                f"Validation check failed: found {non_positive_quantity_count} records with total_quantity <= 0 in local file lineitem.parquet"
            )
        logging.info("validation check - total_quantity <= 0 count in local file: %d", non_positive_quantity_count)

        copy_local_file_to_s3("lineitem.parquet", s3_target_path)

        logging.info("copied aggregated data to target s3 path %s", s3_target_path)

        send_slack_notification(
            "success",
            "Lineitem ETL process completed successfully.",
            title="Lineitem ETL Completed",
            fields={
                "Stage": "Publish",
                "Output": s3_target_path,
                "Record Count": record_count,
                "Null Group Keys": null_group_key_count,
                "Non-positive Quantity": non_positive_quantity_count,
            },
        )

        logging.info("Lineitem ETL process completed successfully.")
        return 0
    except Exception as exc:
        logging.exception("Lineitem ETL process failed: %s", str(exc))
        send_slack_notification(
            "error",
            f"Lineitem ETL process failed: {str(exc)}",
            title="Lineitem ETL Failed",
            fields={
                "Stage": "Execution",
                "Error Type": type(exc).__name__,
            },
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
