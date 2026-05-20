import logging
from helpers.common import establish_duckdb_connection, send_slack_notification, copy_local_file_to_s3

def main():

    try:

        cn = establish_duckdb_connection(
            aws_region="us-east-1",
            extension_directory="/opt/duckdb/extensions",
        )

        #cn.execute("select cast('abc' as int)") # test exception handling and notification

        logging.info("established duckdb connection with aws/httpfs extensions loaded")

        raw_bucket_name = 's3-sales-raw-test'
        agg_bucket_name = 's3-sales-agg-test'

        s3_raw_path = f"s3://{raw_bucket_name}/tpch/orders_raw/orders.parquet"
        s3_target_path = f"s3://{agg_bucket_name}/tpch/orders_agg/orders_agg.parquet"
        
        ### aggregate to local file
        cn.execute(
            f"""
            COPY (
                select o_custkey, count(*) as order_count, sum(o_totalprice) as total_price
                from read_parquet('{s3_raw_path}')
                group by o_custkey
            )
                TO 'data.parquet'
            """
        )

        ## << QA CHECKS >> 

        ## check row count > 0
        result = cn.execute(f"SELECT count(*) AS record_count FROM read_parquet('data.parquet')").fetchone()
        record_count = result[0] if result else 0
        if record_count == 0:
            send_slack_notification(
                "error",
                "Validation check failed: no records found in local file data.parquet",
                title="Sales Order ETL Validation Failed",
                fields={
                    "Stage": "QA Gate",
                    "Check": "record_count > 0",
                    "Record Count": record_count,
                    "Local File": "data.parquet",
                },
            )
            raise ValueError("Validation check failed: no records found in local file data.parquet")
        logging.info("validation check - record count in local file: %d", record_count)

        ## check for nulls on cust key
        result = cn.execute(f"SELECT count(*) AS null_custkey_count FROM read_parquet('data.parquet') WHERE o_custkey IS NULL").fetchone()
        null_custkey_count = result[0] if result else 0
        if null_custkey_count > 0:
            send_slack_notification(
                "error",
                f"Validation check failed: found {null_custkey_count} records with null o_custkey in local file data.parquet",
                title="Sales Order ETL Validation Failed",
                fields={
                    "Stage": "QA Gate",
                    "Check": "o_custkey IS NOT NULL",
                    "Null o_custkey": null_custkey_count,
                    "Local File": "data.parquet",
                },
            )
            raise ValueError(f"Validation check failed: found {null_custkey_count} records with null o_custkey in local file data.parquet")
        logging.info("validation check - null o_custkey count in local file: %d", null_custkey_count)

        ## check for total price < 100
        result = cn.execute(f"SELECT count(*) AS low_total_price_count FROM read_parquet('data.parquet') WHERE total_price < 100").fetchone()
        low_total_price_count = result[0] if result else 0
        if low_total_price_count > 0:
            send_slack_notification(
                "error",
                f"Validation check failed: found {low_total_price_count} records with total_price < 100 in local file data.parquet",
                title="Sales Order ETL Validation Failed",
                fields={
                    "Stage": "QA Gate",
                    "Check": "total_price >= 100",
                    "Low total_price Count": low_total_price_count,
                    "Local File": "data.parquet",
                },
            )
            raise ValueError(f"Validation check failed: found {low_total_price_count} records with total_price < 100 in local file data.parquet")
        logging.info("validation check - total_price < 100 count in local file: %d", low_total_price_count)

        ## All QA Checks Passed - sending to s3 destination

        ## copy to s3 target path
        copy_local_file_to_s3('data.parquet', s3_target_path)
        
        logging.info("copied aggregated data to target s3 path %s", s3_target_path)

        send_slack_notification(
            "success",
            "Sales Order Aggregate ETL process completed successfully.",
            title="Sales Order ETL Completed",
            fields={
                "Stage": "Publish",
                "Output": s3_target_path,
                "Record Count": record_count,
                "Null o_custkey": null_custkey_count,
                "Low total_price Count": low_total_price_count,
            },
        )

        logging.info("Sales ETL process completed successfully.")
    except Exception as exc:
        logging.exception("Sales Order ETL process failed: %s", str(exc))
        send_slack_notification(
            "error",
            f"Sales Order Aggregate ETL process failed: {str(exc)}",
            title="Sales Order ETL Failed",
            fields={
                "Stage": "Execution",
                "Error Type": type(exc).__name__,
            },
        )
        return 1

if __name__ == "__main__":
    main()