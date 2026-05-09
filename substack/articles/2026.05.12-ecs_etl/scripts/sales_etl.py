import logging
import os

from helpers.common import establish_duckdb_connection, send_slack_notification

def main():

    try:

        cn = establish_duckdb_connection(
            aws_region="us-east-1",
            extension_directory="/opt/duckdb/extensions",
        )

        logging.info("established duckdb connection with aws/httpfs extensions loaded")

        raw_bucket_name = 's3-sales-raw-test'
        agg_bucket_name = 's3-sales-agg-test'

        s3_raw_path = f"s3://{raw_bucket_name}/tpch/orders_raw/orders.parquet"
        s3_target_path = f"s3://{agg_bucket_name}/tpch/cust_agg/cust_agg.parquet"
        
        ### agg and transform data and write out to our destination s3 path
        cn.execute(
            f"""
            COPY (
                select o_custkey, count(*) as order_count, sum(o_totalprice) as total_price
                from read_parquet('{s3_raw_path}')
                group by o_custkey
            )
                TO '{s3_target_path}'
            """
        )

        logging.info("copied aggregated data to target s3 path %s", s3_target_path)

        #cn.sql(f"from read_parquet('{s3_target_path}') limit 5").show()

        send_slack_notification("info", "Sales Customer Order Aggregate ETL process completed successfully.")
        logging.info("Sales ETL process completed successfully.")
    except Exception as exc:
        logging.exception("Sales ETL process failed: %s", str(exc))
        send_slack_notification("error", f"Sales Customer Order Aggregate ETL process failed: {str(exc)}")
        return 1

if __name__ == "__main__":
    main()