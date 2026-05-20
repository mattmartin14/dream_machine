import logging
from helpers.common import establish_duckdb_connection, send_slack_notification, copy_local_file_to_s3

def main():

    try:

        cn = establish_duckdb_connection(
            aws_region="us-east-1",
            extension_directory="/opt/duckdb/extensions",
        )

        logging.info("established duckdb connection with aws/httpfs extensions loaded")

        raw_bucket_name = 's3-sales-raw-test'
        agg_bucket_name = 's3-sales-agg-test'

        s3_raw_path = f"s3://{raw_bucket_name}/tpch/customer_raw/customer.parquet"
        s3_target_path = f"s3://{agg_bucket_name}/tpch/cust_agg/cust_agg.parquet"

        ### aggregate to local file
        cn.execute(
            f"""
            COPY (
                select c_mktsegment, sum(c_acctbal) as acct_bal, count(distinct c_phone) as phone_cnt 
                from read_parquet('{s3_raw_path}')
                group by all
            )
                TO 'cust.parquet'
            """
        )

        ## << QA CHECKS >> 

        ## check row count > 0
        result = cn.execute(f"SELECT count(*) AS record_count FROM read_parquet('cust.parquet')").fetchone()
        record_count = result[0] if result else 0
        if record_count == 0:
            send_slack_notification(
                "error",
                "Validation check failed: no records found in local file cust.parquet",
                title="Cust ETL Validation Failed",
                fields={
                    "Stage": "QA Gate",
                    "Check": "record_count > 0",
                    "Record Count": record_count,
                    "Local File": "cust.parquet",
                },
            )
            raise ValueError("Validation check failed: no records found in local file cust.parquet")
        logging.info("validation check - record count in local file: %d", record_count)

        ## check for nulls on cust key
        result = cn.execute(f"SELECT count(*) AS null_custkey_count FROM read_parquet('cust.parquet') WHERE c_mktsegment IS NULL").fetchone()
        null_custkey_count = result[0] if result else 0
        if null_custkey_count > 0:
            send_slack_notification(
                "error",
                f"Validation check failed: found {null_custkey_count} records with null c_mktsegment in local file cust.parquet",
                title="Cust ETL Validation Failed",
                fields={
                    "Stage": "QA Gate",
                    "Check": "c_mktsegment IS NOT NULL",
                    "Null c_mktsegment": null_custkey_count,
                    "Local File": "cust.parquet",
                },
            )
            raise ValueError(f"Validation check failed: found {null_custkey_count} records with null c_mktsegment in local file cust.parquet")
        logging.info("validation check - null c_mktsegment count in local file: %d", null_custkey_count)

        ## check for acct_bal < 100
        result = cn.execute(f"SELECT count(*) AS low_acct_bal_count FROM read_parquet('cust.parquet') WHERE acct_bal < 100").fetchone()
        low_acct_bal_count = result[0] if result else 0
        if low_acct_bal_count > 0:
            send_slack_notification(
                "error",
                f"Validation check failed: found {low_acct_bal_count} records with acct_bal < 100 in local file cust.parquet",
                title="Cust ETL Validation Failed",
                fields={
                    "Stage": "QA Gate",
                    "Check": "acct_bal >= 100",
                    "Low acct_bal Count": low_acct_bal_count,
                    "Local File": "cust.parquet",
                },
            )
            raise ValueError(f"Validation check failed: found {low_acct_bal_count} records with acct_bal < 100 in local file cust.parquet")
        logging.info("validation check - acct_bal < 100 count in local file: %d", low_acct_bal_count)

        ## All QA Checks Passed - sending to s3 destination

        ## copy to s3 target path
        copy_local_file_to_s3('cust.parquet', s3_target_path)
        
        logging.info("copied aggregated data to target s3 path %s", s3_target_path)

        send_slack_notification(
            "success",
            "Cust ETL process completed successfully.",
            title="Cust ETL Completed",
            fields={
                "Stage": "Publish",
                "Output": s3_target_path,
                "Record Count": record_count,
                "Null c_mktsegment": null_custkey_count,
                "Low acct_bal Count": low_acct_bal_count,
            },
        )

        logging.info("Cust ETL process completed successfully.")
    except Exception as exc:
        logging.exception("Cust ETL process failed: %s", str(exc))
        send_slack_notification(
            "error",
            f"Cust ETL process failed: {str(exc)}",
            title="Cust ETL Failed",
            fields={
                "Stage": "Execution",
                "Error Type": type(exc).__name__,
            },
        )
        return 1

if __name__ == "__main__":
    main()