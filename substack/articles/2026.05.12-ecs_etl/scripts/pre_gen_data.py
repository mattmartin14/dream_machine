import duckdb
import os

## used to pre-land and generate our raw data that the main etl script will consume;
## this is ran as a one-off locally to simulate having raw data already in s3
def main():

    cn = duckdb.connect()

    raw_bucket_name = 's3-sales-raw-test'

    cn.execute("install aws; load aws;")
    cn.execute("install httpfs; load httpfs;")
    cn.execute("install tpch; load tpch;")
    cn.execute("call dbgen(sf=1)")
    cn.execute("create secret aws_s3 (type s3, provider credential_chain);")
    cn.execute(f"""
        COPY (
            select *
            from orders
            limit 100
        ) TO 's3://{raw_bucket_name}/tpch/orders_raw/orders.parquet'       
    """)

    cn.sql(f"from read_parquet('s3://{raw_bucket_name}/tpch/orders_raw/orders.parquet') select *").show()

if __name__ == "__main__":
    main()