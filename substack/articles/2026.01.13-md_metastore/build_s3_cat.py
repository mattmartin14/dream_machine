import duckdb
import os


def build_s3_tables() -> None:
    cn = duckdb.connect()

    aws_acct_id = os.getenv("AWS_ACCT_ID")

    cn.execute("install aws; load aws;")
    cn.execute("install iceberg; load iceberg")

    cn.execute("""
        CREATE SECRET s3_creds (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN,
            REGION 'us-east-1'
        )
    """)

    cn.execute(f"""
    attach or replace 'arn:aws:s3tables:us-east-1:{aws_acct_id}:bucket/icehouse-tbl-bucket1' as s3cat (
        type iceberg,
        endpoint_type s3_tables,
        secret s3_creds
        )
    """)

    glue_db_name = "ns1"

    cn.execute(f"DROP TABLE IF EXISTS s3cat.{glue_db_name}.order_header")
    cn.execute(f"DROP TABLE IF EXISTS s3cat.{glue_db_name}.order_detail")
    
    cn.execute(f"""
        CREATE TABLE IF NOT EXISTS s3cat.{glue_db_name}.order_header (
            order_id BIGINT,
            order_date TIMESTAMP,
            store_id INTEGER,
            customer_id INTEGER,
            status STRING,
            payment_method STRING,
            subtotal DECIMAL(18,2),
            tax DOUBLE,
            total DOUBLE,
            line_count BIGINT
        )   
    """)


    cn.execute(f"""
        INSERT INTO s3cat.{glue_db_name}.order_header
        SELECT *
        FROM read_parquet('data/order_headers.parquet')
    """)

    cn.execute(f"""
       CREATE TABLE IF NOT EXISTS s3cat.{glue_db_name}.order_detail (
        order_id BIGINT,
        product_id INTEGER,
        product_name STRING,
        category STRING,
        unit_price DECIMAL(4,2),
        quantity INTEGER,
        line_total DECIMAL(14,2)
       )        
    """)

    cn.execute(f"""
       INSERT INTO s3cat.{glue_db_name}.order_detail
       SELECT * 
       FROM read_parquet('data/order_details.parquet')       
    """)

if __name__ == "__main__":
    build_s3_tables()