import duckdb
import os

def create_duckdb_cn() -> duckdb.DuckDBPyConnection:
    cn = duckdb.connect()
    cn.execute("install aws; load aws;")
    cn.execute("install iceberg; load iceberg")

    cn.execute("""
        CREATE SECRET s3_creds (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN,
            REGION 'us-east-1'
        )
    """)

    aws_acct_id = os.getenv("AWS_ACCT_ID")

    cn.execute(f"""
    attach or replace 'arn:aws:s3tables:us-east-1:{aws_acct_id}:bucket/icehouse-tbl-bucket1' as s3cat (
        type iceberg,
        endpoint_type s3_tables
        )
    """)


    num_rows = 50

    cn.execute(f"""
        CREATE OR REPLACE VIEW v_data_gen AS
        SELECT 
            t.row_id, 
            uuid()::varchar as txn_key,  -- Cast to varchar to avoid binary UUID issues
            current_date as rpt_dt,
            round(random() * 100, 2) as some_val
        FROM generate_series(1, {num_rows}) t(row_id)
    """)

    return cn


def create_s3_iceberg_table(cn, glue_db_name, table_name):
    sql = f"""
    CREATE TABLE IF NOT EXISTS s3cat.{glue_db_name}.{table_name} (
        row_id INTEGER,
        txn_key VARCHAR,
        rpt_dt DATE,
        some_val DOUBLE
    )
    """
    cn.execute(sql)


def write_iceberg_s3_tables_using_the_duck(cn, glue_db_name, table_name):
    sql = f"""
    INSERT INTO s3cat.{glue_db_name}.{table_name}
    SELECT * FROM v_data_gen
    limit 3
    """
    cn.execute(sql)


if __name__ == "__main__":
    cn = create_duckdb_cn()
    glue_db_name = "ns1"
    table_name = "iceyhot_s3"
    create_s3_iceberg_table(cn, glue_db_name, table_name)
    write_iceberg_s3_tables_using_the_duck(cn, glue_db_name, table_name)