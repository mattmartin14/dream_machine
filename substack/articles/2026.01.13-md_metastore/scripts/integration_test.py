import duckdb
import os

def create_multi_metastore_conn(md_db_name)-> duckdb.DuckDBPyConnection:
    
    cn = duckdb.connect(f'md:{md_db_name}?motherduck_token={os.getenv("MD_TOKEN")}')

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

    return cn

def integration_test_1(cn: duckdb.DuckDBPyConnection, md_db_name: str, glue_db_name: str) -> None: 

    sql = f"""
        CREATE OR REPLACE TABLE {md_db_name}.main.state_agg_stats
        AS
        SELECT geo.state
            ,current_timestamp as load_ts
            ,count(distinct oh.order_id) as ord_cnt
            ,count(distinct od.product_id) as product_cnt
            ,sum(od.quantity) as quantity_cnt
            ,sum(od.line_total) as total_retail
        FROM s3cat.{glue_db_name}.order_header as oh
            INNER JOIN s3cat.{glue_db_name}.order_detail as od
                on oh.order_id = od.order_id
            LEFT JOIN {md_db_name}.main.store_geo as geo
                on oh.store_id = geo.store_id
        GROUP BY ALL 
    """

    cn.execute(sql)
    print("state_agg_stats table rebuilt")


def integration_test_2(cn: duckdb.DuckDBPyConnection, md_db_name: str, glue_db_name: str) -> None:    


    sql = f"""
        CREATE OR REPLACE TABLE {md_db_name}.main.zip_agg_stats
        AS
        SELECT geo.zip as zip_cd
            ,current_timestamp as load_ts
            ,count(distinct oh.order_id) as ord_cnt
            ,count(distinct od.product_id) as product_cnt
            ,sum(od.quantity) as quantity_cnt
            ,sum(od.line_total) as total_retail
        FROM s3cat.{glue_db_name}.order_header as oh
            INNER JOIN s3cat.{glue_db_name}.order_detail as od
                on oh.order_id = od.order_id
            LEFT JOIN {md_db_name}.main.store_geo as geo
                on oh.store_id = geo.store_id
        GROUP BY ALL 
    """

    cn.execute(sql)

    print("zip_agg_stats table rebuilt")

def run_all():
    md_db_name = "multi_metastore"
    glue_db_name = "ns1"

    cn = create_multi_metastore_conn(md_db_name)
    integration_test_1(cn, md_db_name, glue_db_name)
    integration_test_2(cn, md_db_name, glue_db_name)

if __name__ == "__main__":
    run_all()
    