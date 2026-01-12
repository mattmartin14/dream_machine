import duckdb
import os


def join_stuff(): 

    md_db_name = "multi_metastore"

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

    glue_db_name = "ns1"

    sql = f"""
        SELECT geo.state
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
        ORDER BY 2 DESC
        LIMIT 15
    """

    cn.sql(sql).show()

if __name__ == "__main__":
    join_stuff()