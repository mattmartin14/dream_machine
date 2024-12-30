
### simple sample/validator
import duckdb
from fsspec import filesystem
import os
import warnings

# Suppress specific Google Cloud SDK warning
warnings.filterwarnings(
    "ignore",
    message="Your application has authenticated using end user credentials.*",
    category=UserWarning,
    module="google.auth._default"
)

def validate_data(gcs_bucket: str, namespace: str, table_name: str) ->None:


    cn = duckdb.connect()
    cn.register_filesystem(filesystem('gcs'))
    cn.execute("""
        INSTALL ICEBERG;
        LOAD ICEBERG;
    """)

    table_path = f"gs://{gcs_bucket}/icehouse/{namespace}/{table_name}"

    sql = f"""
    select *
    from iceberg_scan('{table_path}')
    limit 5
    """

    #sample iceberg data
    cn.sql(sql).show()

    #agg up validation attributes
    sql = f"select count(*) as row_cnt, max(first_promise_dt) as mx_dt from iceberg_scan('{table_path}')"
    row_cnt, mx_dt = cn.execute(sql).fetchone()
    
    #this is from the data pipeline process; it wrote out some columns at the end for downstream validation
    sql_pipeline = f"select row_cnt, mx_dt from read_parquet('./validation/*.parquet')"
    row_cnt_pipe, mx_dt_pipe = cn.execute(sql_pipeline).fetchone()

    #assert them
    assert row_cnt == row_cnt_pipe, f"Row count mismatch: Expected {row_cnt_pipe}, but got {row_cnt}"
    assert mx_dt == mx_dt_pipe, f"Max Date mismatch: Expected {mx_dt_pipe}, but got {mx_dt}"

    print('validation completed')


if __name__ == "__main__":

    catalog_name = "icyhot"
    namespace = "bike_shop"
    table_name = "orders_agg"
    gcs_bucket = os.getenv("GCS_BUCKET")

    validate_data(gcs_bucket, namespace, table_name)