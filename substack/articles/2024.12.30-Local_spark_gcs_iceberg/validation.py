
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

    ### part 2: query it via duckdb

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

    cn.sql(sql).show()

if __name__ == "__main__":

    catalog_name = "icyhot"
    namespace = "bike_shop"
    table_name = "orders_agg"
    gcs_bucket = os.getenv("GCS_BUCKET")

    validate_data(gcs_bucket, namespace, table_name)