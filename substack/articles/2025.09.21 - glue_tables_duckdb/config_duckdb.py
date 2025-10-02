import duckdb
import os

def config_duckdb() -> duckdb.DuckDBPyConnection:
    """Configure and return a DuckDB connection with necessary extensions."""
    cn = duckdb.connect()
    
    # Load necessary extensions
    cn.execute("install iceberg; load iceberg")
    cn.execute("install aws; load aws")

    cn.execute("""
        CREATE SECRET s3_creds (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN,
            REGION 'us-east-1'
        )
    """)

    #attach iceberg catalog
    cn.execute(f"""
    attach '{os.environ("AWS_ACCT_ID")}' as iceberg_catalog (
        type iceberg,
        endpoint_type glue
        )
    """)
    
    return cn