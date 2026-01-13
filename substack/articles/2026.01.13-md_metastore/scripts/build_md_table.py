import duckdb
import os

def load_md_table():

    db_name = "multi_metastore"

    cn = duckdb.connect(f'md:{db_name}?motherduck_token={os.getenv("MD_TOKEN")}')

    cn.execute(f"""
        CREATE OR REPLACE TABLE {db_name}.main.store_geo
        AS
        SELECT *
        FROM read_parquet('../data/store_geography.parquet')
    """)

if __name__ == "__main__": 
    load_md_table()