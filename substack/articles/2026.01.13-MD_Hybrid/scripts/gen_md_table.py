import duckdb
import os

def load_md_table():

    db_name = "duckland"

    cn = duckdb.connect(f'md:{db_name}?motherduck_token={os.getenv("MD_TOKEN")}')

    cn.execute(f"""
        CREATE OR REPLACE TABLE {db_name}.main.inventory
        AS
        SELECT *
        FROM read_csv_auto('~/test_data/inventory*.csv')
    """)

if __name__ == "__main__": 
    load_md_table()