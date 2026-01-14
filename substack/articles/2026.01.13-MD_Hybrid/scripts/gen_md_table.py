import duckdb
import os
import time

def load_md_table():

    t0 = time.perf_counter()

    db_name = "duckland"

    cn = duckdb.connect(f'md:{db_name}?motherduck_token={os.getenv("MD_TOKEN")}')

    cn.execute(f"""
        CREATE OR REPLACE TABLE {db_name}.main.inventory
        AS
        SELECT *
        FROM read_csv_auto('~/test_data/inventory*.csv')
    """)

    elapsed = time.perf_counter() - t0
    print(f"time to generate MotherDuck inventory table: {elapsed:.3f}s")

if __name__ == "__main__": 
    load_md_table()