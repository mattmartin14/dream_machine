import duckdb
import time
import os

def main():

    cn = duckdb.connect()
    cn.execute("install ducklake;")

    cn.execute(f"ATTACH 'ducklake:local_flock.ducklake' as local_flock")
    cn.execute("USE local_flock;")

    cn.execute("SET memory_limit = '8GB';")   
    cn.execute(f"SET temp_directory = '{os.path.expanduser('~/duckdb_tmp')}';")  
    cn.execute("SET preserve_insertion_order = false;") 


    cn.execute("""
        CREATE OR REPLACE VIEW local_flock.all_data AS
        SELECT * FROM read_parquet('~/test_dummy_data/duckdb/data[1-3].parquet')
    """)
    
    start_time = time.time()

    cn.sql("""
        SELECT rpt_dt, 
               count(distinct txn_key) AS unique_txn_keys,
               count(*) AS total_rows,
               sum(sales_amt) AS total_sales_amt,
               avg(sales_amt) AS avg_sales_amt
        FROM local_flock.all_data
        GROUP BY rpt_dt
    """).show()

    end_time = time.time()
    print(f"Total time to compute dataset via ducklake: {end_time - start_time:.2f} seconds")
    cn.close()

if __name__ == "__main__":
    main()