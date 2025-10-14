import duckdb
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
        CREATE OR REPLACE VIEW local_flock.v_all_data AS
        SELECT * FROM read_parquet('~/test_dummy_data/duckdb/data[1-2].parquet')
    """)

    cn.execute("create or replace table local_flock.t_all_data as select * from v_all_data")

    cn.close()

if __name__ == "__main__":
    main()