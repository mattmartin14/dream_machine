import duckdb
import concurrent.futures
import time

data_sql = """
    select t.row_id, uuid() as txn_key, current_date as rpt_dt
        ,round(random() * 100,2) as some_val
    from generate_series(1,{rows}) t(row_id)
"""

def write_data(file_num, rows):
    parquet_sql_template = f"COPY ({data_sql.format(rows=rows)}) TO '~/test_dummy_data/duckdb/data{file_num}.parquet' (FORMAT PARQUET)"
    cn = duckdb.connect()
    cn.sql(parquet_sql_template)
    cn.close()

def main():

    start_time = time.time()

    num_files = 10
    rows = 50000000

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_files) as executor:
        futures = [executor.submit(write_data, i + 1, rows) for i in range(num_files)]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred: {e}")    

    end_time = time.time()
    print(f"Total time to create dataset: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()