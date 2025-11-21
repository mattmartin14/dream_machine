import duckdb
import os
import time

def create_data_gen_view(cn: duckdb.DuckDBPyConnection):

    lower_bound = "2020-01-01"
    upper_bound = "2025-01-01" 
    rows = 50_000_000

    sql = f"""
    create or replace view v_data as
    select  
              t.row_id
            , cast(uuid() as varchar(30)) as txn_key
            , date '{lower_bound}' 
            + (random() * (date_diff('day', date '{lower_bound}', date '{upper_bound}')))::int as rand_dt
            , round(random() * 100, 2) as rand_val
            , substr(
                  'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', 
                  1, 
                  1 + (random() * 25)::int
              ) as rand_str
            , (random() * 1000000)::int as rand_int
            , random() < 0.5 as rand_bool
            , ['Option A', 'Option B', 'Option C', 'Option D'][1 + (random() * 3)::int] as rand_category
            , round(random() * 10000 - 5000, 2) as rand_balance
            , cast(now() - interval (random() * 365) day as timestamp) as rand_timestamp
        from generate_series(1,{rows}) t(row_id)

    """

    cn.execute(sql)

    print('view created')


def load_data(cn: duckdb.DuckDBPyConnection):
    
    start_time = time.time()

    sql = f"""
        create or replace table t_data as
        select * from v_data
    """

    cn.execute(sql)

    elapsed_time = time.time() - start_time
    print(f'table t_data created in {elapsed_time:.2f} seconds')

    total_iterations = 10

    for i in range(total_iterations):
        iter_start_time = time.time()
        cn.execute("insert into t_data select * from t_data")
        elapsed_time = time.time() - iter_start_time
        print(f'iteration {i+1} completed in {elapsed_time:.2f} seconds')
    
    elapsed_time = time.time() - start_time
    print(f'data loaded to table t_data in {elapsed_time:.2f} seconds')

def create_cn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(f'md:db1?motherduck_token={os.getenv("MD_TOKEN")}')

if __name__ == "__main__":
    cn = create_cn()
    create_data_gen_view(cn)
    load_data(cn)
    print('data loaded to motherduck')