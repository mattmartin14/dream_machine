import duckdb
import os
import time

def create_assets(cn: duckdb.DuckDBPyConnection):

    rows = 20_000_000

    sql = f"""
    create or replace table temp_stg as
    select  
              t.row_id
            , cast(uuid() as varchar(30)) as txn_key
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

    print('staging table created')


    sql = f"""
        create or replace table t_data_sorted 
        (
              row_id BIGINT
            , txn_key VARCHAR(30)
            , rand_dt DATE
            , rand_val DOUBLE
            , rand_str VARCHAR
            , rand_int INTEGER
            , rand_bool BOOLEAN
            , rand_category VARCHAR
            , rand_balance DOUBLE
            , rand_timestamp TIMESTAMP   
        )
    """

    cn.execute(sql)
    print('target table created')

    cn.execute("""
       create or replace table t_logger (
               log_ts timestamp,
               iteration_nbr integer
        )        
    """)
    print('logger table created')

def load_data(cn: duckdb.DuckDBPyConnection, iterations):
    
    start_time = time.time()


    elapsed_time = time.time() - start_time
    print(f'table t_data created in {elapsed_time:.2f} seconds')

    # Starting date
    from datetime import date, timedelta
    current_date = date(2024, 1, 1)

    for i in range(iterations):
        iter_start_time = time.time()

        sql = f"""
            insert into t_data_sorted (rand_dt, row_id, txn_key, rand_val, rand_str, rand_int, rand_bool, rand_category, rand_balance, rand_timestamp)
            select '{current_date}', row_id, txn_key, rand_val, rand_str, rand_int, rand_bool, rand_category, rand_balance, rand_timestamp
            from temp_stg
        """

        cn.execute(sql)
        elapsed_time = time.time() - iter_start_time
        print(f'iteration {i+1} (date: {current_date}) completed in {elapsed_time:.2f} seconds')
        
        cn.execute(f"""
            insert into t_logger (log_ts, iteration_nbr)
            values (now(), {i+1})
        """)

        # Increment date by 1 day
        current_date += timedelta(days=1)
    
    elapsed_time = time.time() - start_time
    print(f'data loaded to table t_data in {elapsed_time:.2f} seconds')

def create_cn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(f'md:db_sorted?motherduck_token={os.getenv("MD_TOKEN")}')

if __name__ == "__main__":

    _ITERATIONS = 30
    # 1,400

    cn = create_cn()
    create_assets(cn)
    load_data(cn, iterations=_ITERATIONS)
    print('data loaded to motherduck')