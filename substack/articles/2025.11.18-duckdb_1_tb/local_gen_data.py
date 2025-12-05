import duckdb
import time
import os
import argparse
from concurrent.futures import ProcessPoolExecutor
from functools import partial

def gen_dataset(file_num, rows):

    lower_bound = "2020-01-01"
    upper_bound = "2025-01-01"  
    
    output_path = f'/Volumes/xd1/data/ds_{rows}_rows_file_{file_num}.parquet'

    duckdb.execute(f"""
        COPY (
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
        ) TO '{output_path}' (FORMAT 'parquet')
    """)
    #) TO '{output_path}' (FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE 122880)
    
    print(f"Completed file {file_num}: {output_path}")
    return output_path


def main(num_files, max_workers, rows_per_file):
    
    start_time = time.time()
    
    print(f"Generating {num_files} files with {rows_per_file:,} rows each using {max_workers} workers...")
    
    # Create partial function with rows fixed
    gen_func = partial(gen_dataset, rows=rows_per_file)
    
    # Generate files in parallel
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        output_paths = list(executor.map(gen_func, range(1, num_files + 1)))
    
    elapsed_time = time.time() - start_time
    
    # Calculate total size
    total_size_bytes = sum(os.path.getsize(path) for path in output_paths)
    total_size_gb = total_size_bytes / (1024**3)
    
    # Calculate speed per GB
    speed_per_gb = elapsed_time / total_size_gb if total_size_gb > 0 else 0
    
    print(f"\n{'='*60}")
    print(f"Total files: {num_files}")
    print(f"Total rows: {num_files * rows_per_file:,}")
    print(f"Total time: {elapsed_time:.2f} seconds")
    print(f"Total size: {total_size_gb:.2f} GB")
    print(f"Write speed: {speed_per_gb:.2f} seconds per GB")
    print(f"{'='*60}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate random parquet files in parallel')
    parser.add_argument('--num-files', type=int, default=5, help='Number of files to generate (default: 5)')
    parser.add_argument('--max-workers', type=int, default=10, help='Number of parallel workers (default: 10)')
    parser.add_argument('--rows-per-file', type=int, default=50_000_000, help='Rows per file (default: 50,000,000)')
    
    args = parser.parse_args()
    
    main(
        num_files=args.num_files,
        max_workers=args.max_workers,
        rows_per_file=args.rows_per_file
    )

# uv run gen_data.py --num-files 10
# uv run gen_data.py --num-files 375 -- gets you over 1 tb