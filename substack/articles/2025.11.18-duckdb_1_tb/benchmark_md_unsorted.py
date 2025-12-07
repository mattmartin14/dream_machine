import duckdb
import time
import os
import statistics
import random

def benchmark_md_unsorted():
    """Benchmark MotherDuck unsorted table"""
    
    num_runs = 5
    min_rand_val = 0
    max_rand_val = 100
    
    db_name = "db1"
    table_name = "t_data"
    label = "MotherDuck Unsorted"
    
    print("="*70)
    print(f"BENCHMARKING: {label}")
    print("="*70 + "\n")
    
    cn_md = duckdb.connect(f'md:?motherduck_token={os.getenv("MD_TOKEN")}')
    
    print(f"Database: {db_name}, Table: {table_name}\n")
    
    times = []
    cold_start_time = None
    
    # Run 0 = cold start, Runs 1-5 = actual benchmarks
    for i in range(num_runs + 1):
        # Generate random lower and upper limits for each run
        lower_lim = random.uniform(min_rand_val, max_rand_val - 10)
        upper_lim = random.uniform(lower_lim + 5, max_rand_val)
        
        sql = f"""
            SELECT rand_dt
                , sum(case when rand_val between {lower_lim} AND {upper_lim} then rand_val end), count(*)
            FROM {db_name}.main.{table_name}
            GROUP BY 1
            ORDER BY 1 desc
            LIMIT 30
        """
        
        start_time = time.time()
        result = cn_md.execute(sql).fetchall()
        elapsed_time = time.time() - start_time
        
        if i == 0:
            # Cold start - don't include in benchmark stats
            cold_start_time = elapsed_time
            print(f"Run {i} (cold start): {elapsed_time:.2f}s (limits: {lower_lim:.2f} - {upper_lim:.2f})")
        else:
            # Actual benchmark runs
            times.append(elapsed_time)
            print(f"Run {i}/{num_runs}: {elapsed_time:.2f}s (limits: {lower_lim:.2f} - {upper_lim:.2f})")
    
    cn_md.close()
    
    # Print results
    print("\n" + "="*70)
    print("RESULTS")
    print("="*70)
    print(f"Cold Start: {cold_start_time:.2f}s")
    print(f"Average (Runs 1-5): {statistics.mean(times):.2f}s")
    print(f"Min: {min(times):.2f}s")
    print(f"Max: {max(times):.2f}s")
    print(f"Median: {statistics.median(times):.2f}s")
    print("="*70)


if __name__ == "__main__":
    benchmark_md_unsorted()
