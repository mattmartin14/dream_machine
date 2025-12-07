import duckdb
import time
import statistics

def benchmark_local():
    """Benchmark local parquet files"""
    
    num_runs = 5
    file_path = "/Volumes/xd1/data/*.parquet"
    label = "Local Parquet"
    
    print("="*70)
    print(f"BENCHMARKING: {label}")
    print("="*70 + "\n")
    
    print(f"File Path: {file_path}\n")
    
    cn_local = duckdb.connect(':memory:')
    
    sql = f"""
        select rand_dt, sum(rand_val), count(*)
        from read_parquet('{file_path}')
        group by 1
        order by 1 desc
        limit 30
    """
    
    times = []
    for i in range(1, num_runs + 1):
        start_time = time.time()
        result = cn_local.execute(sql).fetchall()
        elapsed_time = time.time() - start_time
        times.append(elapsed_time)
        print(f"Run {i}/{num_runs}: {elapsed_time:.2f}s")
    
    cn_local.close()
    
    # Print results
    print("\n" + "="*70)
    print("RESULTS")
    print("="*70)
    print(f"Average: {statistics.mean(times):.2f}s")
    print(f"Min: {min(times):.2f}s")
    print(f"Max: {max(times):.2f}s")
    print(f"Median: {statistics.median(times):.2f}s")
    print("="*70)


if __name__ == "__main__":
    benchmark_local()
