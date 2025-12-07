import duckdb
import time
import os
import statistics
import random

def benchmark_queries():
    """Run benchmarks across MotherDuck and local datasets"""
    
    # Configuration
    # md_dbs_and_tables = [
    #     {"db_name": "db1", "table_name": "t_data", "label": "MotherDuck Unsorted"},
    #     {"db_name": "db_sorted", "table_name": "t_data_sorted", "label": "MotherDuck Sorted"}
    # ]

    md_dbs_and_tables = [
        {"db_name": "db_sorted", "table_name": "t_data_sorted", "label": "MotherDuck Sorted"},
        {"db_name": "db1", "table_name": "t_data", "label": "MotherDuck Unsorted"}
        
    ]
    
    local_config = {
        "file_path": "/Volumes/xd1/data/*.parquet",
        "label": "Local Parquet"
    }
    
    num_runs = 5
    results = []
    cold_start_times = []
    
    # Benchmark MotherDuck queries
    print("="*70)
    print("RUNNING MOTHERDUCK BENCHMARKS")
    print("="*70 + "\n")
    
    cn_md = duckdb.connect(f'md:?motherduck_token={os.getenv("MD_TOKEN")}')
    
    min_rand_val = 0
    max_rand_val = 100

    for config in md_dbs_and_tables:
        print(f"Testing: {config['label']}")
        print(f"  Database: {config['db_name']}, Table: {config['table_name']}")
        
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
                FROM {config['db_name']}.main.{config['table_name']}
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
                print(f"  Run {i} (cold start): {elapsed_time:.2f}s (limits: {lower_lim:.2f} - {upper_lim:.2f})")
            else:
                # Actual benchmark runs
                times.append(elapsed_time)
                print(f"  Run {i}/{num_runs}: {elapsed_time:.2f}s (limits: {lower_lim:.2f} - {upper_lim:.2f})")
        
        cold_start_times.append({
            "label": config['label'],
            "time": cold_start_time
        })
        
        results.append({
            "label": config['label'],
            "times": times,
            "avg": statistics.mean(times),
            "min": min(times),
            "max": max(times),
            "median": statistics.median(times)
        })
        print()
    
    cn_md.close()
    
    # Benchmark local query
    print("="*70)
    print("RUNNING LOCAL BENCHMARK")
    print("="*70 + "\n")
    
    print(f"Testing: {local_config['label']}")
    print(f"  File Path: {local_config['file_path']}")
    
    cn_local = duckdb.connect(':memory:')
    
    sql = f"""
        select rand_dt, sum(rand_val), count(*)
        from read_parquet('{local_config['file_path']}')
        group by 1
        order by 1 desc
        limit 30
    """
    
    times = []
    for i in range(num_runs):
        start_time = time.time()
        result = cn_local.execute(sql).fetchall()
        elapsed_time = time.time() - start_time
        times.append(elapsed_time)
        print(f"  Run {i+1}/{num_runs}: {elapsed_time:.2f}s")
    
    results.append({
        "label": local_config['label'],
        "times": times,
        "avg": statistics.mean(times),
        "min": min(times),
        "max": max(times),
        "median": statistics.median(times)
    })
    
    cn_local.close()
    
    # Print cold start times
    print("\n" + "="*70)
    print("COLD START TIMES")
    print("="*70)
    print(f"{'Query':<30} {'Cold Start (s)':>15}")
    print("-"*70)
    
    for cold_start in cold_start_times:
        print(f"{cold_start['label']:<30} {cold_start['time']:>15.2f}")
    
    print("="*70)
    
    # Print summary
    print("\n" + "="*70)
    print("BENCHMARK RESULTS SUMMARY (Runs 1-5)")
    print("="*70)
    print(f"{'Query':<25} {'Avg (s)':>10} {'Min (s)':>10} {'Max (s)':>10} {'Median (s)':>10}")
    print("-"*70)
    
    for result in results:
        print(f"{result['label']:<25} {result['avg']:>10.2f} {result['min']:>10.2f} {result['max']:>10.2f} {result['median']:>10.2f}")
    
    print("="*70)
    
    # Find fastest
    fastest = min(results, key=lambda x: x['avg'])
    print(f"\nFastest: {fastest['label']} with average of {fastest['avg']:.2f} seconds")


if __name__ == "__main__":
    benchmark_queries()