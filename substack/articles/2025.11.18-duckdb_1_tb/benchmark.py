import duckdb
import time
import os
import statistics

def benchmark_queries():
    """Run benchmarks across MotherDuck and local datasets"""
    
    # Configuration
    md_dbs_and_tables = [
        {"db_name": "db1", "table_name": "t_data", "label": "MotherDuck Unsorted"},
        {"db_name": "db_sorted", "table_name": "t_data_sorted", "label": "MotherDuck Sorted"}
    ]
    
    local_config = {
        "file_path": "/Volumes/xd1/data/*.parquet",
        "label": "Local Parquet"
    }
    
    num_runs = 5
    results = []
    
    # Benchmark MotherDuck queries
    print("="*70)
    print("RUNNING MOTHERDUCK BENCHMARKS")
    print("="*70 + "\n")
    
    cn_md = duckdb.connect(f'md:?motherduck_token={os.getenv("MD_TOKEN")}')
    
    for config in md_dbs_and_tables:
        print(f"Testing: {config['label']}")
        print(f"  Database: {config['db_name']}, Table: {config['table_name']}")
        
        sql = f"""
            select rand_dt, sum(rand_val), count(*)
            from {config['db_name']}.main.{config['table_name']}
            group by 1
            order by 1 desc
            limit 30
        """
        
        times = []
        for i in range(num_runs):
            start_time = time.time()
            result = cn_md.execute(sql).fetchall()
            elapsed_time = time.time() - start_time
            times.append(elapsed_time)
            print(f"  Run {i+1}/{num_runs}: {elapsed_time:.2f}s")
        
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
    
    # Print summary
    print("\n" + "="*70)
    print("BENCHMARK RESULTS SUMMARY")
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