import duckdb
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
import statistics
import os

DATA_PATH = '/Volumes/xd1/data/*.parquet'

def create_spark_session():
    """Create and configure Spark session once"""
    # Suppress Hadoop native library warning
    os.environ['HADOOP_HOME'] = '/tmp/hadoop'
    
    spark = (SparkSession.builder
             .appName("BenchmarkSparkRead")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .getOrCreate())
    
    # Suppress verbose logging
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark

def benchmark_spark_read(spark):
    start_time = time.time()
    df = spark.read.parquet(DATA_PATH)
    grouped = df.groupBy("rand_dt").agg(F.count_distinct("rand_str").alias("distinct_rand_str_count")).orderBy("distinct_rand_str_count", ascending=False)
    grouped.collect()
    elapsed_time = time.time() - start_time
    return elapsed_time

def benchmark_duckdb_read():
    start_time = time.time()
    cn = duckdb.connect()
    cn.sql(f"select rand_dt, count(distinct rand_str) from read_parquet('{DATA_PATH}') group by all order by 2 desc").fetchall()
    cn.close()
    elapsed_time = time.time() - start_time
    return elapsed_time

def run_benchmarks(num_runs=5):
    print(f"Running {num_runs} benchmark iterations...\n")
    print("Initializing Spark session...")
    spark = create_spark_session()
    print("Spark session ready.\n")
    
    duckdb_times = []
    spark_times = []
    
    try:
        for i in range(1, num_runs + 1):
            print(f"Run {i}/{num_runs}...")
            
            # DuckDB
            duckdb_time = benchmark_duckdb_read()
            duckdb_times.append(duckdb_time)
            print(f"  DuckDB: {duckdb_time:.2f}s")
            
            # Spark
            spark_time = benchmark_spark_read(spark)
            spark_times.append(spark_time)
            print(f"  Spark:  {spark_time:.2f}s")
            print()
    finally:
        # Clean up Spark session
        spark.stop()
        print("Spark session stopped.\n")
    
    # Calculate statistics
    duckdb_avg = statistics.mean(duckdb_times)
    duckdb_min = min(duckdb_times)
    duckdb_max = max(duckdb_times)
    duckdb_median = statistics.median(duckdb_times)
    
    spark_avg = statistics.mean(spark_times)
    spark_min = min(spark_times)
    spark_max = max(spark_times)
    spark_median = statistics.median(spark_times)
    
    # Print results
    print("\n" + "="*70)
    print(f"{'BENCHMARK RESULTS':^70}")
    print("="*70)
    print(f"{'Metric':<20} {'DuckDB':>20} {'Spark':>20} {'Winner':>10}")
    print("-"*70)
    print(f"{'Average':<20} {duckdb_avg:>18.2f}s {spark_avg:>18.2f}s {_winner(duckdb_avg, spark_avg):>10}")
    print(f"{'Median':<20} {duckdb_median:>18.2f}s {spark_median:>18.2f}s {_winner(duckdb_median, spark_median):>10}")
    print(f"{'Min':<20} {duckdb_min:>18.2f}s {spark_min:>18.2f}s {_winner(duckdb_min, spark_min):>10}")
    print(f"{'Max':<20} {duckdb_max:>18.2f}s {spark_max:>18.2f}s {_winner(duckdb_max, spark_max):>10}")
    print("-"*70)
    
    speedup = spark_avg / duckdb_avg if duckdb_avg > 0 else 0
    if speedup >= 1:
        print(f"DuckDB is {speedup:.2f}x faster than Spark on average")
    else:
        print(f"Spark is {1/speedup:.2f}x faster than DuckDB on average")
    print("="*70)

def _winner(duckdb_val, spark_val):
    return "DuckDB" if duckdb_val < spark_val else "Spark"

if __name__ == "__main__":
    run_benchmarks(num_runs=5)