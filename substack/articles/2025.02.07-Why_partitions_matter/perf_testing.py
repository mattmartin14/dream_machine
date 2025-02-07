
#import os
#os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@11'

spark_version = "3.5"
scala_version = "2.12"
iceberg_version = "1.7.0"
import time

from pyspark.sql import SparkSession

catalog_name = "iceberg"
warehouse_path = "./icehouse"


spark = (SparkSession.builder 
    .appName("local_iceberg_example") 
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") 
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") 
    .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") 
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) 
    .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-{spark_version}_{scala_version}:{iceberg_version}") 
    .config("spark.driver.bindAddress", "127.0.0.1") 
    .config("spark.driver.host", "localhost") 
    .config("spark.sql.iceberg.vectorization.enabled", "false") 
    .getOrCreate()
)

namespace = "test_ns"

spark.sql(f"select * from {catalog_name}.{namespace}.ord_hdr_no_partition limit 5").show()

non_partitioned_sql = f"""
    SELECT hdr.order_status, sum(dtl.price) as ext_price
    from {catalog_name}.{namespace}.ord_hdr_no_partition as hdr
        inner join {catalog_name}.{namespace}.ord_dtl_no_partition as dtl
            USING(order_date, order_id)
    where hdr.order_date = cast('2023-01-01' as date)
        and dtl.order_date = cast('2023-01-01' as date)
    GROUP BY hdr.order_status
"""

#spark.sql(sql).show()

partitioned_sql = f"""
    SELECT hdr.order_status, sum(dtl.price) as ext_price
    from {catalog_name}.{namespace}.ord_hdr_partitioned as hdr
        inner join {catalog_name}.{namespace}.ord_dtl_partitioned as dtl
            USING(order_date, order_id)
    where hdr.order_date = cast('2023-01-01' as date)
        and dtl.order_date = cast('2023-01-01' as date)
    GROUP BY hdr.order_status
"""

def run_timed_query(query, description="Query"):
    """Runs a query and returns its execution time in milliseconds."""
    start_time = time.time()
    spark.sql(query).collect()  # Using collect() instead of show() to avoid UI overhead
    end_time = time.time()
    execution_time_ms = (end_time - start_time) * 1000
    return execution_time_ms

def benchmark_query(query, description, runs=5):

    execution_times = []
    
    for i in range(runs):
        exec_time = run_timed_query(query, f"{description} Run {i+1}")
        print(f"{description} Run {i+1}: {exec_time:.2f} ms")
        execution_times.append(exec_time)

    avg_time = sum(execution_times[1:]) / (runs - 1)
    print(f"\nAverage execution time for {description} (excluding first run): {avg_time:.2f} ms\n")

benchmark_query(non_partitioned_sql, "Non-Partitioned Query")
benchmark_query(partitioned_sql, "Partitioned Query")

print('table stats')
print('header table counts)')
spark.sql(f"select count(*) from {catalog_name}.{namespace}.ord_hdr_partitioned").show()
spark.sql(f"select count(*) from {catalog_name}.{namespace}.ord_hdr_no_partition").show()

print('detail table counts)')
spark.sql(f"select count(*) from {catalog_name}.{namespace}.ord_dtl_partitioned").show()
spark.sql(f"select count(*) from {catalog_name}.{namespace}.ord_dtl_no_partition").show()

print('explain plans -------------')
spark.sql(f"explain {partitioned_sql}").show(truncate=False)
spark.sql(f"explain {non_partitioned_sql}").show(truncate=False)


