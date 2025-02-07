spark_version = "3.5"
scala_version = "2.12"
iceberg_version = "1.7.0"

import shutil
import os
from pyspark.sql import SparkSession

catalog_name = "iceberg"
warehouse_path = "./icehouse"

if os.path.exists(warehouse_path):
    shutil.rmtree(warehouse_path)
os.makedirs(warehouse_path) 

spark = (SparkSession.builder 
    .appName("local_iceberg_example") 
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") 
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") 
    .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") 
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) 
    .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-{spark_version}_{scala_version}:{iceberg_version}") 
    .config("spark.driver.bindAddress", "127.0.0.1") 
    .config("spark.driver.host", "localhost") 
    .config("spark.driver.memory", "4g") 
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

namespace = "test_ns"

dfh = spark.read.parquet("./raw_data/ord_hdr/*")
dfd = spark.read.parquet("./raw_data/ord_dtl/*")

table_name = "ord_hdr_no_partition"
dfh.writeTo(f"{catalog_name}.{namespace}.{table_name}") \
    .using("iceberg") \
    .createOrReplace()

table_name = "ord_dtl_no_partition"
dfd.writeTo(f"{catalog_name}.{namespace}.{table_name}") \
    .using("iceberg") \
    .createOrReplace()


table_name = "ord_hdr_partitioned"
dfh.createOrReplaceTempView("hdr")

spark.sql(f"""
        CREATE OR REPLACE TABLE {catalog_name}.{namespace}.{table_name}
        USING ICEBERG
        PARTITIONED BY (order_date)
        AS SELECT *
        FROM hdr
          """)


table_name = "ord_dtl_partitioned"
dfd.createOrReplaceTempView("dtl")

spark.sql(f"""
        CREATE OR REPLACE TABLE {catalog_name}.{namespace}.{table_name}
        USING ICEBERG
        PARTITIONED BY (order_date)
        AS SELECT *
        FROM dtl
          """)


print('process completed')