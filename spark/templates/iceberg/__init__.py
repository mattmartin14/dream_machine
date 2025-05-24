spark_version = "3.5"
scala_version = "2.12"
iceberg_version = "1.7.0"

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, rand, floor, expr

catalog_name = "iceberg"
warehouse_path = "./icehouse"

spark = (
        SparkSession.builder 
            .appName("iceberg_timetravel_stuff") 
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") 
            .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") 
            .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") 
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) 
            .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-{spark_version}_{scala_version}:{iceberg_version}") 
            .config("spark.driver.bindAddress","127.0.0.1") 
            .config("spark.driver.host", "localhost") 
            .getOrCreate()
)

namespace = "test_ns"
spark.sql(f"create namespace {namespace}")