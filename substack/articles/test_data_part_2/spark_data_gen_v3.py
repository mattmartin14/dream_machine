from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, rand, floor, expr
import time, os

home_dir = os.path.expanduser("~")

def write_data(spark, rows):

    df = spark.range(0, rows) \
        .withColumn('rpt_dt', current_date()) \
        .withColumn('some_val', floor(rand() * 100)) \
        .withColumn("txn_key", expr("uuid()")) \
        .withColumnRenamed('id', 'row_id') \
        .toDF('row_id', 'rpt_dt', 'some_val', 'txn_key') 
    
    f_path = os.path.join(home_dir, f"test_dummy_data/spark")
    df.write.mode('overwrite').parquet(f_path)

def main():
    start_time = time.time()

    rows = 500_000_000

    spark = SparkSession.builder \
        .appName("Spark Data Generation") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "12g") \
        .config("spark.executor.cores", "8") \
        .getOrCreate()

    write_data(spark, rows)

    end_time = time.time()
    print(f"Total time to create dataset: {end_time - start_time:.2f} seconds")
    spark.stop()

if __name__ == "__main__":
    main()
