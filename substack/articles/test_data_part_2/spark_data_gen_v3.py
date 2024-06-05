from pyspark.sql import SparkSession
import uuid, random, os
from pyspark.sql.functions import current_date, udf, monotonically_increasing_id, expr
from pyspark.sql.types import StringType, IntegerType
import time

@udf(StringType())
def generate_uuid():
    return str(uuid.uuid4())

@udf(IntegerType())
def generate_random_int():
    return random.randint(0, 100)

home_dir = os.path.expanduser("~")

def write_data(spark, rows):

    df = spark.range(0, rows) \
        .withColumn("txn_key", generate_uuid()) \
        .withColumn("rpt_dt", current_date()) \
        .withColumn("some_val", generate_random_int()) \
        .withColumn("row_id", monotonically_increasing_id()) \
        .select("row_id", "txn_key", "rpt_dt", "some_val")

    f_path = os.path.join(home_dir, f"test_dummy_data/spark")
    #print(f_path)

    df.write.mode('overwrite').parquet(f_path)

def main():
    start_time = time.time()

    num_batches = 10
    rows = 500_000_000

    #  .config("spark.sql.shuffle.partitions", "200") \
    spark = SparkSession.builder \
        .appName("Spark Data Generation") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "8") \
        .getOrCreate()

    write_data(spark, rows)

    end_time = time.time()
    print(f"Total time to create dataset: {end_time - start_time:.2f} seconds")
    spark.stop()

if __name__ == "__main__":
    main()
