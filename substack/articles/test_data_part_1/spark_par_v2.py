
"""
    FYI - its a bad idea to try and run a processpoolexecutor on spark since it runs in parallel itself
    you will get dozens of errors on broken pipes,etc

    spark for this type of use-case is slow...takes 240 seconds to run 
"""

from pyspark.sql import SparkSession
import uuid, random, os
from pyspark.sql.functions import current_date, udf
from pyspark.sql.types import StringType, IntegerType
import time

def crt_uuid():
    return str(uuid.uuid4())

def crt_int():
    return random.randint(0, 100)

def main():
    home_dir = os.path.expanduser("~")
    num_batches = 10
    rows_per_batch = 50_000_000

    spark = SparkSession.builder \
        .appName("BatchProcessing") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()

    s_crt_uuid = udf(crt_uuid, StringType())
    s_crt_int = udf(crt_int, IntegerType())

    start_time = time.time()

    for i in range(num_batches):
        df = spark.range(1, rows_per_batch + 1) \
            .withColumn("uuid", s_crt_uuid()) \
            .withColumn("rpt_dt", current_date()) \
            .withColumn("rand_val", s_crt_int()) \
            .toDF('row_id', 'uuid', 'rpt_dt', 'rand_val')

        f_path = os.path.join(home_dir, f"test_dummy_data/spark/batch_{i+1}")
        df.write.mode('overwrite').parquet(f_path)
        print(f"Written batch {i+1} to {f_path}")

    spark.stop()

    end_time = time.time()
    print(f"Total time to create dataset: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
