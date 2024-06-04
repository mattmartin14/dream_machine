from pyspark.sql import SparkSession
import uuid, random, os
from pyspark.sql.functions import current_date, udf
from pyspark.sql.types import StringType, IntegerType

def crt_uuid():
    return str(uuid.uuid4())

def crt_int():
    return random.randint(0, 100)

s_crt_uuid = udf(crt_uuid, StringType())
s_crt_int = udf(crt_int, IntegerType())

home_dir = os.path.expanduser("~")

import time

def write_data(batch_num, rows):

    spark = SparkSession.builder \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    df = spark.range(1,rows+1) \
        .withColumn("uuid", s_crt_uuid()) \
        .withColumn("rpt_dt", current_date()) \
        .withColumn("rand_val", s_crt_int()) \
        .toDF('row_id', 'uuid', 'rpt_dt', 'rand_val')
    #df.show()

    f_path = os.path.join(home_dir, f"test_dummy_data/spark/{batch_num}")
    print(f_path)

    df.write.mode('overwrite').parquet(f_path)

def main():

    start_time = time.time()

    num_batches = 10
    rows = 500_000_000

    write_data(1, rows)  

    end_time = time.time()
    print(f"Total time to create dataset: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()