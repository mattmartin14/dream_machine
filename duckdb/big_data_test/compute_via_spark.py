from pyspark.sql import SparkSession
import time
import os


def main():

    spark = (SparkSession.builder 
        .appName("Big Data Test")
        .config("spark.driver.memory", "10g")
        .config("spark.executor.memory", "10g")
        .config("spark.driver.maxResultSize", "2g")  # Optional: limit result size
        .getOrCreate()
    )

    df = spark.read.parquet(os.path.expanduser('~/test_dummy_data/duckdb/data[1-3].parquet'))

    df.createOrReplaceTempView("all_data")

    start_time = time.time()

    result = spark.sql("""
        SELECT rpt_dt, 
               count(distinct txn_key) AS unique_txn_keys,
               count(*) AS total_rows,
               sum(sales_amt) AS total_sales_amt,
               avg(sales_amt) AS avg_sales_amt
        FROM all_data
        GROUP BY rpt_dt
    """)    

    result.show()

    end_time = time.time()
    print(f"Total time for spark to compute dataset: {end_time - start_time:.2f} seconds")
    spark.stop()    

if __name__ == "__main__":  
    main()