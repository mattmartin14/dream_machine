from pyspark.sql import SparkSession

def create_spark_session():
    """
    Create a Spark session for local execution.
    """
    spark = (
            SparkSession.builder 
                .appName("LocalSession") 
                .master("local[*]") 
                .config("spark.driver.memory", "4g") 
                .config("spark.executor.memory", "4g") 
                .config("spark.sql.shuffle.partitions", "2") 
                .config("spark.driver.bindAddress","127.0.0.1") 
                .config("spark.driver.host", "localhost") 
                .getOrCreate()
    )
    
    return spark
