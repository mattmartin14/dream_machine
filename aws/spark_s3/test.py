import os
from pyspark.sql import SparkSession
import duckdb

def set_spark_session(aws_region: str | None = None) -> SparkSession:
    """Create a SparkSession configured to read from S3 using the s3a connector."""

    packages = [
        'org.apache.hadoop:hadoop-aws:3.3.4',
    ]

    master = os.getenv("SPARK_MASTER", "local[*]")
    bind_addr = os.getenv("SPARK_BIND_ADDRESS", "127.0.0.1")
    driver_host = os.getenv("SPARK_DRIVER_HOST", bind_addr)

    spark = (
        SparkSession.builder
        .appName('spark-s3a-read')
        .master(master)
        .config('spark.jars.packages', ','.join(packages))
        # Core S3A settings
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')
        .config('spark.hadoop.fs.s3a.path.style.access', 'false')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'true')
        # Networking and driver binding for local usage
        .config('spark.driver.bindAddress', bind_addr)
        .config('spark.driver.host', driver_host)
        .config('spark.network.timeout', '120s')
        .config('spark.executor.heartbeatInterval', '30s')
        .config('spark.hadoop.fs.s3a.endpoint', f's3.{aws_region}.amazonaws.com')
        .config('spark.driver.extraJavaOptions', '-Djava.net.preferIPv4Stack=true')
        .config('spark.executor.extraJavaOptions', '-Djava.net.preferIPv4Stack=true')
        .getOrCreate()
    )
    
    return spark

def gen_data():
    
    cn = duckdb.connect()
    cn.execute("install tpch; load tpch;")
    cn.execute("install aws; load aws")
    cn.execute("call dbgen(sf=1)")
    cn.execute("create secret aws_s3 (type s3, provider credential_chain, region 'us-east-1')")
    cn.execute(f"copy (select * from orders limit 50) to 's3://{os.getenv('aws_bucket')}/test2/orders.parquet' (format 'parquet')")

def main():
    # Region is optional for reading from S3; set if you want a specific endpoint
    aws_region = 'us-east-1'

    spark = set_spark_session(aws_region)
    df = spark.read.parquet(f's3a://{os.getenv("aws_bucket")}/test2/*')
    df.createOrReplaceTempView("data")
    df.show()

    spark.sql("select count(*) as row_cnt from data").write.mode("overwrite").parquet(f's3a://{os.getenv("aws_bucket")}/test2/output_count')

    spark.stop()

if __name__ == "__main__":
    #gen_data()
    main()