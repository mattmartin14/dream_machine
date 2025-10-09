from pyspark.sql import SparkSession
import duckdb
import helpers as ah


def get_spark(aws_region: str | None = None) -> SparkSession:
    """Create a SparkSession configured to read from S3 using the s3a connector."""

    packages = [
        'org.apache.hadoop:hadoop-aws:3.3.4',
    ]

    master = "local[*]"
    bind_addr = "127.0.0.1"
    driver_host = bind_addr

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


def main():
    from run_stuff import get_setup, s3_parquet_test_harness

    catalog_name, aws_region, aws_acct_id, bucket, prefix, glue_db_name = get_setup()

    ah.gen_data_for_s3(bucket)
    print('data generated for s3 parquet test')

    spark = get_spark(aws_region)

    s3_parquet_test_harness(spark, bucket)

    spark.stop()

    ah.nuke_bucket_prefix(bucket, 'test2/')

if __name__ == "__main__":
    main()