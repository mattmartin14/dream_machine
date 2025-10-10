import os
from pyspark.sql import SparkSession
import helpers as ah

def get_spark(catalog_name: str, bucket: str, prefix: str, aws_region: str) -> SparkSession:
    # Stop any existing session to ensure clean state
    existing_spark = SparkSession.getActiveSession()
    if existing_spark:
        existing_spark.stop()
        SparkSession._instantiatedSession = None
        SparkSession._activeSession = None

    packages = [
        'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2',
        'org.apache.iceberg:iceberg-aws:1.9.2',
        'software.amazon.awssdk:bundle:2.34.0',
        'software.amazon.awssdk:url-connection-client:2.34.0',
        'org.apache.hadoop:hadoop-aws:3.3.4'
    ]

    master = "local[*]"
    bind_addr = "127.0.0.1"
    driver_host = bind_addr

    spark = (
        SparkSession.builder
        .appName('iceberg-glue-spark')
        .master(master)
        .config('spark.jars.packages', ','.join(packages))
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config(f'spark.sql.catalog.{catalog_name}.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
        .config(f'spark.sql.catalog.{catalog_name}', 'org.apache.iceberg.spark.SparkCatalog')
        .config(f'spark.sql.catalog.{catalog_name}.warehouse', f's3://{bucket}/{prefix}')
        .config(f'spark.sql.catalog.{catalog_name}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .config(f'spark.sql.catalog.{catalog_name}.glue.region', aws_region)
        .config('spark.driver.bindAddress', bind_addr)
        .config('spark.driver.host', driver_host)        
        .config('spark.network.timeout', '120s')
        .config('spark.executor.heartbeatInterval', '30s')
        .config('spark.driver.extraJavaOptions', '-Djava.net.preferIPv4Stack=true')
        .config('spark.executor.extraJavaOptions', '-Djava.net.preferIPv4Stack=true')
        .getOrCreate()
    )

    return spark


def main():
    import test_harness as rs

    catalog_name, aws_region, aws_acct_id, bucket, prefix, glue_db_name = rs.get_setup()

    rs.prework(bucket, prefix, glue_db_name, aws_region)

    spark = get_spark(catalog_name, prefix, bucket, aws_region)
    rs.iceberg_test_harness(spark, catalog_name, glue_db_name, bucket, prefix)

    spark.stop()

    rs.postwork(bucket, prefix, glue_db_name, aws_region)


if __name__ == "__main__":
    main()