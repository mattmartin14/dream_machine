import os
import sys
from pyspark.sql import SparkSession
import boto3
import subprocess

#from setup_env import setup_aws_environment

def set_spark_session(catalog_name: str, aws_acct_id: str, aws_region: str) -> SparkSession:

    packages = [
        'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2',
        'org.apache.iceberg:iceberg-aws:1.9.2',
        'software.amazon.awssdk:bundle:2.34.0',
        'software.amazon.awssdk:url-connection-client:2.34.0',
        'org.apache.hadoop:hadoop-aws:3.3.4'
    ]

    master = os.getenv("SPARK_MASTER", "local[*]")
    bind_addr = os.getenv("SPARK_BIND_ADDRESS", "127.0.0.1")
    driver_host = os.getenv("SPARK_DRIVER_HOST", bind_addr)

    spark = (
        SparkSession.builder
        .appName('osspark')
        .master(master)
        .config('spark.jars.packages', ','.join(packages))
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config(f'spark.sql.catalog.{catalog_name}.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
        .config(f'spark.sql.catalog.{catalog_name}', 'org.apache.iceberg.spark.SparkCatalog')
        .config(f'spark.sql.catalog.{catalog_name}.warehouse', f's3://{os.getenv("aws_bucket")}/icehouse1')
        .config(f'spark.sql.catalog.{catalog_name}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .config(f'spark.sql.catalog.{catalog_name}.glue.region', aws_region)

        .config('spark.driver.bindAddress', bind_addr)   # what the driver binds to
        .config('spark.driver.host', driver_host)        # what the driver advertises to executors
        .config('spark.network.timeout', '120s')
        .config('spark.executor.heartbeatInterval', '30s')
        .config('spark.driver.extraJavaOptions', '-Djava.net.preferIPv4Stack=true')
        .config('spark.executor.extraJavaOptions', '-Djava.net.preferIPv4Stack=true')

        # # Optional: let OS choose free ports (defaults already do this, but harmless)
        # .config('spark.blockManager.port', '0')
        .getOrCreate()
    )

    return spark

def render_sql(sql: str, formats: dict) -> str:
    if formats:
        return sql.format(**formats)
    return sql

def process_script(spark: SparkSession, file_path: str, formats: dict=None) -> None:
    with open(file_path, 'r') as f:
        for sql in f.read().split(';'):
            sql = sql.strip()
            if sql:
                rendered_sql = render_sql(sql, formats)
                print(f"Executing SQL: {rendered_sql}")
                spark.sql(rendered_sql)

def nuke_bucket_prefix(bucket: str, prefix: str) -> None:
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    delete_us = dict(Objects=[])
    for item in pages.search('Contents'):
        if item:
            delete_us['Objects'].append(dict(Key=item['Key']))
            if len(delete_us['Objects']) >= 1000:
                s3.delete_objects(Bucket=bucket, Delete=delete_us)
                delete_us = dict(Objects=[])
    if len(delete_us['Objects']):
        s3.delete_objects(Bucket=bucket, Delete=delete_us)

def create_glue_database(db_name: str, aws_region: str) -> None:
    glue = boto3.client('glue', region_name=aws_region)
    try:
        glue.create_database(DatabaseInput={'Name': db_name})
        print(f"Created Glue database: {db_name}")
    except glue.exceptions.AlreadyExistsException:
        print(f"Glue database already exists: {db_name}")
    except Exception as e:
        print(f"Error creating Glue database {db_name}: {e}")

def drop_glue_database(db_name: str, aws_region: str) -> None:
    glue = boto3.client('glue', region_name=aws_region)
    try:
        glue.delete_database(Name=db_name)
        print(f"Dropped Glue database: {db_name}")
    except glue.exceptions.EntityNotFoundException:
        print(f"Glue database not found for deletion: {db_name}")
    except Exception as e:
        print(f"Error deleting Glue database {db_name}: {e}")

def main():

    catalog_name = "iceberg_catalog"
    aws_acct_id = os.getenv('AWS_ACCT_ID')
    bucket = os.getenv("aws_bucket")
    aws_region = 'us-east-1'
    prefix = 'icehouse'

    spark = set_spark_session(catalog_name, aws_acct_id, aws_region)

    ## The Configs:
    print('--ENVIRONMENT CONFIGURATIONS--')
    print(f"Python version: {sys.version}")
    print(f"Spark version: {spark.version}")
    print(f"Scala version: {spark.sparkContext._jvm.scala.util.Properties.versionString()}")
    print(f"Java version: {spark.sparkContext._jvm.System.getProperty('java.version')}")
    print('--ENVIRONMENT CONFIGURATIONS--')

    spark.sql("select 1 as x").show()

    db_name = "icebox5"

    drop_glue_database(db_name, aws_region)
    create_glue_database(db_name, aws_region)

    params = {'BUCKET': bucket, 'DB_NAME': db_name, 'WAREHOUSE': f's3://{bucket}/{prefix}'}


    # nuke tables
    sql_file = 'sql/nuke_tables.sql'
    process_script(spark, sql_file, formats=params)

    # nuke s3 warehouse
    nuke_bucket_prefix(bucket, prefix)

    # create tables
    sql_file = 'sql/create_tables.sql'
    process_script(spark, sql_file, formats=params)

    # merge into
    try:
        sql_file = 'sql/test_merge.sql'
        process_script(spark, sql_file, formats=params)
        print("Merge worked")
    except Exception as e:
        print(f"Error attempting merge script: {sql_file}: {e}")

    # insert left join
    try:
        sql_file = 'sql/test_insert_left_join.sql'
        process_script(spark, sql_file, formats=params)
        print("Insert left join worked")
    except Exception as e:
        print(f"Error attempting insert left join script: {sql_file}: {e}")

    # update
    try:
        sql_file = 'sql/test_update.sql'
        process_script(spark, sql_file, formats=params)
        print("Update worked")
    except Exception as e:
        print(f"Error attempting update script: {sql_file}: {e}")

    # delete
    try:
        sql_file = 'sql/test_delete.sql'
        process_script(spark, sql_file, formats=params)
        print("Delete worked")
    except Exception as e:
        print(f"Error attempting delete script: {sql_file}: {e}")


    try:
        sql_file = 'sql/test_ctas.sql'
        process_script(spark, sql_file, formats=params)
    except Exception as e:
        print(f"Error attempting ctas script: {sql_file}: {str(e)[:200]}")

    

    #final output
    spark.sql(f"select * from iceberg_catalog.{db_name}.test1").show()

    # clean up
    sql_file = 'sql/nuke_tables.sql'
    process_script(spark, sql_file, formats=params)
    nuke_bucket_prefix(bucket, prefix)

    spark.stop()

if __name__ == "__main__":
    main()