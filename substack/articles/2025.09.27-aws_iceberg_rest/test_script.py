import os
import sys
from pyspark.sql import SparkSession
import boto3

from setup_env import setup_aws_environment

def set_spark_session(catalog_name: str, aws_acct_id: str, aws_region: str) -> SparkSession:

    packages = [
        'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2',
        'software.amazon.awssdk:bundle:2.34.0',
        'software.amazon.awssdk:url-connection-client:2.34.0',
        'org.apache.hadoop:hadoop-aws:3.3.4'
    ]

    spark = (SparkSession.builder.appName('osspark') 
        .config('spark.jars.packages', ','.join(packages)) 
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') 
        .config('spark.sql.defaultCatalog', catalog_name) 
        .config(f'spark.sql.catalog.{catalog_name}', 'org.apache.iceberg.spark.SparkCatalog') 
        .config(f'spark.sql.catalog.{catalog_name}.type', 'rest') 
        .config(f'spark.sql.catalog.{catalog_name}.uri',f'https://glue.{aws_region}.amazonaws.com/iceberg') 
        .config(f'spark.sql.catalog.{catalog_name}.warehouse',aws_acct_id) 
        .config(f'spark.sql.catalog.{catalog_name}.rest.sigv4-enabled','true') 
        .config(f'spark.sql.catalog.{catalog_name}.rest.signing-name','glue') 
        .config(f'spark.sql.catalog.{catalog_name}.rest.signing-region', aws_region) 
        .config(f'spark.sql.catalog.{catalog_name}.io-impl','org.apache.iceberg.aws.s3.S3FileIO') 
        .config(f'spark.hadoop.fs.s3a.aws.credentials.provider','com.amazonaws.auth.DefaultAWSCredentialsProviderChain') 
        .config(f'spark.sql.catalog.{catalog_name}.rest-metrics-reporting-enabled','false') 
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
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

def nuke_bucket_prefix(session: boto3.Session, bucket: str, prefix: str) -> None:
    s3 = session.client('s3')
    prefix = 'icehouse/'
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

def main():

    aws_session = setup_aws_environment()

    catalog_name = "iceberg_catalog"
    aws_acct_id = os.getenv('AWS_ACCT_ID')
    bucket = os.getenv("aws_bucket")
    aws_region = 'us-east-1'

    spark = set_spark_session(catalog_name, aws_acct_id, aws_region)

    ## The Configs:
    print(f"PySpark version: {spark.version}")
    print(f"Scala version: {spark.sparkContext._jvm.scala.util.Properties.versionString()}")
    print(f"Java version: {spark.sparkContext._jvm.System.getProperty('java.version')}")

    # nuke tables
    sql_file = 'sql/nuke_tables.sql'
    process_script(spark, sql_file, formats=None)

    # nuke s3 warehouse
    nuke_bucket_prefix(aws_session, bucket, "icehouse/")

    # create tables
    sql_file = 'sql/create_tables.sql'
    process_script(spark, sql_file, formats={'BUCKET': bucket})

    # merge into
    try:
        sql_file = 'sql/test_merge.sql'
        process_script(spark, sql_file, formats=None)
        print("Merge worked")
    except Exception as e:
        print(f"Error attempting merge script: {sql_file}: {e}")

    # insert left join
    try:
        sql_file = 'sql/test_insert_left_join.sql'
        process_script(spark, sql_file, formats=None)
        print("Insert left join worked")
    except Exception as e:
        print(f"Error attempting insert left join script: {sql_file}: {e}")

    # update
    try:
        sql_file = 'sql/test_update.sql'
        process_script(spark, sql_file, formats=None)
        print("Update worked")
    except Exception as e:
        print(f"Error attempting update script: {sql_file}: {e}")

    # update join (doesn't work...just use merge)
    try:
        sql_file = 'sql/test_update_join.sql'
        process_script(spark, sql_file, formats=None)
        print("Update join worked")
    except Exception as e:
        print(f"Error attempting update join script: {sql_file}: {e}")

    # delete
    try:
        sql_file = 'sql/test_delete.sql'
        process_script(spark, sql_file, formats=None)
        print("Delete worked")
    except Exception as e:
        print(f"Error attempting delete script: {sql_file}: {e}")


    # ctas...doesn't work; according to aws docs, "stage-create" is not supported on its iceberg rest endpoint
    try:
        sql_file = 'sql/test_ctas.sql'
        process_script(spark, sql_file, formats={'BUCKET': bucket})
    except Exception as e:
        print(f"Error attempting ctas script: {sql_file}: {str(e)[:200]}")

    # to mimick a ctas, we have to create the skeleton table, then just overwrite it
    try:
        df = spark.read.parquet(f's3a://{bucket}/duckdb/data_gen_parquet/*')
        #spark.sql("select * from {df} limit 5", df=df).show()
        spark.sql(f"""
            create table if not exists iceberg_catalog.icebox1.ctas 
                (row_id int, txn_key string, rpt_dt date, some_val float) 
            using iceberg
            location 's3://{bucket}/icehouse/ctas'
        """)
        df.write.format("iceberg").mode("overwrite").saveAsTable("iceberg_catalog.icebox1.ctas")
        print("CTAS via dataframe worked")
    except Exception as e:
        print(f"Error attempting ctas via dataframe: {str(e)[:200]}")

    #final output
    spark.sql("select * from iceberg_catalog.icebox1.test1").show()

    # clean up
    sql_file = 'sql/nuke_tables.sql'
    process_script(spark, sql_file, formats=None)
    nuke_bucket_prefix(aws_session, bucket, "icehouse/")

    spark.stop()

if __name__ == "__main__":
    main()