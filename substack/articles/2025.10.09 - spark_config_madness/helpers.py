import boto3
from pyspark.sql import SparkSession
import duckdb

def gen_data_for_s3(bucket):

    cn = duckdb.connect()
    cn.execute("install tpch; load tpch;")
    cn.execute("install aws; load aws")
    cn.execute("call dbgen(sf=1)")
    cn.execute("create secret aws_s3 (type s3, provider credential_chain, region 'us-east-1')")
    cn.execute(f"copy (select * from orders limit 50) to 's3://{bucket}/test2/orders.parquet' (format 'parquet')")

def test_harness(_spark: SparkSession, catalog_name: str, glue_db_name: str, bucket: str, prefix: str) -> None:

    _spark.sql(f"drop table if exists {catalog_name}.{glue_db_name}.test_ice")
    print('table dropped')
    _spark.sql(f"""
       create table {catalog_name}.{glue_db_name}.test_ice (
           id int,
           val string
       )
       using iceberg
       location 's3://{bucket}/{prefix}/test_ice'       
    """)

    _spark.sql(f"insert into {catalog_name}.{glue_db_name}.test_ice values (1, 'a'), (2, 'b'), (3, 'c')")

    _spark.sql(f"select * from {catalog_name}.{glue_db_name}.test_ice").show()

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