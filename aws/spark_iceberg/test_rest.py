import sys
import os
import time
from pyspark.sql import SparkSession

#from setup_env import set_aws_creds
from setup_env import setup_aws_environment

aws_acct_id = os.getenv('AWS_ACCT_ID')
aws_region = 'us-east-1'

aws_session = setup_aws_environment()

catalog_name = "iceberg_catalog"

spark = (SparkSession.builder.appName('osspark') 
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160') 
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') 
    .config('spark.sql.defaultCatalog', catalog_name) 
    .config(f'spark.sql.catalog.{catalog_name}', 'org.apache.iceberg.spark.SparkCatalog') 
    .config(f'spark.sql.catalog.{catalog_name}.type', 'rest') 
    .config(f'spark.sql.catalog.{catalog_name}.uri',f'https://glue.{aws_region}.amazonaws.com/iceberg') 
    .config(f'spark.sql.catalog.{catalog_name}.warehouse',aws_acct_id) 
    .config(f'spark.sql.catalog.{catalog_name}.rest.sigv4-enabled','true') 
    .config(f'spark.sql.catalog.{catalog_name}.rest.signing-name','glue') 
    .config(f'spark.sql.catalog.{catalog_name}.rest.signing-region', aws_region) \
    .config(f'spark.sql.catalog.{catalog_name}.io-impl','org.apache.iceberg.aws.s3.S3FileIO') 
    .config(f'spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialProvider') 
    .config(f'spark.sql.catalog.{catalog_name}.rest-metrics-reporting-enabled','false') 
    .getOrCreate()
)

s3_warehouse_path = f's3://{os.getenv("aws_bucket")}/icehouse/'
glue_db_name = 'icebox1'

spark.sql(f"select * from {glue_db_name}.iceberg_test limit 5").show()

# example: https://aws.amazon.com/blogs/big-data/read-and-write-s3-iceberg-table-using-aws-glue-iceberg-rest-catalog-from-open-source-apache-spark/

## doesn't support create or replace but supports create
#spark.sql(f"create table {catalog_name}.{glue_db_name}.iceberg_test2 (id bigint, data string) using iceberg location '{s3_warehouse_path}/iceberg_test2'")

spark.sql(f"insert into {catalog_name}.{glue_db_name}.iceberg_test2 values(1, 'a'), (2, 'b')").show()

spark.sql(f"update {catalog_name}.{glue_db_name}.iceberg_test2 set data = 'c' where id = 1")
print("update worked")

#test delete
spark.sql(f"delete from {catalog_name}.{glue_db_name}.iceberg_test2 where id = 2")
print("delete worked")

#test ctas
spark.sql(f"""create or replace table {catalog_name}.{glue_db_name}.iceberg_test3 
          using iceberg
          location '{s3_warehouse_path}/iceberg_test3'
          as 
          select * from {catalog_name}.{glue_db_name}.iceberg_test2
""")

print("ctas worked")

spark.sql(f"drop table if exists {catalog_name}.{glue_db_name}.iceberg_test4")
spark.sql(f"drop table if exists {catalog_name}.{glue_db_name}.iceberg_test5")

spark.sql(f"""create table {catalog_name}.{glue_db_name}.iceberg_test4 
          (id int, val string)
          using iceberg
          location '{s3_warehouse_path}/iceberg_test4'
""")

print('ctas 2 worked')

spark.sql(f"insert into {catalog_name}.{glue_db_name}.iceberg_test4 values(1, 'x'), (2, 'y'), (3, 'z')")

spark.sql(f"""create table {catalog_name}.{glue_db_name}.iceberg_test5
          (id int, val string)
          using iceberg
          location '{s3_warehouse_path}/iceberg_test5'
""")

spark.sql(f"insert into {catalog_name}.{glue_db_name}.iceberg_test5 values(2, 'y2'), (3, 'z2'), (4, 'w')")


#test merge
merge_sql = f"""
    MERGE INTO {catalog_name}.{glue_db_name}.iceberg_test4 as tgt
        USING {catalog_name}.{glue_db_name}.iceberg_test5 as src
            ON tgt.id = src.id
    WHEN MATCHED THEN UPDATE SET tgt.val = src.val
    WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val)
"""

spark.sql(merge_sql)
print("merge worked")

spark.sql(f"select * from {catalog_name}.{glue_db_name}.iceberg_test2 limit 5").show()

# this ctas doesnt work
spark.sql(f"""create or replace table {catalog_name}.{glue_db_name}.iceberg_test6
          using iceberg
          location '{s3_warehouse_path}/iceberg_test6'
          as
          with vals as (
             select 1 as id, 'a' as data
             union all
             select 2 as id, 'b' as data
             union all
             select 3 as id, 'c' as data
          )
          select * from vals

""")

spark.stop()