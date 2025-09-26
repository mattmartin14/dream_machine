import sys
import os
import time
from pyspark.sql import SparkSession

#from setup_env import set_aws_creds
from setup_env import setup_aws_environment

aws_acct_id = os.getenv('AWS_ACCT_ID')
aws_region = 'us-east-1'

aws_session = setup_aws_environment()

s3_warehouse_path = 's3://matt-sbx-bucket-1-us-east-1/icehouse/'
catalog_name = "iceberg_catalog"

spark = (
    SparkSession.builder
    .appName("IcebergLocalDev")
    # Include all necessary AWS SDK dependencies for Glue integration
    .config('spark.jars.packages', 
           "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
           "org.apache.iceberg:iceberg-aws:1.4.3,"
           "software.amazon.awssdk:bundle:2.20.18,"
           "software.amazon.awssdk:url-connection-client:2.20.18,"
           "software.amazon.awssdk:glue:2.20.18,"
           "software.amazon.awssdk:s3:2.20.18,"
           "software.amazon.awssdk:sts:2.20.18,"
           "software.amazon.awssdk:dynamodb:2.20.18")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", s3_warehouse_path)
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)


glue_db_name = 'icebox1'

spark.sql(f"""
   create or replace table {catalog_name}.{glue_db_name}.iceberg_test2 
    using iceberg 
    location '{s3_warehouse_path}/iceberg_test2' 
    as
    select 1 as id, 'a' as data
    union all
    select 2 as id, 'b' as data      
""")

spark.sql(f"""
   create or replace table {catalog_name}.{glue_db_name}.iceberg_test3
       using iceberg 
        location '{s3_warehouse_path}/iceberg_test3' 
        as
        select 1 as id, 'c' as data
        union all
        select 3 as id, 'r' as data  
""")



spark.sql(f"select * from {catalog_name}.{glue_db_name}.iceberg_test limit 5").show()

# test merge
sql = f"""
    merge into {catalog_name}.{glue_db_name}.iceberg_test2 as tgt
        using {catalog_name}.{glue_db_name}.iceberg_test3 as src
            on tgt.id = src.id
    when matched then update set tgt.data = src.data
    when not matched then insert (id, data) values (src.id, src.data)
"""
spark.sql(sql)

print("merge ran")

spark.stop()