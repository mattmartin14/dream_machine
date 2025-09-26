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

s3_warehouse_path = 's3://matt-sbx-bucket-1-us-east-1/icehouse/'
glue_db_name = 'icebox1'

spark.sql(f"select * from {glue_db_name}.iceberg_test limit 5").show()

# example: https://aws.amazon.com/blogs/big-data/read-and-write-s3-iceberg-table-using-aws-glue-iceberg-rest-catalog-from-open-source-apache-spark/

## doesn't support create or replace but supports create
#spark.sql(f"create table {catalog_name}.{glue_db_name}.iceberg_test2 (id bigint, data string) using iceberg location '{s3_warehouse_path}/iceberg_test2'")

spark.sql(f"insert into {catalog_name}.{glue_db_name}.iceberg_test2 values(1, 'a'), (2, 'b')").show()

spark.stop()