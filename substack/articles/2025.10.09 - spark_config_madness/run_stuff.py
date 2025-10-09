import os
import helpers as ah
from pyspark.sql import SparkSession

def test_harness(_spark, catalog_name, glue_db_name, bucket, prefix):

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


def get_setup():

    catalog_name = "iceberg_catalog"
    aws_region = "us-east-1"
    aws_acct_id = os.getenv("AWS_ACCT_ID")
    bucket = os.getenv("aws_bucket")
    prefix = "iceberg"

    glue_db_name = "ice_ice_baby"

    return catalog_name, aws_region, aws_acct_id, bucket, prefix, glue_db_name

def main(spark:SparkSession):

    catalog_name, aws_region, aws_acct_id, bucket, prefix, glue_db_name = get_setup()

    ah.nuke_bucket_prefix(bucket, prefix)

    ah.drop_glue_database(glue_db_name, aws_region)
    ah.create_glue_database(glue_db_name, aws_region)

    test_harness(spark, catalog_name, glue_db_name, bucket, prefix)
    spark.stop()
    
    ah.nuke_bucket_prefix(bucket, prefix)
    ah.drop_glue_database(glue_db_name, aws_region)

