import os
import sys
from pyspark.sql import SparkSession

from setup_env import setup_aws_environment


aws_session = setup_aws_environment()

def set_spark_session(catalog_name: str, aws_acct_id: str, aws_region: str) -> SparkSession:

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
        .config('spark.driver.extraJavaOptions', '-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN')
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

def main():

    catalog_name = "iceberg_catalog"
    aws_acct_id = os.getenv('AWS_ACCT_ID')
    bucket = os.getenv("aws_bucket")
    aws_region = 'us-east-1'

    spark = set_spark_session(catalog_name, aws_acct_id, aws_region)
    
    # nuke tables
    sql_file = 'sql/nuke_tables.sql'
    process_script(spark, sql_file, formats=None)

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


    # ctas...doesn't work
    try:
        sql_file = 'sql/test_ctas.sql'
        process_script(spark, sql_file, formats={'BUCKET': bucket})
    except Exception as e:
        print(f"Error attempting ctas script: {sql_file}: {str(e)[:200]}")

    #final output
    spark.sql("select * from iceberg_catalog.icebox1.test1").show()

    spark.stop()

if __name__ == "__main__":
    main()