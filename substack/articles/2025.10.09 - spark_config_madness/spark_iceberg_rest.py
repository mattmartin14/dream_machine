from pyspark.sql import SparkSession

def get_spark(catalog_name, aws_region, aws_acct_id):


    packages = [
       "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1",
       "software.amazon.awssdk:bundle:2.20.160",
       "software.amazon.awssdk:url-connection-client:2.20.160"
    ]

    spark = (SparkSession.builder.appName('iceberg-rest-spark') 
        .config('spark.jars.packages', ','.join(packages)) 
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

    return spark


def main():
    import run_stuff as rs
   
    catalog_name, aws_region, aws_acct_id, bucket, prefix, glue_db_name = rs.get_setup()

    rs.prework(bucket, prefix, glue_db_name, aws_region)

    spark = get_spark(catalog_name, aws_region, aws_acct_id)

    rs.iceberg_test_harness(spark, catalog_name, glue_db_name, bucket, prefix)
    spark.stop()

    rs.postwork(bucket, prefix, glue_db_name, aws_region)


if __name__ == "__main__":
    main()
