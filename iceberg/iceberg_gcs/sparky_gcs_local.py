
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, rand, floor, expr
import warnings

"""
    Author: Matt Martin
    Date: 12/27/24
    Desc:

        This program demonstrates how to use spark to directly write iceberg tables to gcs, but on a local workstation
            - no expensive cloud notebook UI needed!

        At the end for validation, we pull in duckdb and query the iceberg table

"""


# Suppress specific Google Cloud SDK warning
warnings.filterwarnings(
    "ignore",
    message="Your application has authenticated using end user credentials.*",
    category=UserWarning,
    module="google.auth._default"
)


def get_spark_instance_via_adc(catalog_name: str, gcs_bucket: str) -> SparkSession:
    """
    Creates a Spark session configured for reading and writing Iceberg tables on GCS with ADC (Application Default Credentials).

    Args:
        catalog_name (str): The Iceberg catalog name. You can call it whatever you want
        gcs_bucket (str): The GCS bucket we are targeting for our iceberg warehouse

    Returns:
        SparkSession: Configured Spark session instance.
    """
    # Iceberg Runtime stuff
    spark_version = os.getenv("SPARK_VERSION", "3.5")
    scala_version = os.getenv("SCALA_VERSION", "2.12")
    iceberg_version = os.getenv("ICEBERG_VERSION", "1.7.0")

    iceberg_package = f"org.apache.iceberg:iceberg-spark-runtime-{spark_version}_{scala_version}:{iceberg_version}"

    # Define the Iceberg warehouse path
    warehouse_path = f"gs://{gcs_bucket}/icehouse"

    # this jar here: https://github.com/GoogleCloudDataproc/hadoop-connectors/releases
    ## seems to be the one that works well (not the gcs-conenctor-hadoop3-latest, which is missing stuff)
    local_jar_path = "./jars/gcs-connector-3.0.4-shaded.jar"

    # if you wanted to use a google service acct, you'd need to add in these configs:
    # .config("spark.hadoop.google.cloud.auth.service.account.enable","true") \
    # .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) \
    # you'd also need to create a gsa and store it locally (not recommened for security reasons)

    return SparkSession.builder \
        .appName("local_spark_gcs") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") \
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
        .config("spark.jars.packages", iceberg_package) \
        .config("spark.jars", local_jar_path) \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "false") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT") \
        .getOrCreate()


## #################################################################

def process_data(catalog_name: str, gcs_bucket:str, namespace: str, table_name: str):

    spark = get_spark_instance_via_adc(catalog_name=catalog_name, gcs_bucket = gcs_bucket)
    print('spark instance created')

    spark.sparkContext.setLogLevel("ERROR")

    #generate some data in a data frame

    row_cnt = 5_000
    df = spark.range(0, row_cnt) \
        .withColumn('rpt_dt', current_date()) \
        .withColumn('some_val', floor(rand() * 100)) \
        .withColumn("txn_key", expr("uuid()")) \
        .withColumnRenamed('id', 'row_id') \
        .toDF('row_id', 'rpt_dt', 'some_val', 'txn_key')

    print('dummy dataset created')

    df.writeTo(f"{catalog_name}.{namespace}.{table_name}") \
        .using("iceberg") \
        .createOrReplace()

    print('data written to gcs')

def validate_data(gcs_bucket: str, namespace: str, table_name: str) ->None:

    ### part 2: query it via duckdb
    import duckdb
    from fsspec import filesystem
    cn = duckdb.connect()
    cn.register_filesystem(filesystem('gcs'))
    cn.execute("""
        INSTALL ICEBERG;
        LOAD ICEBERG;
    """)

    table_path = f"gs://{gcs_bucket}/icehouse/{namespace}/{table_name}"

    sql = f"""
    select *
    from iceberg_scan('{table_path}')
    limit 5
    """

    cn.sql(sql).show()

if __name__ == "__main__":

    catalog_name = "icyhot"
    namespace = "dummy_dataset"
    table_name = "dummy_data"
    gcs_bucket = os.getenv("GCS_BUCKET")

    process_data(catalog_name, gcs_bucket, namespace, table_name)
    validate_data(gcs_bucket, namespace, table_name)