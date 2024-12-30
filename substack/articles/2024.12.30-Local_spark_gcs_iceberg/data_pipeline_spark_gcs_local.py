
import os
from pyspark.sql import SparkSession
import warnings

"""
    Author: Matt Martin
    Date: 12/27/24
    Desc:

        This program demonstrates how to use spark to directly write iceberg tables to gcs, but on a local workstation
            - no expensive cloud notebook UI needed!

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
    local_jar_path = "./jars/gcs-connector-3.0.4-shaded.jar"

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

    # read raw datasets
    df_order_header = spark.read.parquet(f"gs://{gcs_bucket}/test_data/order_hdr.parquet")
    df_order_header.createOrReplaceTempView("hdr")
    df_order_detail = spark.read.parquet(f"gs://{gcs_bucket}/test_data/order_dtl.parquet")
    df_order_detail.createOrReplaceTempView("dtl")
    print("raw datasets read in")
    
    ## aggregate up
    sql = """
    SELECT hdr.o_orderkey, hdr.o_custkey, sum(dtl.l_quantity) as tot_qty
        ,sum(dtl.l_extendedprice) as tot_retl
        ,min(dtl.l_shipdate) as first_promise_dt
    FROM hdr
        inner join dtl
            on hdr.o_orderkey = dtl.l_orderkey
    GROUP BY ALL
    """

    df_agg = spark.sql(sql)
    print("datasets combined/aggregated up")

    #write out results
    df_agg.writeTo(f"{catalog_name}.{namespace}.{table_name}") \
        .using("iceberg") \
        .createOrReplace()

    print('data written to gcs')


if __name__ == "__main__":

    catalog_name = "icyhot"
    namespace = "bike_shop"
    table_name = "orders_agg"
    gcs_bucket = os.getenv("GCS_BUCKET")

    process_data(catalog_name, gcs_bucket, namespace, table_name)
