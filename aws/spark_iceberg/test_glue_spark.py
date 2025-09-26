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
iceberg_catalog = "iceberg_catalog"

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
           "software.amazon.awssdk:dynamodb:2.20.18,"
           "com.amazonaws:aws-java-sdk-glue:1.12.470,"
           "org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    
    # Configure Iceberg catalog for Iceberg tables
    .config(f"spark.sql.catalog.{iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{iceberg_catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{iceberg_catalog}.warehouse", s3_warehouse_path)
    .config(f"spark.sql.catalog.{iceberg_catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    
    # Configure Hive to use AWS Glue Data Catalog (like AWS Glue)
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .config("spark.hadoop.hive.metastore.warehouse.dir", s3_warehouse_path)
    
    # Disable local database creation - use Glue catalog instead
    .config("spark.hadoop.javax.jdo.option.ConnectionURL", "")
    .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "")
    .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "")
    .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "")
    .config("spark.hadoop.datanucleus.autoCreateSchema", "false")
    .config("spark.hadoop.datanucleus.schema.autoCreateAll", "false")
    .config("spark.hadoop.datanucleus.schema.validateTables", "false")
    .config("spark.hadoop.datanucleus.schema.validateColumns", "false")
    .config("spark.hadoop.datanucleus.schema.validateConstraints", "false")
    .config("spark.hadoop.hive.metastore.schema.verification", "false")
    .config("spark.hadoop.hive.metastore.schema.verification.record.version", "false")
    
    # Disable embedded metastore service
    .config("spark.hadoop.hive.metastore.uris", "")
    .config("spark.hadoop.hive.metastore.local", "false")
    
    # S3 and AWS configurations for reading regular tables
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    
    .getOrCreate()
)


glue_db_name = 'icebox1'

# Create Iceberg tables using the Iceberg catalog
spark.sql(f"""
   create or replace table {iceberg_catalog}.{glue_db_name}.iceberg_test2 
    using iceberg 
    location '{s3_warehouse_path}/iceberg_test2' 
    as
    select 1 as id, 'a' as data
    union all
    select 2 as id, 'b' as data      
""")

spark.sql(f"""
   create or replace table {iceberg_catalog}.{glue_db_name}.iceberg_test3
       using iceberg 
        location '{s3_warehouse_path}/iceberg_test3' 
        as
        select 1 as id, 'c' as data
        union all
        select 3 as id, 'r' as data  
""")

# Read Iceberg table using Iceberg catalog
spark.sql(f"select * from {iceberg_catalog}.{glue_db_name}.iceberg_test limit 5").show()

# Test merge on Iceberg tables
sql = f"""
    merge into {iceberg_catalog}.{glue_db_name}.iceberg_test2 as tgt
        using {iceberg_catalog}.{glue_db_name}.iceberg_test3 as src
            on tgt.id = src.id
    when matched then update set tgt.data = src.data
    when not matched then insert (id, data) values (src.id, src.data)
"""
spark.sql(sql)

print("merge ran")

# Test accessing regular Glue tables using SQL (like in AWS Glue)
print("\n3. Testing access to regular Glue tables using SQL:")
try:
    # Show databases
    print("Available databases:")
    spark.sql("SHOW DATABASES").show()
    
    # Use the database
    spark.sql(f"USE {glue_db_name}")
    
    # Show all tables (both Iceberg and regular)
    print(f"\nAll tables in {glue_db_name}:")
    spark.sql("SHOW TABLES").show()
    
    # Try to query a regular table (if it exists)
    # This simulates how you would access regular Glue tables in AWS Glue
    try:
        print(f"\nTrying to access regular Glue tables:")
        regular_table_query = f"""
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = '{glue_db_name}'
        """
        spark.sql(regular_table_query).show()
    except Exception as e:
        print(f"Information schema query failed (expected for some configurations): {e}")
        
        # Alternative: try to access a specific table if you know it exists
        # Uncomment and modify this if you have a known regular table:
        # spark.sql("SELECT * FROM your_regular_table_name LIMIT 5").show()
        
except Exception as e:
    print(f"Error accessing Glue catalog via SQL: {e}")

print('\n✅ Local Spark environment configured for AWS Glue compatibility!')
print('📝 This setup allows you to:')
print('   - Use Iceberg tables via iceberg_catalog.database.table')
print('   - Access regular Glue tables via database.table (default catalog)')
print('   - Write the same SQL that works in AWS Glue')

spark.stop()