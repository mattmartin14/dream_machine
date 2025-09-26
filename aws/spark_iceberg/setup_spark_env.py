import os
from pyspark.sql import SparkSession

# Conditional import for local AWS setup
try:
    from setup_env import get_session, set_aws_env_vars
    HAS_LOCAL_AWS_SETUP = True
except ImportError:
    HAS_LOCAL_AWS_SETUP = False
    print("⚠️  setup_env not available - AWS credentials must be configured externally")

def setup_spark_env():
    """
    Setup Spark environment that works both locally and in AWS Glue.
    
    Detects the environment and configures Spark accordingly:
    - Local: Full configuration with Iceberg + AWS integration
    - AWS Glue: Uses the existing Glue Spark session
    
    Returns:
        SparkSession: Configured Spark session
    """
    
    # Detect if we're running in AWS Glue
    is_aws_glue = 'glue_context' in globals() or os.environ.get('AWS_GLUE_JOB_NAME')
    
    if is_aws_glue:
        # In AWS Glue - use the existing Glue context
        print("🔍 Detected AWS Glue environment")
        try:
            from awsglue.context import GlueContext
            from pyspark.context import SparkContext
            
            # Use existing Glue Spark session if available
            if 'spark' in globals():
                return globals()['spark']
            
            # Or create from Glue context
            sc = SparkContext.getOrCreate()
            glueContext = GlueContext(sc)
            return glueContext.spark_session
            
        except ImportError:
            # Fallback if Glue libraries aren't available
            return SparkSession.builder.appName("GlueCompatible").getOrCreate()
    
    else:
        # Local environment - full configuration
        print("🔍 Detected local environment - setting up Spark with Iceberg + AWS integration")
        
        # Setup AWS credentials (if available)
        if HAS_LOCAL_AWS_SETUP:
            session = get_session()
            # get_session() already sets the AWS environment variables
        else:
            print("⚠️  Using existing AWS credentials from environment")
        
        # Configuration for local Spark that mimics AWS Glue
        s3_warehouse_path = "s3a://matt-sbx-bucket-1-us-east-1/icehouse/"  # Use your actual valid bucket path
        iceberg_catalog = "iceberg_catalog"
        
        spark = (SparkSession.builder
            .appName("LocalGlueCompatible")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            
            # Iceberg + AWS dependencies
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
            
            # Iceberg catalog configuration
            .config(f"spark.sql.catalog.{iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{iceberg_catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{iceberg_catalog}.warehouse", s3_warehouse_path)
            .config(f"spark.sql.catalog.{iceberg_catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            
            # S3 configuration
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
            
            # Use in-memory catalog for default catalog (avoids Hive/Derby issues)
            .config("spark.sql.catalogImplementation", "in-memory")
            
            .getOrCreate()
        )
        
        print("✅ Local Spark environment ready!")
        print(f"📋 Iceberg catalog: {iceberg_catalog}")
        print("🔗 Use iceberg_catalog.database.table for Iceberg tables")
        print("🗂️  For regular Glue tables, use boto3 or DataFrameReader with S3 paths")
        
        return spark

def create_iceberg_table_sql(spark, catalog="iceberg_catalog", database="icebox1", table_name="test_table", s3_path=None):
    """
    Helper function to create Iceberg tables with consistent SQL.
    
    Args:
        spark: SparkSession
        catalog: Iceberg catalog name
        database: Database name  
        table_name: Table name
        s3_path: Optional S3 path for table location
        
    Returns:
        str: SQL statement to create the table
    """
    if not s3_path:
        s3_path = f"s3a://matt-sbx-bucket-1-us-east-1/icehouse/{table_name}"
    
    sql = f"""
        CREATE OR REPLACE TABLE {catalog}.{database}.{table_name} 
        USING ICEBERG 
        LOCATION '{s3_path}' 
        AS 
        SELECT 1 as id, 'sample' as data, current_timestamp() as created_at
    """
    
    return sql

def read_regular_glue_table_via_s3(spark, s3_path, format="parquet"):
    """
    Helper function to read regular (non-Iceberg) Glue tables via S3.
    
    This mimics how you'd access regular tables when Hive metastore 
    integration is challenging in local environments.
    
    Args:
        spark: SparkSession
        s3_path: S3 path to the table data
        format: File format (parquet, delta, etc.)
        
    Returns:
        DataFrame
    """
    return spark.read.format(format).load(s3_path)

def list_glue_tables_boto3(database_name):
    """
    Helper function to list tables in a Glue database using boto3.
    
    This is useful for discovering tables when Hive metastore 
    integration is not available locally.
    
    Args:
        database_name: Name of the Glue database
        
    Returns:
        dict: Dictionary with table information
    """
    import boto3
    
    glue_client = boto3.client('glue')
    
    try:
        response = glue_client.get_tables(DatabaseName=database_name)
        
        tables_info = {
            'iceberg_tables': [],
            'regular_tables': []
        }
        
        for table in response['TableList']:
            table_info = {
                'name': table['Name'],
                'location': table.get('StorageDescriptor', {}).get('Location', 'Unknown'),
                'input_format': table.get('StorageDescriptor', {}).get('InputFormat', 'Unknown')
            }
            
            # Classify tables
            if table.get('Parameters', {}).get('table_type') == 'ICEBERG':
                tables_info['iceberg_tables'].append(table_info)
            else:
                tables_info['regular_tables'].append(table_info)
        
        return tables_info
        
    except Exception as e:
        print(f"Error listing tables: {e}")
        return {'iceberg_tables': [], 'regular_tables': []}

# Example usage patterns
def demo_usage():
    """
    Demonstrates how to use the setup in both environments.
    """
    
    # Setup Spark (works in both local and AWS Glue)
    spark = setup_spark_env()
    
    # Iceberg operations (work the same in both environments)
    iceberg_catalog = "iceberg_catalog" 
    database = "icebox1"
    
    # Create Iceberg table
    create_sql = create_iceberg_table_sql(spark, iceberg_catalog, database, "demo_table")
    spark.sql(create_sql)
    
    # Query Iceberg table
    spark.sql(f"SELECT * FROM {iceberg_catalog}.{database}.demo_table LIMIT 5").show()
    
    # For regular Glue tables, the approach differs by environment:
    # Local: Use boto3 + S3 direct access
    # AWS Glue: Use normal SQL against default catalog
    
    # Check if we're in AWS Glue
    is_aws_glue = 'glue_context' in globals() or os.environ.get('AWS_GLUE_JOB_NAME')
    
    if is_aws_glue:
        # In AWS Glue - can use SQL directly
        # spark.sql("SELECT * FROM database.regular_table LIMIT 5").show()
        pass
    else:
        # In local - use boto3 discovery + S3 direct access
        tables_info = list_glue_tables_boto3(database)
        print("📊 Available tables:")
        print(f"  Iceberg: {[t['name'] for t in tables_info['iceberg_tables']]}")
        print(f"  Regular: {[t['name'] for t in tables_info['regular_tables']]}")
        
        # Read a regular table via S3 (if available)
        if tables_info['regular_tables']:
            regular_table = tables_info['regular_tables'][0]
            s3_location = regular_table['location']
            if s3_location and s3_location != 'Unknown':
                print(f"📖 Reading {regular_table['name']} from {s3_location}")
                # df = read_regular_glue_table_via_s3(spark, s3_location)
                # df.show(5)

if __name__ == "__main__":
    demo_usage()