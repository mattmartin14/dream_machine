#!/usr/bin/env python3
"""
Test script demonstrating the AWS Glue-compatible local Spark environment.

This approach focuses on what actually works:
1. Iceberg tables work via dedicated catalog (both local and AWS Glue)
2. Regular Glue tables accessed differently:
   - Local: boto3 discovery + S3 direct access
   - AWS Glue: Standard SQL queries
"""

from setup_spark_env import setup_spark_env, create_iceberg_table_sql, list_glue_tables_boto3
import os

def main():
    print("🚀 Testing AWS Glue-compatible Spark environment")
    print("=" * 60)
    
    # Setup Spark environment (auto-detects local vs AWS Glue)
    spark = setup_spark_env()
    
    # Test Iceberg functionality
    print("\n1. Testing Iceberg table operations:")
    iceberg_catalog = "iceberg_catalog"
    database = "icebox1" 
    
    try:
        # Create an Iceberg table
        create_sql = create_iceberg_table_sql(
            spark, 
            catalog=iceberg_catalog, 
            database=database, 
            table_name="test_local_env",
            s3_path="s3a://mattmartindatascience/warehouse/test_local_env"
        )
        
        print(f"Creating Iceberg table...")
        spark.sql(create_sql)
        print("✅ Iceberg table created successfully!")
        
        # Query the Iceberg table
        print("🔍 Querying Iceberg table:")
        result = spark.sql(f"SELECT * FROM {iceberg_catalog}.{database}.test_local_env LIMIT 3")
        result.show()
        
        # Test Iceberg merge functionality
        print("🔄 Testing Iceberg MERGE operation:")
        merge_sql = f"""
            MERGE INTO {iceberg_catalog}.{database}.test_local_env as target
            USING (SELECT 2 as id, 'updated' as data, current_timestamp() as created_at) as source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET data = source.data
            WHEN NOT MATCHED THEN INSERT (id, data, created_at) VALUES (source.id, source.data, source.created_at)
        """
        spark.sql(merge_sql)
        print("✅ MERGE operation completed!")
        
        # Show updated results
        print("📊 Updated table contents:")
        spark.sql(f"SELECT * FROM {iceberg_catalog}.{database}.test_local_env ORDER BY id").show()
        
    except Exception as e:
        print(f"❌ Iceberg test failed: {e}")
    
    # Test regular Glue table discovery
    print("\n2. Testing regular Glue table access:")
    
    # Check if we're in AWS Glue or local
    is_aws_glue = 'glue_context' in globals() or os.environ.get('AWS_GLUE_JOB_NAME')
    
    if is_aws_glue:
        print("🔍 AWS Glue environment detected - using standard SQL")
        try:
            # In AWS Glue, you can query regular tables directly
            spark.sql("SHOW DATABASES").show()
            # Example: spark.sql("SELECT * FROM database.regular_table LIMIT 5").show()
        except Exception as e:
            print(f"❌ AWS Glue table access failed: {e}")
    else:
        print("🔍 Local environment detected - using boto3 + S3 access")
        try:
            # In local environment, use boto3 for discovery
            tables_info = list_glue_tables_boto3(database)
            
            print("📋 Available tables in Glue catalog:")
            print(f"   Iceberg tables: {len(tables_info['iceberg_tables'])}")
            for table in tables_info['iceberg_tables']:
                print(f"     - {table['name']}")
            
            print(f"   Regular tables: {len(tables_info['regular_tables'])}")
            for table in tables_info['regular_tables']:
                print(f"     - {table['name']} ({table['location']})")
            
            # Demonstrate how you'd read a regular table via S3
            if tables_info['regular_tables']:
                regular_table = tables_info['regular_tables'][0]
                s3_location = regular_table['location']
                
                if s3_location and s3_location != 'Unknown' and s3_location.startswith('s3'):
                    print(f"\n💡 To read regular table '{regular_table['name']}' locally:")
                    print(f"   df = spark.read.parquet('{s3_location}')")
                    print(f"   df.show()")
                    
                    # Uncomment to actually read (if table exists and has data):
                    # try:
                    #     df = spark.read.parquet(s3_location)
                    #     print("📖 Sample data from regular table:")
                    #     df.show(3)
                    # except Exception as e:
                    #     print(f"⚠️  Could not read table (may be empty or access issue): {e}")
            
        except Exception as e:
            print(f"❌ Table discovery failed: {e}")
    
    print("\n" + "=" * 60)
    print("✅ Test completed!")
    print("\n📝 Summary:")
    print("   ✅ Iceberg tables: Use iceberg_catalog.database.table (works everywhere)")
    print("   ✅ Regular Glue tables:")
    print("      - AWS Glue: Use database.table (standard SQL)")
    print("      - Local: Use boto3 discovery + spark.read.parquet(s3_path)")
    print("\n🎯 This setup provides AWS Glue compatibility with minimal code changes!")
    
    # Clean up
    spark.stop()

if __name__ == "__main__":
    main()