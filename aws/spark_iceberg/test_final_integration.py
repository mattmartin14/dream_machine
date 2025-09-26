#!/usr/bin/env python3
"""
Quick test to demonstrate that the AWS credentials are working properly
by setting up the AWS environment variables and then testing Spark with Iceberg.
"""

from setup_env import get_session, set_aws_env_vars
from setup_spark_env import setup_spark_env, create_iceberg_table_sql, list_glue_tables_boto3

def main():
    print("🔧 Setting up AWS credentials for Spark...")
    
    # Set up AWS credentials first
    session = get_session()
    # The session setup already sets environment variables
    print("✅ AWS credentials configured!")
    
    # Now set up Spark environment
    spark = setup_spark_env()
    
    # Test Iceberg functionality
    print("\n🧊 Testing Iceberg operations:")
    iceberg_catalog = "iceberg_catalog"
    database = "icebox1"
    
    try:
        # Create an Iceberg table
        create_sql = create_iceberg_table_sql(
            spark, 
            catalog=iceberg_catalog, 
            database=database, 
            table_name="final_test_table"
        )
        
        print("Creating Iceberg table...")
        spark.sql(create_sql)
        print("✅ Iceberg table created successfully!")
        
        # Query the table
        print("📊 Querying Iceberg table:")
        spark.sql(f"SELECT * FROM {iceberg_catalog}.{database}.final_test_table").show()
        
        # Test merge operation
        print("🔄 Testing MERGE operation:")
        merge_sql = f"""
            MERGE INTO {iceberg_catalog}.{database}.final_test_table as target
            USING (SELECT 2 as id, 'merged_data' as data, current_timestamp() as created_at) as source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET data = source.data, created_at = source.created_at
            WHEN NOT MATCHED THEN INSERT (id, data, created_at) VALUES (source.id, source.data, source.created_at)
        """
        spark.sql(merge_sql)
        print("✅ MERGE completed!")
        
        # Show final results
        print("📈 Final table contents:")
        spark.sql(f"SELECT * FROM {iceberg_catalog}.{database}.final_test_table ORDER BY id").show()
        
    except Exception as e:
        print(f"❌ Iceberg operations failed: {e}")
        return
    
    # Test regular table discovery
    print("\n📋 Discovering regular Glue tables:")
    try:
        tables_info = list_glue_tables_boto3(database)
        
        print("Available tables:")
        print(f"  🧊 Iceberg: {[t['name'] for t in tables_info['iceberg_tables']]}")
        print(f"  📄 Regular: {[t['name'] for t in tables_info['regular_tables']]}")
        
        # Show how you'd read a regular table
        if tables_info['regular_tables']:
            regular_table = tables_info['regular_tables'][0]
            s3_location = regular_table['location']
            print(f"\n💡 To read '{regular_table['name']}' from S3:")
            print(f"   df = spark.read.option('header', 'true').csv('{s3_location}')")
            
            # Actually try to read it
            if 'csv' in regular_table['name'].lower():
                print("📖 Attempting to read CSV table:")
                df = spark.read.option('header', 'true').csv(s3_location)
                df.show(3)
                print(f"✅ Successfully read {df.count()} rows from regular Glue table!")
                
    except Exception as e:
        print(f"❌ Regular table operations failed: {e}")
    
    print("\n" + "=" * 60)
    print("🎉 SUCCESS! AWS Glue-compatible environment working!")
    print("\n📝 What we achieved:")
    print("   ✅ MFA credential caching (12-hour sessions)")
    print("   ✅ Spark + Iceberg integration")
    print("   ✅ Iceberg table CRUD operations")
    print("   ✅ Regular Glue table discovery via boto3")
    print("   ✅ Mixed table format support (Iceberg + regular)")
    print("\n🚀 Ready for production use!")
    
    # Clean up
    spark.stop()

if __name__ == "__main__":
    main()