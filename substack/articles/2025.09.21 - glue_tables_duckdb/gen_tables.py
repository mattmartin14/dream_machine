#!/usr/bin/env python3
"""
Generate test data and create tables in various formats:
- DuckDB generated data
- Export to S3 as Parquet and CSV
- Create Iceberg table via PyIceberg
- Create Glue catalog tables for Parquet and CSV

This script demonstrates integration between DuckDB, PyIceberg, and AWS Glue.
"""

import boto3
import duckdb
from pyiceberg.catalog import load_catalog
import os


def setup_aws_credentials():
    """Get AWS credentials from boto3 session for use with DuckDB and PyIceberg."""
    session = boto3.Session()
    credentials = session.get_credentials()
    
    if not credentials:
        raise Exception("No AWS credentials found. Make sure you're logged in via AWS CLI/SSO.")
    
    return session, credentials


def setup_pyiceberg_catalog(credentials):
    """Create PyIceberg Glue catalog with explicit AWS credentials."""
    catalog = load_catalog(
        "glue",
        **{
            "type": "glue",
            "glue.region": "us-east-1",
            "glue.account-id": os.getenv('aws_account_nbr'),
            "s3.access-key-id": credentials.access_key,
            "s3.secret-access-key": credentials.secret_key,
            "s3.session-token": credentials.token if credentials.token else "",
            "s3.region": "us-east-1"
        }
    )
    return catalog


def setup_duckdb_connection(credentials):
    """Create DuckDB connection and configure S3 access."""
    cn = duckdb.connect()
    
    # Install and load AWS extension
    cn.execute("INSTALL AWS; LOAD AWS")
    
    # Drop existing secret if it exists
    cn.execute("DROP SECRET IF EXISTS s3_creds")
    
    # Create S3 secret with session token
    cn.execute(f"""
        CREATE SECRET s3_creds (
            TYPE S3,
            PROVIDER CONFIG,
            KEY_ID '{credentials.access_key}',
            SECRET '{credentials.secret_key}',
            SESSION_TOKEN '{credentials.token}',
            REGION 'us-east-1'
        )
    """)
    
    return cn

def generate_test_data(cn, num_rows=100):
    """Generate test data using DuckDB."""
    cn.execute(f"""
        CREATE OR REPLACE VIEW v_data_gen AS
        SELECT 
            t.row_id, 
            uuid()::varchar as txn_key,  -- Cast to varchar to avoid binary UUID issues
            current_date as rpt_dt,
            round(random() * 100, 2) as some_val
        FROM generate_series(1, {num_rows}) t(row_id)
    """)
    
    return True


def export_to_s3(cn, bucket=None):
    """Export data to S3 in Parquet and CSV formats."""
    if bucket is None:
        bucket = os.getenv('aws_bucket')
    
    # Export to Parquet
    parquet_path = f"s3://{bucket}/duckdb/data_gen_parquet/data.parquet"
    cn.sql(f"COPY v_data_gen TO '{parquet_path}' (FORMAT PARQUET)")
    
    # Export to CSV with headers
    csv_path = f"s3://{bucket}/duckdb/data_gen_csv/data.csv"
    cn.sql(f"COPY v_data_gen TO '{csv_path}' (FORMAT CSV, HEADER TRUE)")
    
    return parquet_path, csv_path


def create_iceberg_table(catalog, cn):
    
    # Get data as Arrow table
    duck_df = cn.execute("SELECT * FROM v_data_gen")
    arrow_table = duck_df.arrow().read_all()
    
    # Drop existing table if it exists
    try:
        catalog.drop_table("icebox1.iceberg_test")
    except:
        pass
    
    # Create new table and append data
    ice_table = catalog.create_table("icebox1.iceberg_test", schema=arrow_table.schema)
    ice_table.append(arrow_table)
    
    return ice_table


def create_glue_tables(glue_client, bucket=None):
    """Create Glue catalog tables for Parquet and CSV data."""
    if bucket is None:
        bucket = os.getenv('aws_bucket')
    
    print("Creating Glue catalog tables...")
    
    # Define common column schema
    columns = [
        {'Name': 'row_id', 'Type': 'bigint'},
        {'Name': 'txn_key', 'Type': 'string'},
        {'Name': 'rpt_dt', 'Type': 'date'},
        {'Name': 'some_val', 'Type': 'double'}
    ]
    
    #parquet table
    try:
        # Delete existing table first
        try:
            glue_client.delete_table(DatabaseName='icebox1', Name='parquet_test')
        except:
            pass
        
        glue_client.create_table(
            DatabaseName='icebox1',
            TableInput={
                'Name': 'parquet_test',
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': f's3://{bucket}/duckdb/data_gen_parquet/',
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        
    except Exception as e:
        print(f"Error creating Parquet table: {e}")
    
    # Create CSV table
    try:
        # Delete existing table first
        try:
            glue_client.delete_table(DatabaseName='icebox1', Name='csv_test')
        except:
            pass
        
        glue_client.create_table(
            DatabaseName='icebox1',
            TableInput={
                'Name': 'csv_test',
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': f's3://{bucket}/duckdb/data_gen_csv/',
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {
                            'field.delim': ',',
                            'skip.header.line.count': '1'
                        }
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        
    except Exception as e:
        print(f"Error creating CSV table: {e}")


def setup_notebook_environment():
    """
    Convenience function to set up all necessary components for notebook usage.
    Returns configured DuckDB connection, PyIceberg catalog, and Glue client.
    """
    try:
        # Setup AWS credentials
        session, credentials = setup_aws_credentials()
        
        # Setup PyIceberg catalog
        catalog = setup_pyiceberg_catalog(credentials)
        
        # Setup DuckDB connection
        cn = setup_duckdb_connection(credentials)
        
        # Setup Glue client
        glue_client = boto3.client('glue', region_name='us-east-1')
        
        return cn, catalog, glue_client
    
    except Exception as e:
        print(f"❌ Failed to setup notebook environment: {e}")
        raise


def main():
    """Main execution function."""
    
    try:
        # Setup environment 
        cn, catalog, glue_client = setup_notebook_environment()
        
        # Generate test data
        generate_test_data(cn, num_rows=100)
        
        # Export to S3
        parquet_path, csv_path = export_to_s3(cn)
        
        # Create Iceberg table
        ice_table = create_iceberg_table(catalog, cn)
        
        # Create Glue catalog tables
        create_glue_tables(glue_client)

        print("process complete")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()