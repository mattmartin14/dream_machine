#!/usr/bin/env python3
"""
Query Glue Table Module

This module provides functionality to create DuckDB views from AWS Glue tables
of any type (Iceberg, Parquet, CSV). It automatically detects the table type
and creates the appropriate DuckDB view for unified querying.
"""
import boto3
import duckdb
import os
#from pyiceberg.catalog import load_catalog

def attach_catalog(cn, catalog_name):
    """Attach the Iceberg catalog to the DuckDB connection."""
    cn.execute(f"""
        create schema if not exists {catalog_name};
    """)

def register_table(cn, catalog_name, glue_db_name, glue_table_name):

    view_name = f"{catalog_name}.{glue_table_name}"

    # Get table metadata from Glue
    glue_client = boto3.client('glue', region_name='us-east-1')
    response = glue_client.get_table(DatabaseName=glue_db_name, Name=glue_table_name)
    table_metadata = response['Table']
    
    # Check if it's an Iceberg table
    table_type = table_metadata.get('Parameters', {}).get('table_type', '').upper()
    
    if table_type == 'ICEBERG':
        iceberg_metadata_path = table_metadata['Parameters']['metadata_location']
        cn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM iceberg_scan('{iceberg_metadata_path}')")
        
    else:
        # Handle regular Glue tables (Parquet/CSV)
        storage_desc = table_metadata['StorageDescriptor']
        location = storage_desc['Location'].rstrip('/')
        serde_lib = storage_desc['SerdeInfo']['SerializationLibrary']
        
        if 'parquet' in serde_lib.lower():
            # Parquet table
            #print(f"📄 Detected Parquet table: {db_name}.{table_name}")
            parquet_path = location if location.endswith('.parquet') else f"{location}/*.parquet"
            cn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{parquet_path}')")
            
        elif 'lazy' in serde_lib.lower() or 'csv' in serde_lib.lower():
            # CSV table
            #print(f"📝 Detected CSV table: {db_name}.{table_name}")
            serde_params = storage_desc['SerdeInfo'].get('Parameters', {})
            delimiter = serde_params.get('field.delim', ',')
            skip_header = serde_params.get('skip.header.line.count', '0')
            header_option = "true" if skip_header == '1' else "false"
            
            csv_path = location if location.endswith('.csv') else f"{location}/*.csv"
            cn.execute(f"""
                CREATE OR REPLACE VIEW {view_name} AS 
                SELECT * FROM read_csv('{csv_path}', delim='{delimiter}', header={header_option})
            """)
        else:
            raise ValueError(f"Unsupported table format. SerDe: {serde_lib}")
    
