#!/usr/bin/env python3
"""
Query Glue Table Module

This module provides functionality to create DuckDB views from AWS Glue tables
of any type (Iceberg, Parquet, CSV). It automatically detects the table type
and creates the appropriate DuckDB view for unified querying.
"""

def create_duckdb_view_from_glue_table(db_name, table_name, glue_client, catalog, cn):
    """
    Create a DuckDB view for any AWS Glue table (Iceberg, Parquet, or CSV).
    
    This function automatically detects the table type from Glue metadata and creates 
    the appropriate DuckDB view:
    - Iceberg tables: Uses PyIceberg to load and register with DuckDB
    - Parquet tables: Uses DuckDB's read_parquet() function
    - CSV tables: Uses DuckDB's read_csv() function with proper delimiters
    
    Args:
        db_name (str): Glue database name
        table_name (str): Glue table name  
        glue_client: boto3 Glue client instance
        catalog: PyIceberg catalog instance
        cn: DuckDB connection instance
    
    Returns:
        str: Name of the created view (v_{table_name})
        
    Raises:
        ValueError: If table format is not supported
        Exception: If table creation fails
    """
    view_name = f"v_{table_name}"
    
    # Get table metadata from Glue
    response = glue_client.get_table(DatabaseName=db_name, Name=table_name)
    table_metadata = response['Table']
    
    # Check if it's an Iceberg table
    table_type = table_metadata.get('Parameters', {}).get('table_type', '').upper()
    
    if table_type == 'ICEBERG':
        # Handle Iceberg table
        #print(f"📊 Detected Iceberg table: {db_name}.{table_name}")
        ice_table = catalog.load_table(f"{db_name}.{table_name}")
        cn.register(table_name, ice_table.scan().to_arrow())
        cn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {table_name}")
        
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
    
    return view_name
