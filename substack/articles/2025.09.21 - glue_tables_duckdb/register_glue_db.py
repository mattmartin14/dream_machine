import boto3
import duckdb


def register_glue_tables(cn: duckdb.DuckDBPyConnection, glue_db_name: str):
    """
        Loops through all tables in a glue database and creates corresponding DuckDB views.
        Supports Iceberg, Parquet, and CSV table formats.
    """

    cn.execute(f"create schema if not exists {glue_db_name}")

    glue_client = boto3.client('glue', region_name='us-east-1')
    response = glue_client.get_tables(DatabaseName=glue_db_name)
    tables = response['TableList']

    #iterate through the table list
    for table in tables:
        glue_table_name = table['Name']
        view_name = f"{glue_db_name}.{glue_table_name}"

        response = glue_client.get_table(DatabaseName=glue_db_name, Name=glue_table_name)
        table_metadata = response['Table']
        
        # what is the table type?
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
                table_type = 'PARQUET'
                parquet_path = location if location.endswith('.parquet') else f"{location}/*.parquet"
                cn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{parquet_path}')")
                
            elif 'lazy' in serde_lib.lower() or 'csv' in serde_lib.lower():
                table_type = 'CSV'
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

        print(f"registered glue table {glue_table_name} as {view_name} (type: {table_type})")
