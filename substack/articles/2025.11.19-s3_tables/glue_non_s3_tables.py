import boto3
import duckdb
from pyiceberg.catalog import load_catalog
import os


def create_duckdb_cn() -> duckdb.DuckDBPyConnection:
    cn = duckdb.connect()
    cn.execute("install aws; load aws;")
    cn.execute("install iceberg; load iceberg")

    cn.execute("""
        CREATE SECRET s3_creds (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN,
            REGION 'us-east-1'
        )
    """)

    aws_acct_id = os.getenv("AWS_ACCT_ID")

    cn.execute(f"""
       attach '{aws_acct_id}' as glue_catalog (
        TYPE 'iceberg',
        ENDPOINT 'glue.us-east-1.amazonaws.com/iceberg',
        AUTHORIZATION_TYPE 'sigv4',
        support_stage_create FALSE
       )       
              
    """)

    num_rows = 1

    cn.execute(f"""
        CREATE OR REPLACE VIEW v_data_gen AS
        SELECT 
            t.row_id, 
            uuid()::varchar as txn_key,  -- Cast to varchar to avoid binary UUID issues
            current_date as rpt_dt,
            round(random() * 100, 2) as some_val
        FROM generate_series(1, {num_rows}) t(row_id)
    """)

    return cn

def drop_glue_db(db_name):
    client = boto3.client('glue', region_name='us-east-1')

    try:
        client.delete_database(Name=db_name)
    except client.exceptions.EntityNotFoundException:
        pass  

def create_glue_db(db_name):
    client = boto3.client('glue', region_name='us-east-1')
    bucket_name = os.getenv("aws_bucket")
    
    try:
        client.create_database(
            DatabaseInput={
                'Name': db_name,
                'Description': 'Database for Iceberg tables',
                'LocationUri': f's3://{bucket_name}/{db_name}/'
            }
        )
    except client.exceptions.AlreadyExistsException:
        pass  


def create_non_s3_iceberg_table(cn, glue_db_name, table_name):
    catalog = load_catalog(
        "glue",
        **{
            "type": "glue",
            "glue.region": "us-east-1",
            "glue.account-id": os.getenv('AWS_ACCT_ID'),
            "s3.region": "us-east-1"
        }
    )

    # Get data as Arrow table
    duck_df = cn.execute("SELECT * FROM v_data_gen limit 1")
    arrow_table = duck_df.arrow().read_all()
    
    # Drop existing table if it exists
    try:
        catalog.drop_table(f"{glue_db_name}.{table_name}")
    except:
        pass
    
    # Create new table and append data
    ice_table = catalog.create_table(f"{glue_db_name}.{table_name}", schema=arrow_table.schema)
    ice_table.append(arrow_table)

def write_iceberg_non_s3_tables_using_the_duck(cn, glue_db_name, table_name):

    sql = f"""
        insert into glue_catalog.{glue_db_name}.{table_name} (row_id, txn_key, rpt_dt, some_val)
        values (6, 'txn_6', current_date, 42.0),
               (7, 'txn_7', current_date, 84.0)
    """

    cn.execute(sql)

if __name__ == "__main__":
    cn = create_duckdb_cn()
    glue_db_name = "db1"
    table_name = "iceyhot"
    drop_glue_db(glue_db_name)
    create_glue_db(glue_db_name)
    create_non_s3_iceberg_table(cn, glue_db_name, table_name)
    write_iceberg_non_s3_tables_using_the_duck(cn, glue_db_name, table_name)