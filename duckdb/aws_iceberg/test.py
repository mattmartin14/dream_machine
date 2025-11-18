import duckdb
import os

def create_ice_table():

    cn = duckdb.connect()
    cn.execute("install aws; load aws;")
    cn.execute("install iceberg; load iceberg")

    cn.execute("""
       create or replace secret aws_conn (
               TYPE s3,
               PROVIDER credential_chain
               )        
    """)

    aws_acct_id = os.getenv("AWS_ACCT_ID")
    bucket_name = os.getenv("aws_bucket")

    cn.execute(f"""
       attach '{aws_acct_id}' as glue_catalog (
        TYPE 'iceberg',
        ENDPOINT 'glue.us-east-1.amazonaws.com/iceberg',
        AUTHORIZATION_TYPE 'sigv4',
        support_stage_create FALSE
       )       
              
    """)


    # doesn't support it yet
    #sql = f"""create table glue_catalog.db1.iceyhot2 (id bigint,name string)"""

    #cn.execute(sql)



    cn.sql("show all tables").show()
    print('test')

    cn.execute("insert into glue_catalog.db1.iceyhot values (1, 'alice'), (2, 'bob')")

    cn.sql(f"select * from glue_catalog.db1.iceyhot").show()

if __name__ == "__main__":
    create_ice_table()