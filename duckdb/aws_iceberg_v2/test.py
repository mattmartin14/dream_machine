import os
import duckdb

def test_create_ice_tbl():

    s3_path = f's3://{os.getenv("aws_bucket")}/icyhot/ice_tbl'
    glue_db_name = "db1"

    cn = duckdb.connect()
    cn.execute("install iceberg; install aws; load iceberg; load aws;")
    cn.execute("create secret aws_creds(type s3, provider credential_chain)")
    cn.execute(f"""
               attach or replace '{os.getenv("aws_acct_nbr")}' as glue_ice (
                TYPE ICEBERG,
                ENDPOINT_TYPE 'GLUE'
               )
        """)

    cn.execute(f"""
               create table glue_ice.{glue_db_name}.ice_tbl (id int, name varchar) 
               WITH (
                    'location' = '{s3_path}'
            )
        """)
    
    cn.execute(f"insert into glue_ice.{glue_db_name}.ice_tbl values (1, 'foo'), (2, 'bar')")

    cn.sql(f"select * from glue_ice.{glue_db_name}.ice_tbl").show()

if __name__ == "__main__":
    test_create_ice_tbl()