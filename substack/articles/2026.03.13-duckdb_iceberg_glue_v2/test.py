import os
import duckdb
import boto3

def delete_files_in_s3_path(s3_path):
    s3 = boto3.resource('s3')
    bucket_name, prefix = s3_path.replace("s3://", "").split("/", 1)
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()

def drop_glue_table_if_exists(glue_db_name, table_name):
    glue = boto3.client('glue')
    try:
        glue.delete_table(DatabaseName=glue_db_name, Name=table_name)
    except glue.exceptions.EntityNotFoundException:
        pass
    except Exception as e:
        print(f"Error dropping table: {e}")

def test_create_ice_tbl_ctas():

    s3_path = f's3://{os.getenv("aws_bucket")}/icyhot/ice_tbl_ctas'
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
    
    # needed before drop
    delete_files_in_s3_path(s3_path)

    #adding the purge_requested = false on the attach allows us to drop the table
    drop_glue_table_if_exists(glue_db_name, "ice_tbl_ctas")

    # currently the duck does not appear to support this?
    # can add this  purge_requested false to the attach, but it still does not nuke the table data
    #cn.execute(f"""drop table if exists glue_ice.{glue_db_name}.ice_tbl_ctas """)

    cn.execute(f"""
               create table glue_ice.{glue_db_name}.ice_tbl_ctas
               WITH (
                    'location' = '{s3_path}'
            )
            AS
            SELECT 1 as id, 'foo' as name
            UNION ALL
            SELECT 2 as id, 'bar' as name
        """)
    

    cn.execute(f"delete from glue_ice.{glue_db_name}.ice_tbl_ctas where id = 1")
    cn.execute(f"update glue_ice.{glue_db_name}.ice_tbl_ctas set name = 'baz' where id = 2")
    cn.execute(f"insert into glue_ice.{glue_db_name}.ice_tbl_ctas values (3, 'quack')")

    cn.sql(f"select * from glue_ice.{glue_db_name}.ice_tbl_ctas").show()

if __name__ == "__main__":
    test_create_ice_tbl_ctas()