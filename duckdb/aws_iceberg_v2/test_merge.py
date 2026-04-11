import os
import duckdb
import boto3

def delete_files_in_s3_path(s3_path):
    s3 = boto3.resource('s3')
    bucket_name, prefix = s3_path.replace("s3://", "").split("/", 1)
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()

def test_create_ice_tbl_merge():

    tbl_nm = "ice_tbl_target"

    s3_path = f's3://{os.getenv("aws_bucket")}/icyhot/{tbl_nm}'
    glue_db_name = "db1"

    cn = duckdb.connect()
    cn.execute("install iceberg; install aws; load iceberg; load aws;")
    cn.execute("create secret aws_creds(type s3, provider credential_chain)")
    cn.execute(f"""
               attach or replace '{os.getenv("aws_acct_nbr")}' as glue_ice (
                TYPE ICEBERG,
                ENDPOINT_TYPE 'GLUE',
                purge_requested false
               )
        """)
    
    # purge s3 directory
    delete_files_in_s3_path(s3_path)

    cn.execute(f"""drop table if exists glue_ice.{glue_db_name}.{tbl_nm} """)

    cn.execute(f"""
               create table glue_ice.{glue_db_name}.{tbl_nm}
               WITH (
                    'location' = '{s3_path}'
            )
            AS
            SELECT 1 as id, 'foo' as name
            UNION ALL
            SELECT 2 as id, 'bar' as name
        """)
    
    cn.execute("""
        create or replace table src as
            SELECT 1 as id, 'foo' as name
            UNION ALL
            SELECT 2 as id, 'bar' as name
            UNION ALL
            SELECT 3 as id, 'baz' as name
    """)

    cn.execute(f"""
               merge into glue_ice.{glue_db_name}.{tbl_nm} target
               using src
               on target.id = src.id
               when matched then update set name = src.name || '_updated'
               when not matched then insert (id, name) values (src.id, src.name || '_new')
        """)

    cn.sql(f"select * from glue_ice.{glue_db_name}.{tbl_nm}").show()

if __name__ == "__main__":
    test_create_ice_tbl_merge()