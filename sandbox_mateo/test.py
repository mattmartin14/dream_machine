import duckdb
import os


def main():

    cn = duckdb.connect()

    cn.sql("select 1 as 'blah'").show()


    cn.execute("create table x as select 1 as id, 'foo' as name")

    cn.execute("copy x to 'test.csv'")
    cn.execute("copy x to  'test.parquet'")

    cn.execute("create or replace table y as select * from read_csv_auto('test.csv')")

    cn.sql("from y").show() 

    sql = """
         create or replace table z as
         select 1 as id, 'foo' as name
            union all
            select 2 as id, 'bar' as name
            
    """
    cn.execute(sql)

    cn.execute("create view v_z as select * from z where id = 1")

    cn.sql("from v_z").show()

    for i in range(10):
        print(f"{i}")


    aws_bucket = os.getenv("AWS_BUCKET")
    print(aws_bucket)

if __name__ == "__main__":
    main()