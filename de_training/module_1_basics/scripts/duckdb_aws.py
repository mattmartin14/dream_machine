import os
import duckdb


def main() -> None:
    bucket = os.getenv("AWS_BUCKET")
    if not bucket:
        raise ValueError("Set AWS_BUCKET before running this script")

    cn = duckdb.connect()

    cn.execute("INSTALL AWS")
    cn.execute("LOAD AWS")
    cn.execute("CREATE OR REPLACE SECRET aws_creds (TYPE S3, PROVIDER CREDENTIAL_CHAIN)")

    cn.execute(
        """
        CREATE OR REPLACE TABLE sales AS
        SELECT 1 AS id, '2024-01-01'::DATE AS sale_date, 100.0 AS amount
        UNION ALL
        SELECT 2, '2024-01-02', 150.0
        UNION ALL
        SELECT 3, '2024-01-03', 200.0
        """
    )

    print('since we are using python, no need for local file copy; we can one shot it')
    cn.execute(f"COPY sales TO 's3://{bucket}/sales.csv'")
    cn.execute(f"COPY sales TO 's3://{bucket}/sales.parquet'")


    print("reading back from s3 as a csv file")
    sql = f"SELECT * FROM read_csv_auto(concat('s3://{bucket}/sales.csv'))"
    cn.sql(sql).show()

    print("reading back from s3 as a parquet file")
    sql = f"SELECT * FROM read_parquet(concat('s3://{bucket}/sales.parquet'))"
    cn.sql(sql).show()

    print("additionally because we are in python, we can pull and push to s3 in a single shot")
    sql = f"""
        COPY (SELECT * FROM read_csv_auto('s3://{bucket}/sales.csv'))
        TO 's3://{bucket}/sales_csv_to_par.parquet'
    """

    cn.execute(sql)

if __name__ == "__main__":
    main()
