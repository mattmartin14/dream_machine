import os
import duckdb

def main():
    db_name = "c2"
    cn = duckdb.connect(f'md:{db_name}?motherduck_token={os.getenv("MD_TOKEN")}')

    sql = """
        CREATE OR REPLACE TABLE c2.summary_data AS
        SELECT *
        from read_csv_auto('~/concept2/workouts/*summary*.csv')
    """
    cn.execute(sql)

if __name__ == "__main__":
    main()