import duckdb

def process_order_header(cn: duckdb.DuckDBPyConnection) -> int:
    sql = """
        COPY (
            SELECT o_orderkey, count(distinct o_custkey) as num_customers
            FROM orders
            GROUP BY ALL
        ) to order_agg.parquet
    """
    cn.execute(sql)

    return int(cn.sql("select * from 'order_agg.parquet'").fetchall()[0][0])

def process_order_line(cn: duckdb.DuckDBPyConnection) -> int:
    sql = """
        COPY (
            SELECT l_orderkey, count(*) as num_lines
            FROM lineitem
            GROUP BY ALL
        ) to line_agg.parquet
    """
    cn.execute(sql)

    return int(cn.sql("select * from 'line_agg.parquet'").fetchall()[0][0]) 

def main():

    cn = duckdb.connect()
    cn.execute("install tpch; load tpch; call dbgen(sf=1)")
    row_headers_affected = process_order_header(cn)
    print(f'order headers processed. Total rows: {row_headers_affected}')
    row_lines_affected = process_order_line(cn)
    print(f'order lines processed. Total rows: {row_lines_affected}')

if __name__ == "__main__":
    main()

