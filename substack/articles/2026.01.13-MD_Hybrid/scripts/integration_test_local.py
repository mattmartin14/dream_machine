import duckdb
import os
import time

def create_md_db_conn() -> duckdb.DuckDBPyConnection:
    db_name = "duckland"
    cn = duckdb.connect(f'md:{db_name}?motherduck_token={os.getenv("MD_TOKEN")}')
    cn.execute("attach '~/test_dbs/duckland_local.db' as local_db")
    return cn

def test_1(cn: duckdb.DuckDBPyConnection):

    t0 = time.perf_counter()

    sql = """
        create or replace table local_db.ord_stats_by_category
        as
        select iv.category
            ,count(distinct oh.order_id) as num_orders
            ,count(distinct od.line_id) as num_order_lines
            ,sum(od.qty) as total_units_sold
            ,sum(od.line_total_cents)/100.0 as total_sales_dollars
        from local_db.v_order_header as oh
            inner join local_db.v_order_detail as od 
                on oh.order_id = od.order_id
            inner join duckland.main.inventory as iv 
                on od.sku = iv.sku
        group by all
    """

    cn.execute(sql) 
    elapsed = time.perf_counter() - t0
    print(f"test_1 elapsed: {elapsed:.3f}s")

if __name__ == "__main__":
    cn = create_md_db_conn()
    test_1(cn)