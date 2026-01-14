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
        create or replace table duckland.main.channel_loc_qty
        as
        select oh.channel
            , iv.location
            , sum(od.qty) as tot_qty
        from local_db.v_order_header oh
            join local_db.v_order_detail as od 
                on oh.order_id = od.order_id
            join duckland.main.inventory as iv 
                on od.sku = iv.sku
        group by all
    """
    cn.execute(sql)

    elapsed = time.perf_counter() - t0
    print(f"test_1 elapsed: {elapsed:.3f}s")

def test_2(cn: duckdb.DuckDBPyConnection):

    t0 = time.perf_counter()

    sql = """
        create or replace table duckland.main.order_payments_summary
        as
        select oh.payment_method
            ,sum(od.qty) as tot_qty
            ,sum(od.line_total_cents)/100.0 as tot_sales_dollars
        from local_db.v_order_header oh
            left join local_db.v_order_detail od on oh.order_id = od.order_id
        group by all
    """
    cn.execute(sql)

    elapsed = time.perf_counter() - t0
    print(f"test_2 elapsed: {elapsed:.3f}s")

if __name__ == "__main__":
    cn = create_md_db_conn()
    test_1(cn)
    test_2(cn)