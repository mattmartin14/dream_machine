
import duckdb

def gen_local_db():
    cn = duckdb.connect('~/test_dbs/duckland_local.db')
    cn.execute("""
        create or replace view v_order_header as select * from read_csv_auto('~/test_data/orders_header*.csv');
        create or replace view v_order_detail as select * from read_csv_auto('~/test_data/orders_detail*.csv');  
    """)

if __name__ == "__main__":
    gen_local_db()