from ds_order_data import generate_orders
from ds_store_geo import generate_store_geography
from build_md_table import load_md_table
from build_s3_cat import build_s3_tables
from integration_test import run_all

if __name__ == "__main__":
    order_cnt = 10_000
    generate_orders(order_cnt)
    print("generated order data")
    generate_store_geography(100)
    print("generated store geography")
    build_s3_tables()
    print("built s3 tables")
    load_md_table()
    print("loaded md table")
    run_all()
    print("completed integration test")

