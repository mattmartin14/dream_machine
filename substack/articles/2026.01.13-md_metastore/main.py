from gen_order_data import generate_orders
from store_geography import generate_store_geography
from build_md_table import load_md_table
from build_s3_cat import build_s3_tables
from integration_test import integration_test_1

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
    integration_test_1()
    print("completed integration test")

