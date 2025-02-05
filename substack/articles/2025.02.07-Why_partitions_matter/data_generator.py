import polars as pl
import os
import random
import datetime
import argparse


ORDER_ROW_CNT = 10_000

def generate_dummy_order_data(num_rows, start_order_number):
    order_ids = [f"ORD-{start_order_number + i:05}" for i in range(num_rows)]
    customer_ids = [f"CUST-{random.randint(1, 500):04}" for _ in range(num_rows)]
    order_dates = [
        datetime.date(2023, 1, 1) + datetime.timedelta(days=random.randint(0, 364))
        for _ in range(num_rows)
    ]
    order_statuses = random.choices([
        "Pending", "Shipped", "Delivered", "Cancelled"], [0.2, 0.4, 0.35, 0.05], k=num_rows)
    order_totals = [round(random.uniform(50.0, 2000.0), 2) for _ in range(num_rows)]

    return pl.DataFrame({
        "order_id": order_ids,
        "customer_id": customer_ids,
        "order_date": order_dates,
        "order_status": order_statuses,
        "order_total": order_totals,
    })

def generate_dummy_order_details(order_header_df, max_items_per_order=5):
    order_ids = []
    product_ids = []
    quantities = []
    prices = []

    for order_id in order_header_df["order_id"]:
        num_items = random.randint(1, max_items_per_order)
        for _ in range(num_items):
            order_ids.append(order_id)
            product_ids.append(f"PROD-{random.randint(1, 200):04}")
            quantities.append(random.randint(1, 10))
            prices.append(round(random.uniform(10.0, 500.0), 2))

    return pl.DataFrame({
        "order_id": order_ids,
        "product_id": product_ids,
        "quantity": quantities,
        "price": prices,
    })

def save_csv_files(df_order_header, df_order_detail, base_path, file_date):
    header_table_path = f"{base_path}/raw_data/ord_hdr/order_header_{file_date}.csv"
    detail_table_path = f"{base_path}/raw_data/ord_dtl/order_detail_{file_date}.csv"

    print(f"Saving header file to {header_table_path}")
    df_order_header.write_csv(header_table_path, include_header=True)

    print(f"Saving detail file to {detail_table_path}")
    df_order_detail.write_csv(detail_table_path, include_header=True)