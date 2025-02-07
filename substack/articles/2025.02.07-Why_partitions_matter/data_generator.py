import polars as pl
import os
import random
import datetime
import argparse

_ROW_CNT = 5_000_000

def generate_dummy_order_data(num_rows, start_order_number):
    order_ids = [f"ORD-{start_order_number + i:05}" for i in range(num_rows)]
    customer_ids = [f"CUST-{random.randint(1, 500):04}" for _ in range(num_rows)]
    
    # Increment date every 50,000 orders
    base_date = datetime.date(2023, 1, 1)
    order_dates = [base_date + datetime.timedelta(days=(i // 10_000)) for i in range(num_rows)]

    order_statuses = random.choices(
        ["Pending", "Shipped", "Delivered", "Cancelled"], 
        [0.2, 0.4, 0.35, 0.05], 
        k=num_rows
    )
    order_totals = [round(random.uniform(50.0, 2000.0), 2) for _ in range(num_rows)]

    return pl.DataFrame({
        "order_date": order_dates,  # First column
        "order_id": order_ids,
        "customer_id": customer_ids,
        "order_status": order_statuses,
        "order_total": order_totals,
    })

def generate_dummy_order_details(order_header_df, max_items_per_order=5):
    order_dates = []
    order_ids = []
    product_ids = []
    quantities = []
    prices = []

    for order_id, order_date in zip(order_header_df["order_id"], order_header_df['order_date']):
        num_items = random.randint(1, max_items_per_order)
        for _ in range(num_items):
            order_dates.append(order_date)
            order_ids.append(order_id)
            product_ids.append(f"PROD-{random.randint(1, 200):04}")
            quantities.append(random.randint(1, 10))
            prices.append(round(random.uniform(10.0, 500.0), 2))

    return pl.DataFrame({
        "order_date": order_dates,
        "order_id": order_ids,
        "product_id": product_ids,
        "quantity": quantities,
        "price": prices,
    })


def save_parquet_files(df_order_header, df_order_detail, base_path, file_date):
    header_table_path = f"{base_path}/raw_data/ord_hdr/order_header_{file_date}.parquet"
    detail_table_path = f"{base_path}/raw_data/ord_dtl/order_detail_{file_date}.parquet"

    print(f"Saving header file to {header_table_path}")
    df_order_header.write_parquet(header_table_path)

    print(f"Saving detail file to {detail_table_path}")
    df_order_detail.write_parquet(detail_table_path)

def save_csv_files(df_order_header, df_order_detail, base_path, file_date):
    header_table_path = f"{base_path}/raw_data/ord_hdr/order_header_{file_date}.csv"
    detail_table_path = f"{base_path}/raw_data/ord_dtl/order_detail_{file_date}.csv"

    print(f"Saving header file to {header_table_path}")
    df_order_header.write_csv(header_table_path, include_header=True)

    print(f"Saving detail file to {detail_table_path}")
    df_order_detail.write_csv(detail_table_path, include_header=True)


if __name__ == "__main__":
    df = generate_dummy_order_data(_ROW_CNT,1)
    dfo = generate_dummy_order_details(df, 5)
    save_parquet_files(df, dfo, ".", "v1")