import polars as pl
import random
import numpy as np
import os
from datetime import datetime, timedelta

def generate_data(num_rows: int, output_file: str = "~/dummy_data/c/data.csv"):
    output_file = os.path.expanduser(output_file)
    output_dir = os.path.dirname(output_file)
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    order_ids = np.arange(1, num_rows + 1)
    
    data = []
    for order_id in order_ids:
        num_lines = random.randint(1, 5) 
        for order_line_id in range(1, num_lines + 1):
            order_date = (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%m/%d/%Y")
            quantity = random.randint(1, 10)
            price = round(random.uniform(5.0, 500.0), 2)
            data.append((order_id, order_line_id, order_date, quantity, price))
    
    df = pl.DataFrame(
        data,
        schema=["order_id", "order_line_id", "order_date", "quantity", "price"]
    )
    
    df.write_csv(output_file)
    print(f"CSV file '{output_file}' with {len(df)} rows generated successfully.")

ORDER_CNT = 1_000_000
generate_data(ORDER_CNT)
