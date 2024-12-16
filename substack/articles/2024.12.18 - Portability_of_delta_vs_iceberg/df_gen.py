import polars as pl
import random
import datetime

def generate_dummy_order_data(num_rows):
    order_ids = [f"ORD-{1 + i:05}" for i in range(num_rows)]
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