import polars as pl
import numpy as np
import time

def main():
    start_time = time.time()
    print(f"Script started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
    
    n = 5_000_000
    print(f"Generating {n:,} rows...")
    rng = np.random.default_rng(42)  # reproducible

    # Vectorized random order_id: 10 chars from [A-Z0-9] without Python loops
    alphabet = np.frombuffer(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", dtype="|S1")
    ids = alphabet[rng.integers(0, len(alphabet), size=(n, 10))]
    order_id = ids.copy().view("|S10").ravel().astype(str)  # collapse 10 bytes -> 1 string per row

    customer_id = rng.integers(1000, 10000, size=n, dtype=np.int32)
    product = rng.choice(np.array(["Widget", "Gadget", "Thingamajig", "Doohickey"], dtype="U16"), size=n)
    quantity = rng.integers(1, 21, size=n, dtype=np.int16)
    price = rng.uniform(10.0, 500.0, size=n).round(2)
    days = rng.integers(1, 29, size=n, dtype=np.int16)
    order_date = np.char.add("2025-01-", np.char.zfill(days.astype(str), 2))

    df = (
        pl.DataFrame(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "product": product,
                "quantity": quantity,
                "price": price,
                "order_date": order_date,
            }
        )
        .with_columns(
            pl.col("product").cast(pl.Categorical),
            pl.col("order_date").cast(pl.Categorical),
        )
        .rechunk()
    )

    print(df.head(5))
   # df.write_parquet("output.parquet")

    result = (
        df.group_by("order_date")
        .agg(
            [
                pl.col("customer_id").n_unique().alias("unique_customers"),
                pl.col("price").sum().alias("total_price"),
            ]
        )
        .sort("order_date")
    )
    print(result)
    result.write_csv("output_grouped.csv")
    
    # End timing and report results
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"\n{'='*50}")
    print(f"Script completed at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
    print(f"Total rows generated: {df.height:,}")
    print(f"Total execution time: {elapsed_time:.2f} seconds")
    print(f"Rows per second: {df.height/elapsed_time:,.0f}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()