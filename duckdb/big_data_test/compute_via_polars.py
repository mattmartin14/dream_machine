import polars as pl
import time
import os

def main():
    start_time = time.time()

    # Use scan_parquet for lazy loading instead of read_parquet
    df = pl.scan_parquet(os.path.expanduser('~/test_dummy_data/duckdb/data[1-3].parquet'))

    result = (
        df.group_by("rpt_dt")
        .agg([
            pl.col("txn_key").n_unique().alias("unique_txn_keys"),
            pl.len().alias("total_rows"),
            pl.col("sales_amt").sum().alias("total_sales_amt"),
            pl.col("sales_amt").mean().alias("avg_sales_amt"),
        ])
        .collect(engine="streaming")  # Only materialize the result at the end
    )

    print(result)

    end_time = time.time()

    print(f"Total time to compute dataset via polars: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()