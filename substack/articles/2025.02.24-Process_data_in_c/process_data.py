import polars as pl
from pathlib import Path

input_file = Path("~/dummy_data/c/data.csv").expanduser()
output_file = Path("~/dummy_data/c/summary.txt").expanduser()

df = pl.read_csv(input_file, try_parse_dates=True)

# agg
summary_df = (
    df.group_by("order_id")
    .agg(
        pl.col("quantity").sum().alias("total_quantity"),
        pl.col("price").sum().alias("total_price"),
        pl.col("order_date").max().alias("max_order_date")
    )
)

summary_df.write_csv(output_file)

print(f"Summary written to {output_file}")
