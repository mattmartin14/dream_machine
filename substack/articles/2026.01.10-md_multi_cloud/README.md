## Data Generators

- **Orders:** Generates ~10,000 synthetic orders for a rowing shop using DuckDB and writes to Parquet.
- **Customer Insights:** Generates survey-style ratings per order keyed by `order_id` (and includes `cust_id`). Depends on the orders output to align keys.

### Quick Start (uv)

Run with uv (recommended):

```bash
uv run scripts/generate_orders.py --count 10000 --customers 3000 --out data/orders.parquet --seed 42
uv run scripts/generate_insights.py --orders-path data/orders.parquet --out data/customer_insights.parquet --seed 42
```

### Files

- Orders script: [scripts/generate_orders.py](scripts/generate_orders.py)
- Customer insights script: [scripts/generate_insights.py](scripts/generate_insights.py)
- Orders output: [data/orders.parquet](data/orders.parquet)
- Insights output: [data/customer_insights.parquet](data/customer_insights.parquet)

### Join Example (DuckDB)

Join insights back to orders on `order_id` (preferred), or `cust_id` if needed:

```python
import duckdb

con = duckdb.connect()
df = con.execute(
	"""
	SELECT *
	FROM read_parquet('data/orders.parquet') o
	JOIN read_parquet('data/customer_insights.parquet') i USING (order_id)
	ORDER BY o.order_date DESC
	LIMIT 10
	"""
).df()

print(df.head())
```

### Notes

- Reproducibility: pass `--seed` to both scripts to get deterministic outputs.
- Future cloud upload: you can swap the `COPY ... TO 'parquet'` parts to write directly to cloud object storage (AWS/GCP) or add a follow-up step to upload the generated files.

