import argparse
from pathlib import Path
import duckdb


def generate_insights(orders_path: Path, out_path: Path, seed: int | None = None) -> None:
    con = duckdb.connect()
    if seed is not None:
        try:
            con.execute(f"PRAGMA random_seed={int(seed)}")
        except Exception:
            pass

    orders_sql_path = orders_path.as_posix()

    sql = f"""
    WITH orders AS (
        SELECT order_id, cust_id, order_date, total_amount FROM read_parquet('{orders_sql_path}')
    ),
    insights AS (
        SELECT
            o.order_id,
            o.cust_id,
            -- Survey date shortly after the order date
            (o.order_date + (round(random() * 14))::INT) AS survey_date,
            -- Ratings 5-10 range
            5 + (round(random()*5))::INT AS overall_satisfaction,
            5 + (round(random()*5))::INT AS product_quality,
            5 + (round(random()*5))::INT AS staff_friendliness,
            5 + (round(random()*5))::INT AS store_cleanliness,
            5 + (round(random()*5))::INT AS delivery_experience,
            5 + (round(random()*5))::INT AS value_for_money,
            CASE round(random()*10)::INT
                WHEN 0 THEN 'Detractor'
                WHEN 1 THEN 'Detractor'
                WHEN 2 THEN 'Passive'
                WHEN 3 THEN 'Passive'
                WHEN 4 THEN 'Passive'
                ELSE 'Promoter'
            END AS nps_category,
            round(random()*100)::INT AS recommend_probability
        FROM orders o
    )
    SELECT * FROM insights
    """

    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({sql}) TO '{out_path.as_posix()}' (FORMAT 'parquet')")

    cnt = con.sql(f"SELECT COUNT(*) AS n FROM read_parquet('{out_path.as_posix()}')").fetchone()[0]
    print(f"Wrote {cnt} customer insights to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic customer survey insights to Parquet using DuckDB")
    parser.add_argument("--orders-path", type=Path, default=Path("data/orders.parquet"), help="Input orders Parquet path")
    parser.add_argument("--out", type=Path, default=Path("data/customer_insights.parquet"), help="Output Parquet file path")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    args = parser.parse_args()

    generate_insights(args.orders_path, args.out, args.seed)
