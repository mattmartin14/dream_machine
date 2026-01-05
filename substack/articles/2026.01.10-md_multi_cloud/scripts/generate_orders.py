import argparse
from pathlib import Path
import duckdb


def generate_orders(count: int, customers: int, out_path: Path, seed: int | None = None) -> None:
    con = duckdb.connect()
    if seed is not None:
        # Try to seed DuckDB's random; ignore if not supported by version
        try:
            con.execute(f"PRAGMA random_seed={int(seed)}")
        except Exception:
            pass

    sql = f"""
    WITH params AS (
        SELECT {int(count)}::INT AS order_count, {int(customers)}::INT AS customers_count
    ),
    base AS (
        SELECT
            (row_number() OVER ()) AS order_id,
            1 + (random() * customers_count)::INT AS cust_id,
            (DATE '2025-01-01' + (round(random() * 364))::INT) AS order_date,
            CASE round(random()*9)::INT
                WHEN 0 THEN 'OAR-CLUB'
                WHEN 1 THEN 'OAR-PRO'
                WHEN 2 THEN 'ERG-100'
                WHEN 3 THEN 'ERG-PLUS'
                WHEN 4 THEN 'BOAT-SINGLE'
                WHEN 5 THEN 'BOAT-DOUBLE'
                WHEN 6 THEN 'SEAT-GEL'
                WHEN 7 THEN 'STRAP-PRO'
                WHEN 8 THEN 'APP-JACKET'
                ELSE 'ACC-GPS'
            END AS sku,
            CASE round(random()*9)::INT
                WHEN 0 THEN 'Oar'
                WHEN 1 THEN 'Oar'
                WHEN 2 THEN 'Rowing Machine'
                WHEN 3 THEN 'Rowing Machine'
                WHEN 4 THEN 'Boat'
                WHEN 5 THEN 'Boat'
                WHEN 6 THEN 'Seat'
                WHEN 7 THEN 'Footstrap'
                WHEN 8 THEN 'Clothing'
                ELSE 'Accessories'
            END AS product_name,
            CASE round(random()*2)::INT
                WHEN 0 THEN 'Online'
                WHEN 1 THEN 'In-Store'
                ELSE 'Phone'
            END AS channel,
            CASE round(random()*2)::INT
                WHEN 0 THEN 'Standard'
                WHEN 1 THEN 'Express'
                ELSE 'Pickup'
            END AS shipping_method,
            CASE round(random()*20)::INT
                WHEN 0 THEN 'Cancelled'
                WHEN 1 THEN 'Pending'
                ELSE 'Completed'
            END AS order_status,
            1 + (round(random()*3))::INT AS quantity
        FROM params, range(order_count)
    ),
    mid AS (
        SELECT
            *,
            (
                CASE sku
                    WHEN 'OAR-CLUB' THEN 89
                    WHEN 'OAR-PRO' THEN 149
                    WHEN 'ERG-100' THEN 799
                    WHEN 'ERG-PLUS' THEN 1099
                    WHEN 'BOAT-SINGLE' THEN 4999
                    WHEN 'BOAT-DOUBLE' THEN 6999
                    WHEN 'SEAT-GEL' THEN 49
                    WHEN 'STRAP-PRO' THEN 29
                    WHEN 'APP-JACKET' THEN 129
                    ELSE 199
                END * (1 + (random()*0.10 - 0.05))
            ) AS unit_price
        FROM base
    )
    SELECT
        order_id,
        order_date,
        cust_id,
        sku,
        product_name,
        channel,
        shipping_method,
        order_status,
        quantity,
        round(unit_price::DOUBLE, 2) AS unit_price,
        round((quantity * unit_price)::DOUBLE, 2) AS total_amount
    FROM mid
    """

    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({sql}) TO '{out_path.as_posix()}' (FORMAT 'parquet')")

    # Optional: quick peek to confirm row count
    cnt = con.sql(f"SELECT COUNT(*) AS n FROM read_parquet('{out_path.as_posix()}')").fetchone()[0]
    print(f"Wrote {cnt} orders to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic rowing shop orders to Parquet using DuckDB")
    parser.add_argument("--count", type=int, default=10_000, help="Number of orders to generate (default: 10000)")
    parser.add_argument("--customers", type=int, default=3_000, help="Number of distinct customers (default: 3000)")
    parser.add_argument("--out", type=Path, default=Path("data/orders.parquet"), help="Output Parquet file path")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    args = parser.parse_args()

    generate_orders(args.count, args.customers, args.out, args.seed)
