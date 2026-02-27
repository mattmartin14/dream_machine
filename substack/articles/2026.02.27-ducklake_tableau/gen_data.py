#!/usr/bin/env python3

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Generate flashlight store order header/detail CSVs using DuckDB."
	)
	parser.add_argument(
		"--orders",
		type=int,
		default=1000,
		help="Number of orders to generate (default: 1000).",
	)
	parser.add_argument(
		"--output-dir",
		type=Path,
		default=Path("."),
		help="Directory to write CSVs (default: current directory).",
	)
	parser.add_argument(
		"--seed",
		type=float,
		default=None,
		help="Optional DuckDB random seed (0-1) for repeatable data.",
	)
	return parser.parse_args()


def generate_data(num_orders: int, output_dir: Path, seed: float | None) -> None:
	output_dir.mkdir(parents=True, exist_ok=True)
	con = duckdb.connect()

	if seed is not None:
		con.execute("SELECT setseed(?)", [seed])

	con.execute(
		"""
		CREATE OR REPLACE TEMP TABLE order_header AS
		SELECT
			order_id,
			order_date,
			CAST(strftime(order_date, '%Y') AS INTEGER) AS order_year,
			CAST(strftime(order_date, '%m') AS INTEGER) AS order_month,
			'C' || lpad(CAST(1 + floor(random() * 5000) AS VARCHAR), 5, '0') AS customer_id,
			list_value('online', 'store', 'phone')[1 + CAST(floor(random() * 3) AS BIGINT)] AS channel,
			list_value('CA', 'TX', 'NY', 'FL', 'WA', 'OR', 'AZ', 'IL')[1 + CAST(floor(random() * 8) AS BIGINT)] AS state
		FROM range(1, ? + 1) AS t(order_id)
		CROSS JOIN (
			SELECT DATE '2024-01-01' + CAST(floor(random() * 365) AS INTEGER) AS order_date
		)
		""",
		[num_orders],
	)

	con.execute(
		"""
		CREATE OR REPLACE TEMP TABLE order_detail AS
		SELECT
			order_id,
			line_number,
			sku,
			product_name,
			quantity,
			unit_price,
			round(quantity * unit_price, 2) AS line_amount
		FROM (
			SELECT
				h.order_id,
				gs AS line_number,
				skus[1 + CAST(floor(random() * array_length(skus)) AS BIGINT)] AS sku,
				names[1 + CAST(floor(random() * array_length(names)) AS BIGINT)] AS product_name,
				1 + floor(random() * 4)::INTEGER AS quantity,
				round(5 + random() * 95, 2) AS unit_price
			FROM order_header AS h
			CROSS JOIN (
				SELECT
					list_value(
						'FL-1000', 'FL-2000', 'HL-1000', 'HB-1000',
						'CL-1000', 'AC-1000', 'AC-2000', 'BK-1000'
					) AS skus,
					list_value(
						'Pocket Beam', 'Trail Blazer', 'Headlamp Lite', 'Headlamp Pro',
						'Camp Lantern', 'USB-C Charger', 'Battery Pack', 'Bike Mount'
					) AS names
			) AS p
			CROSS JOIN range(1, 1 + CAST(floor(random() * 4) AS INTEGER) + 1) AS r(gs)
		)
		"""
	)

	con.execute(
		"""
		CREATE OR REPLACE TEMP TABLE order_header_enriched AS
		SELECT
			h.*,
			round(sum(d.line_amount), 2) AS order_amount
		FROM order_header AS h
		JOIN order_detail AS d ON d.order_id = h.order_id
		GROUP BY ALL
		"""
	)

	header_path = output_dir / "order_header.csv"
	detail_path = output_dir / "order_detail.csv"

	con.execute(
		"COPY order_header_enriched TO ? (HEADER, DELIMITER ',')",
		[str(header_path)],
	)
	con.execute(
		"COPY order_detail TO ? (HEADER, DELIMITER ',')",
		[str(detail_path)],
	)


def main() -> None:
	args = parse_args()
	if args.orders <= 0:
		raise SystemExit("--orders must be a positive integer")
	if args.seed is not None and not (0.0 <= args.seed <= 1.0):
		raise SystemExit("--seed must be between 0 and 1")

	generate_data(args.orders, args.output_dir, args.seed)


if __name__ == "__main__":
	main()
