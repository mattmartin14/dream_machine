import os
from datetime import date, timedelta

import duckdb


# Simplified catalog without awkward quotes in names
PRODUCTS = [
	(1, 'Tools', 'Claw Hammer 16oz', 14.99),
	(2, 'Tools', 'Framing Hammer 20oz', 19.99),
	(3, 'Fasteners', 'Wood Screws 3in (100ct)', 7.49),
	(4, 'Fasteners', 'Drywall Screws (200ct)', 9.99),
	(5, 'Electrical', 'Duplex Outlet 15A', 3.49),
	(6, 'Electrical', 'Light Switch Single Pole', 2.99),
	(7, 'Plumbing', 'PVC Pipe 1/2in x 10ft', 5.99),
	(8, 'Plumbing', 'Brass Ball Valve 3/4in', 12.99),
	(9, 'Paint', 'Interior Paint Gallon White', 24.99),
	(10, 'Paint', "Painter's Tape 2in", 6.49),
	(11, 'Lumber', '2x4x8 SPF Lumber', 3.99),
	(12, 'Lumber', 'Plywood 3/4in 4x8', 42.99),
	(13, 'Flooring', 'Laminate Plank 20sqft', 38.99),
	(14, 'Flooring', 'Ceramic Tile 10sqft', 29.99),
	(15, 'Hardware', 'Door Handle Brushed Nickel', 21.99),
	(16, 'Hardware', 'Hinges 3in (2pk)', 5.49),
	(17, 'Gardening', 'Garden Hose 50ft', 24.49),
	(18, 'Gardening', 'Shovel Round Point', 28.99),
	(19, 'Lighting', 'LED Bulb A19 (4pk)', 9.99),
	(20, 'Lighting', 'Shop Light 4ft LED', 49.99),
	(21, 'Safety', 'Work Gloves L', 8.99),
	(22, 'Safety', 'Safety Glasses', 6.99),
	(23, 'Storage', 'Plastic Bin 18gal', 10.99),
	(24, 'Storage', 'Metal Shelving Unit', 69.99),
	(25, 'Adhesives', 'Construction Adhesive Tube', 5.99),
	(26, 'Adhesives', 'Wood Glue 16oz', 7.99),
	(27, 'Masonry', 'Mortar Mix 60lb', 7.49),
	(28, 'Masonry', 'Concrete Mix 80lb', 6.99),
	(29, 'HVAC', 'Furnace Filter 20x25x1', 12.99),
	(30, 'HVAC', 'Programmable Thermostat', 59.99),
]


def generate_orders(n_orders: int):
	"""Generate order headers and details, saved as parquet files.

	Only parameter is number of order headers.
	Outputs are written to data/order_headers.parquet and data/order_details.parquet.
	"""
	os.makedirs('data', exist_ok=True)

	# Config
	tax_rate = 0.08
	n_stores = 50
	n_customers = 10000

	# Date range: last 365 days
	end_date = date.today()
	start_date = end_date - timedelta(days=365)
	days_span = (end_date - start_date).days

	cn = duckdb.connect()

	# Create products via VALUES to keep it simple
	def esc(s: str) -> str:
		return s.replace("'", "''")

	values_sql = ",\n".join(
		f"({pid}, '{esc(cat)}', '{esc(name)}', {price})" for pid, cat, name, price in PRODUCTS
	)
	cn.execute(
		f"""
		CREATE TEMP TABLE products AS
		SELECT * FROM (VALUES {values_sql}) AS t(product_id, category, product_name, unit_price);
		"""
	)

	# Header base: one row per order with random attributes
	cn.execute(
		"""
		CREATE TEMP TABLE order_headers_base AS
		SELECT
			seq AS order_id,
			(CAST(? AS DATE) + CAST(random() * ? AS INTEGER)) AS order_date,
			CAST(random() * (? - 1) AS INTEGER) + 1 AS store_id,
			CAST(random() * (? - 1) AS INTEGER) + 1 AS customer_id,
			CASE
				WHEN r < 0.60 THEN 'DELIVERED'
				WHEN r < 0.75 THEN 'SHIPPED'
				WHEN r < 0.90 THEN 'PROCESSING'
				WHEN r < 0.95 THEN 'NEW'
				ELSE 'CANCELLED'
			END AS status,
			CASE
				WHEN pm < 0.55 THEN 'CREDIT_CARD'
				WHEN pm < 0.75 THEN 'DEBIT_CARD'
				WHEN pm < 0.88 THEN 'CASH'
				WHEN pm < 0.96 THEN 'GIFT_CARD'
				ELSE 'APPLE_PAY'
			END AS payment_method,
			CAST(random() * 7 AS INTEGER) + 1 AS items_per_order
		FROM (
			SELECT range AS seq, random() AS r, random() AS pm FROM range(1, ? + 1)
		);
		""",
		[
			start_date.isoformat(),
			days_span,
			n_stores,
			n_customers,
			n_orders,
		],
	)

	# Details: pick a random unique subset of products per order, with quantities
	cn.execute(
		"""
		CREATE TEMP TABLE order_details AS
		WITH candidates AS (
			SELECT
				h.order_id,
				h.items_per_order,
				p.product_id,
				p.product_name,
				p.category,
				p.unit_price,
				ROW_NUMBER() OVER (PARTITION BY h.order_id ORDER BY random()) AS rn
			FROM order_headers_base h
			CROSS JOIN products p
		), chosen AS (
			SELECT
				order_id,
				product_id,
				product_name,
				category,
				unit_price
			FROM candidates
			WHERE rn <= items_per_order
		)
		SELECT
			order_id,
			product_id,
			product_name,
			category,
			unit_price,
			CAST(random() * 4 AS INTEGER) + 1 AS quantity,
			ROUND(unit_price * (CAST(random() * 4 AS INTEGER) + 1), 2) AS line_total
		FROM chosen
		;
		"""
	)

	# Aggregate totals for headers
	cn.execute(
		"""
		CREATE TEMP TABLE order_headers AS
		SELECT
			h.order_id,
			h.order_date,
			h.store_id,
			h.customer_id,
			h.status,
			h.payment_method,
			SUM(d.unit_price * d.quantity) AS subtotal,
			ROUND(SUM(d.unit_price * d.quantity) * ?, 2) AS tax,
			ROUND(SUM(d.unit_price * d.quantity) * (1 + ?), 2) AS total,
			COUNT(*) AS line_count
		FROM order_headers_base h
		JOIN order_details d USING(order_id)
		GROUP BY 1,2,3,4,5,6
		ORDER BY order_id;
		""",
		[tax_rate, tax_rate],
	)

	headers_path = '../data/order_headers.parquet'
	details_path = '../data/order_details.parquet'

	cn.execute(
		"COPY (SELECT * FROM order_headers) TO ? (FORMAT 'parquet')",
		[headers_path],
	)
	cn.execute(
		"COPY (SELECT * FROM order_details ORDER BY order_id, product_id) TO ? (FORMAT 'parquet')",
		[details_path],
	)

	return headers_path, details_path
