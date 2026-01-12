import os
import duckdb


def generate_store_geography(n_stores: int = 50):
	"""Generate store geography tied to `store_id` and write parquet.

	Columns: store_id, city, state, zip, store_name
	Output: data/store_geography.parquet
	"""
	os.makedirs('data', exist_ok=True)

	cn = duckdb.connect(database=':memory:')

	# State pool (US two-letter codes)
	states = [
		'CA','TX','NY','FL','IL','PA','OH','GA','NC','MI',
		'WA','AZ','MA','TN','IN','MO','WI','CO','MN','SC',
		'AL','LA','KY','OR','OK','CT','IA','UT','NV','AR'
	]

	# Build a city pool from common name combinations (enough to cover many stores)
	city_words1 = ['Oak','Pine','Maple','Cedar','River','Valley','Hill','Grove','Springs','Meadow','Ridge','Harbor','Creek','Brook','Lake']
	city_words2 = ['View','Point','Ridge','Grove','Town','Heights','Falls','Bay','Harbor','Field','Park','Meadows','Hills','Springs','Crossing']
	cities = []
	for a in city_words1:
		for b in city_words2:
			cities.append(f"{a} {b}")
			if len(cities) >= max(n_stores * 2, 100):
				break
		if len(cities) >= max(n_stores * 2, 100):
			break

	def esc(s: str) -> str:
		return s.replace("'", "''")

	states_values = ",".join(f"('{esc(s)}')" for s in states)
	cities_values = ",".join(f"('{esc(c)}')" for c in cities)

	cn.execute(
		f"""
		CREATE TEMP TABLE state_pool AS
		SELECT * FROM (VALUES {states_values}) AS s(state);
		"""
	)
	cn.execute(
		f"""
		CREATE TEMP TABLE city_pool AS
		SELECT * FROM (VALUES {cities_values}) AS c(city);
		"""
	)

	# Create store geography by assigning a random city to each store_id
	cn.execute(
		"""
		CREATE TEMP TABLE store_geography AS
		WITH store_ids AS (
			SELECT range AS store_id FROM range(1, ? + 1)
		), shuffled_cities AS (
			SELECT city, ROW_NUMBER() OVER () AS rn FROM city_pool ORDER BY random()
		), states_per_store AS (
			-- Assign a random state per store using a partitioned window over a cross join
			SELECT s.store_id, sp.state,
				ROW_NUMBER() OVER (PARTITION BY s.store_id ORDER BY random()) AS pick
			FROM store_ids s
			CROSS JOIN state_pool sp
		)
		SELECT
			s.store_id,
			c.city AS city,
			st.state AS state,
			CAST(10000 + CAST(random() * 89999 AS INTEGER) AS INTEGER) AS zip,
			'HomePro ' || c.city AS store_name
		FROM store_ids s
		JOIN shuffled_cities c ON c.rn = s.store_id
		JOIN states_per_store st ON st.store_id = s.store_id AND st.pick = 1
		;
		""",
		[n_stores],
	)

	out_path = 'data/store_geography.parquet'
	cn.execute("COPY (SELECT * FROM store_geography ORDER BY store_id) TO ? (FORMAT 'parquet')", [out_path])
	return out_path

# Convenience alias matching earlier naming
gen_store_geography = generate_store_geography
