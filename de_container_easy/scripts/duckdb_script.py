import duckdb

cn = duckdb.connect()

cn.execute("CREATE TABLE items (id INTEGER, name VARCHAR)")
cn.execute("INSERT INTO items VALUES (1, 'item1'), (2, 'item2')")

print("Containers for Data Engineering is Brain-Dead Easy!")

cn.sql("COPY (SELECT * FROM items) TO '/scripts/items.parquet' WITH (FORMAT PARQUET)")

