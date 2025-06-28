import duckdb
cn = duckdb.connect('city.duckdb')

sql = """
CREATE OR REPLACE TABLE data (id int, city varchar(50), state char(2), population int);

"""
cn.execute(sql)

cn.execute("INSERT INTO data VALUES (1, 'San Francisco', 'CA', 883305);")
cn.execute("INSERT INTO data VALUES (2, 'Los Angeles', 'CA', 3990456);")
cn.execute("INSERT INTO data VALUES (3, 'New York', 'NY', 8419600);")
cn.execute("INSERT INTO data VALUES (4, 'Chicago', 'IL', 2716000);")
cn.execute("INSERT INTO data VALUES (5, 'Houston', 'TX', 2328000);")
cn.execute("INSERT INTO data VALUES (6, 'Phoenix', 'AZ', 1680992);")
cn.execute("INSERT INTO data VALUES (7, 'Philadelphia', 'PA', 1584200);")
cn.execute("INSERT INTO data VALUES (8, 'San Antonio', 'TX', 1547253);")
cn.execute("INSERT INTO data VALUES (9, 'San Diego', 'CA', 1423851);")
cn.execute("INSERT INTO data VALUES (10, 'Dallas', 'TX', 1341075);")


cn.sql("select * from data").show()