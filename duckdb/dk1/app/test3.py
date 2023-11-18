
#all files in the app sub folder get mapped into the container on /usr/src/app
# from there, you can do python test3.py and it executes this script
import duckdb

duckdb.sql("create table test (name string)")
duckdb.sql("insert into test values ('bob'),('john')")
duckdb.sql('select * from test').show()

duckdb.sql("insert into test select * from test")
duckdb.sql('select count(*) as x from test').show()