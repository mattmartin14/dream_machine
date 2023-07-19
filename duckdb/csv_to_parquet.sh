#!/bin/bash
source_path="./dummy_data.csv"
output_path="./dummy_data2.parquet"
duckdb_cmd="COPY (SELECT * FROM read_csv_auto('$source_path')) TO '$output_path' (FORMAT PARQUET);"
duckdb -c "$duckdb_cmd"
echo "process completed"
#validate
sql="SELECT COUNT(*) AS ROW_CNT FROM read_parquet('$output_path');"
duckdb -c "$sql"
sql="SELECT * FROM read_parquet('$output_path') limit 5;"
duckdb -c "$sql"

