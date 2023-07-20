#!/bin/bash

# ----------------------
#Author: Matt Martin
#Date: 2023-07-20
#Desc: Benchmarks converting a csv to parquet using duckdb
# ----------------------

source_path="$HOME/test_dummy_data/dummy_data2.csv"
fsize=$(stat -f "%z" "$source_path")
fsize_gb=$(bc <<< "scale=2; $fsize / (1024*1024*1024)")
output_path="$HOME/test_dummy_data/dummy_data2.parquet"
duckdb_cmd="COPY (SELECT * FROM read_csv_auto('$source_path')) TO '$output_path' (FORMAT PARQUET);"
start_ts=$(date +%s)
duckdb -c "$duckdb_cmd"
end_ts=$(date +%s)
elapsed=$((end_ts - start_ts))
echo "******************************"
echo "Total time to convert $fsize_gb GB csv file to parquet: $elapsed seconds"
echo "******************************"
echo "Sampling/Validation"
sql="SELECT COUNT(*) AS ROW_CNT FROM read_parquet('$output_path');"
duckdb -c "$sql"
sql="SELECT * FROM read_parquet('$output_path') limit 5;"
duckdb -c "$sql"
