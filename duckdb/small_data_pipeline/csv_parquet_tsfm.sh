#!/bin/bash
source_file="~/test_dummy_data/dummy_data2.csv"
dest_file="~/test_dummy_data/people_agg_tsfm_v2.parquet"
sql=$(cat "tsfm.sql")

## update params
sql=$(echo "$sql" | awk -v find="@source_file" -v replace="$source_file" '{ gsub(find, replace); print }')
sql=$(echo "$sql" | awk -v find="@dest_file" -v replace="$dest_file" '{ gsub(find, replace); print }')
#echo "$sql"
duckdb -c "$sql"
