#!/bin/bash

: '
    Author: Matt Martin
    Date: 10/12/2023
    Desc: uses duckdb and gsutil to upload files in parallel to gbq
'

echo "converting CSV's to parquet"
dir="$HOME/test_dummy_data/gcs"
for f_path in "$dir"/*.csv
do
    if [ -f "$f_path" ]; then
        par_path="${f_path%.csv}.parquet"
        duckdb_cmd="COPY (SELECT * FROM read_csv_auto('$f_path')) TO '$par_path' (FORMAT PARQUET);"
        duckdb -c "$duckdb_cmd"
    fi
done

echo "Uploading files to GCS in parallel"
gsutil -m cp ~/test_dummy_data/gcs/*.parquet gs://$GCS_BUCKET/test_files/par

