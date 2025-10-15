#!/bin/bash
echo "Starting DuckDB query..."
start_time=$(date +%s.%N)

# Create temp directory if it doesn't exist
mkdir -p "$HOME/duckdb_tmp"

files_to_sample="[1-10]"

duckdb << EOF
SET memory_limit = '8GB';
SET temp_directory = '$HOME/duckdb_tmp';
SET max_memory = '8GB';
SET preserve_insertion_order = false;

.maxwidth 1000

CREATE OR REPLACE VIEW all_data 
AS 
SELECT * 
FROM read_parquet('$HOME/test_dummy_data/duckdb/data$files_to_sample.parquet');

SELECT COALESCE(rpt_dt::date::varchar, 'GRAND TOTAL') AS rpt_dt,
    --count(distinct txn_key) AS unique_txn_keys,
    printf('%,d', count(*)) AS total_rows,
    printf('$%,.2f', sum(sales_amt)) AS total_sales_amt,
    printf('$%.2f', avg(sales_amt)) AS avg_sales_amt
FROM all_data
GROUP BY ROLLUP(rpt_dt)
ORDER BY rpt_dt NULLS LAST;
EOF

end_time=$(date +%s.%N)
duration=$(echo "$end_time - $start_time" | bc)

echo "Total time for DuckDB to compute dataset: ${duration} seconds"