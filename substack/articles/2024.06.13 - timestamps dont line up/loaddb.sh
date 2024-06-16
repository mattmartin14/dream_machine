#!/bin/bash
sql=$(cat "order_data.sql")
duckdb "order_repo.db" -c "$sql"