#!/bin/bash
sql1=$(cat "tsfm.sql")
duckdb -c "$sql1"
