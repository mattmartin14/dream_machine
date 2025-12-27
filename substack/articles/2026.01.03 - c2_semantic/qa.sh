#!/bin/bash

duckdb -c "select * from read_csv_auto('~/concept2/workouts/*summary*.csv') limit 10;"

duckdb -c "DESCRIBE SELECT * FROM read_csv_auto('~/concept2/workouts/*summary*.csv');"