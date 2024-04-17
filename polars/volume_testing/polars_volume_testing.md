### Polars Volume Performance Testing
#### Author: Matt Martin
#### Date: 4/15/24

<hr>
<h3>Overview</h3>
The purpose of this exercise is to do some volume and performance testing on Polars to see how well it can handle data larger than what's on the machine. To do this, I wrote 3 scripts:

1. [Generate Test Data](./gen_volume_data.py)
2. [Process Data Python](./process_data.py)
3. [Process Data Rust](./polars_r/src/main.rs)

The first script generates 200 million rows of test data which comes out to about 16GB of total data in 50 CSV files. The second and third script processes the CSV's to do the following:

- Read in all the data to a Polars lazy frame
- Do aggregation to rollup several columns to a grouped level
- Adds a processing timestamp as a new column
- writes out the aggregated results to a parquet file

I wrote the transform script in both Python and Rust to compare any performance differences. Both scripts use the same polars libraries.

<hr>
<h3>Results</h3>
The results of this exercise were promising. Below are the result of 5 time trials in both Python and Rust:

| Time Trial | Python | Rust |
| ---------- | ------ | ---- |
| 1          | 40 sec | 36 sec |
| 2          | 41 sec | 35 sec |


<hr>
<h3>Other Thoughts</h3>
