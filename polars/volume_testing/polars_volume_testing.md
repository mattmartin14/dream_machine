### Polars Volume Performance Testing
#### Author: Matt Martin
#### Date: 4/15/24

<hr>
<h3>Overview</h3>
The purpose of this exercise is to do some volume and performance testing on Polars to see how well it can handle data larger than what's on the machine. To do this, we will use the following 2 scripts:

1. [Generate Test Data](./volume_testing/gen_volume_data.py)
2. [Process Data](./volume_testing/process_data.py)

The first script generates 200 million rows of test data which comes out to about 16GB of total data in 50 CSV files. The second script processes the CSV's to do the following:

- Read in all the data to a Polars lazy frame
- Do aggregation to rollup several columns to a grouped level
- Adds a processing timestamp as a new column
- writes out the aggregated results to a parquet file

<hr>
<h3>Results</h3>
The results of this exercise were promising. The processing job ran on average over 5 time trials in about 40 seconds on an M2 pro, which had about 12GB of ram to give, since 4GB is tied up in system processes. So the overall data footprint exceeded the amount of RAM available, yet the pipeline was able to process at a generally decent enough pace.

<hr>
<h3>Bonus: Tried in Rust as Well</h3>
I also wrote the data processing script in Rust [here](./volume_testing/polars_r/src/main.rs) to see if I could get a greater performance boost. When I ran the rust script in debug mode, it took about 80 seconds. But when I shipped it to release mode, it ran in about 35 seconds consistently, a 13% performance increase over the python code. 

<hr>
<h3>Other Thoughts</h3>
I also wrote the test harness in Rust to see if there was any performance difference and saw that the Rust version was clocking in 5 seconds faster, which is impressive.