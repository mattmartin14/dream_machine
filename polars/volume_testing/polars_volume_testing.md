### Polars Volume Performance Testing
#### Author: Matt Martin
#### Date: 4/15/24

<hr>
<h3>Overview</h3>
The purpose of this exercise is to do some volume and performance testing on Polars to see how well it can handle data larger than what's on the machine. 

<hr>
<h3>The Setup</h3>
The machine I'm testing this performance on has these specs:

Apple Macbook M2 Pro:

- 16 GB Ram: 3-4GB always in use for OS and app level tasks, so I have roughly 12GB to give to process the data.
- 12 Cores CPU

<hr>
<h3>Generating Test Data</h3>
To generate the test data, I wrote the following Python script:

- [Generate Test Data](./gen_volume_data.py)

This script uses python's mimesis package to generate test data. I wrote the script to run in parallel using python's multi-processsing to accelerate the generation. At the end of the script, it generated the following:

- 200 million total rows of data
- 50 CSV files
- 16 GB of data

Since this is an I/O bound task, I went with multi-processing instead of multi-threading, which was signficantly faster. The script ran on my machine in about 4 minutes.

<hr>
<h3>Processing the Data (ETL)</h3>
To process the data, I wrote 2 scripts that do the same thing. One is written in python and the other is in Rust:

1. [Process Data Python](./process_data.py)
2. [Process Data Rust](./polars_r/src/main.rs)

Both scripts perform the following Extract-Transform-Load (ETL) operations:

- Read in all the data to a Polars lazy frame
- Do aggregation to rollup several columns to a grouped level
- Adds a processing timestamp as a new column
- writes out the aggregated results to a parquet file

*Pro Tip* - Use Polar's Lazy Frame reader instead of the normal dataframe CSV reader. This will prevent your machine from locking up due to memory contention. Daniel Beach shared this tip first a month ago or so.

<hr>
<h3>Results</h3>
I ran both scripts 5 times each and documented the runs below. The results of this exercise were promising. Both Python and Rust performed around the same time with Rust getting a slight edge. This was no surprise given the Python implementation of Polars is just a wrapper for the Rust functions.

| Time Trial | Python | Rust |
| ---------- | ------ | ---- |
| 1          | 40 sec | 39 sec |
| 2          | 41 sec | 40 sec |
| 3          | 42 sec | 36 sec |
| 4          | 40 sec | 41 sec |
| 5          | 41 sec | 40 sec |
| **Avg**      | **41 sec** | **39 sec** |


<hr>
<h3>Conclusion</h3>
This test harness demonstrated that you can process more data in polars than the available ram on your machine. The key is to use the lazy frame and the csv scanner so that Polars does not try to load everything into memory all at once. I'm glad I wrote the code in both Python and Rust. Even though I'm still on the fence of using Rust as a daily driver, I can see use-cases for it where you just want a compiled binary to ship and to where you don't have to worry about python runtime environments or dependencies, since Rust just packs it all in a single executable.
<br></br>
*Another Pro Tip* - On Rust, make sure when you are ready to "really" test your code, you run a build/release e.g.:

```bash
cargo build --release
```

This will remove all the debugging overhead and speed up the script. When I ran the Rust code in debug mode, it was nearly twice as long to complete.