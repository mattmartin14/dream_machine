"""
    Author: Matt Martin
    Date: 4/15/24
    Desc: Testing processing 200M rows of data in Polars (16GB of CSVs)

"""

import polars as pl
import time
import pytz
from datetime import datetime

def process_data() -> None:

    start_time = time.time()

    #scan the csv and put it into a lazy frame so tit doesn't gobble up all the memory
    df = pl.scan_csv("~/test_dummy_data/polars/data*.csv").lazy()

    ## transform and aggregate up; use "collect" to physicalize to a dataframe
    tsf = df.group_by('state').agg(
        pl.col("zip_cd").n_unique().alias("zip_cd_cnt"),
        pl.col("first_name").n_unique().alias("unique_first_name_cnt"),
        pl.col("net_worth").sum().alias("total_net_worth")
    ).collect()

    ## add current timestamp as a new column to the aggregated results
    est = pytz.timezone('America/New_York')
    current_time_et = datetime.now(est)
    tsf.with_columns(process_ts = pl.lit(current_time_et))

    ## write out to parquet
    tsf.write_parquet("~/test_dummy_data/polars/state_agg.parquet")

    end_time = time.time()
    total_time = round(end_time - start_time,2)
    print(f"Total time to process data: {total_time} seconds")

if __name__ == "__main__":
    process_data()