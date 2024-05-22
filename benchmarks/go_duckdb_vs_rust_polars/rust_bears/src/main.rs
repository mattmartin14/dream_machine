use polars::prelude::*;
use std::error::Error;
use std::env;
use chrono::prelude::*;
//use chrono_tz::Tz;
use std::fs::File;

// updated crate from 0.38.3 to 0.40.0 and time went from 14 seconds to 5 seconds

fn main() -> Result<(), Box<dyn Error>> {
    let start_time = Utc::now();

    let home_dir = env::var("HOME")?;
    let csv_f_path = format!("{}/test_dummy_data/fd/*.csv", home_dir);

    // Load CSV files lazily
    let lf = LazyCsvReader::new(&csv_f_path).finish()?;

    // Transform the data
    let mut tsf = lf
        .lazy()
        .group_by([col("FirstName")])
        .agg([
            col("TxnKey").n_unique().alias("TXN_KEY_CNT"),
            col("NetWorth").sum().alias("NET_WORTH_TOT"),
        ])
        .collect()?;

    // Add current timestamp to the transformed dataframe
    let current_time_et = Local::now().with_timezone(&chrono_tz::America::New_York).naive_local();
    let tsf = tsf.with_column(
        Series::new("process_ts", vec![current_time_et; tsf.height()])
    )?;

    // Export result to parquet
    let par_f_path = format!("{}/test_dummy_data/fd/bears.parquet", home_dir);
    export_to_parquet(tsf, &par_f_path)?;

    let end_time = Utc::now();
    let total_time = end_time - start_time;
    println!("Total time to process data: {:.2} seconds", total_time.num_seconds() as f64);

    Ok(())
}

// Exports the dataframe to parquet
fn export_to_parquet(df: &mut DataFrame, par_f_path: &str) -> Result<(), PolarsError> {
    let mut file = File::create(par_f_path)?;
    ParquetWriter::new(&mut file).finish(df)?;
    Ok(())
}
