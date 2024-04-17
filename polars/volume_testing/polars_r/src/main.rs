
/*
    Author: Matt Martin
    Date: 4/15/24
    Desc: Polars Volume Testing in Rust
*/

use polars::prelude::*;
use std::error::Error;
use std::env;
use chrono::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {

    let start_time = Utc::now();

    let home_dir = env::var("HOME")?;
    let csv_f_path = format!("{}/test_dummy_data/polars/data*.csv", home_dir);
    
    //println!("{}",csv_f_path);
    //scan the csv
    let lf = LazyCsvReader::new(csv_f_path).finish()?
    ;
  
    //transform
    let mut tsf = lf.clone().lazy()
        .group_by([col("state")])
        .agg([
            col("zip_cd").n_unique().alias("zip_cd_cnt"),
            col("first_name").n_unique().alias("unique_first_name_cnt"),
            col("net_worth").sum().alias("total_net_worth"),
        ])
        .collect()
        .expect("Error getting dataframe created")
    ;

    // add current timestamp to the transformed dataframe
    let current_time_et = Local::now().with_timezone(&chrono_tz::America::New_York).naive_local();
    //println!("{}",current_time_et);
    let _result = tsf.with_column(
        Series::new("process_ts", vec![current_time_et])
    );

    //sample top 5 rows
    //println!("{}", tsf.head(Some(5)));

    // export result to parquet
    let par_f_path = format!("{}/test_dummy_data/polars/state_rust.parquet", home_dir);
    export_to_parquet(&mut tsf, &par_f_path)?;

    let end_time = Utc::now();
    let total_time = end_time - start_time;
    println!("Total time to process data: {:.2} seconds", total_time.num_seconds() as f64);

    Ok(())
}

//exports the dataframe to parquet
fn export_to_parquet(df: &mut DataFrame, par_f_path: &str) -> Result<(), PolarsError> {
    let mut file = std::fs::File::create(par_f_path)?;
    ParquetWriter::new(&mut file).finish(df)?;
    Ok(())
}