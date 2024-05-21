use polars::prelude::*;
use std::error::Error;
use std::env;
use chrono::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {

    let start_time = Utc::now();

    let home_dir = env::var("HOME")?;
    let csv_f_path = format!("{}/test_dummy_data/fd/*.csv", home_dir);
    
    let lf = LazyCsvReader::new(csv_f_path).finish()?;
  
    //transform
    let mut tsf = lf.clone().lazy()
        .group_by([col("FirstName")])
        .agg([
            col("TxnKey").n_unique().alias("TXK_KEY_CNT"),
            col("NetWorth").sum().alias("NET_WORTH_TOT"),
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
    let par_f_path = format!("{}/test_dummy_data/fd/bears.parquet", home_dir);
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