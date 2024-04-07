
/*
    Author: Matt Martin
    Date: 4/7/24
    Desc: Testing Polars on Rust
*/


use polars::prelude::*;
use std::error::Error;
use std::env;

fn main() -> Result<(), Box<dyn Error>> {

    let home_dir = env::var("HOME")?;
    let csv_f_path = format!("{}/test_dummy_data/fd/fin_data_1.csv", home_dir);

    //read the csv
    let df = load_csv_to_df(&csv_f_path)?;
    
    // view the schema
    //println!("{:?}", df.schema());
    
    /*
        A few gotchas here on how to use the transform:
            1) Polars Rust no longer supports just the direct dataframe grouping from the docs.
                Instead you have to use the lazy evaluator
            2) to make the result come back as a DataFrame and not a LazyFrame, invoke the collect() function
            3) Collect will return a dataframe + polars error if applicable:
                see https://docs.rs/polars/latest/polars/prelude/struct.LazyFrame.html#method.collect
                to do this right, you have to add the expect() or unwrap() at the end

    */
    //transform
    let mut df2 = df.clone().lazy()
        .group_by([col("FirstName")])
        .agg([
            col("LastName").count().alias("lm_cnt"),
            col("NetWorth").sum().alias("tot_net_worth")
        ])
        .collect()
        //.unwrap()
        .expect("Error getting dataframe created")
    ;

    //sample top 5 rows
    println!("{}", df2.head(Some(5)));


    // export result to parquet
    let par_f_path = format!("{}/test_dummy_data/fd/rust_par.parquet", home_dir);
    export_to_parquet(&mut df2, &par_f_path)?;
    
    Ok(())
}

// import the csv
fn load_csv_to_df(csv_f_path: &str) -> Result<DataFrame, PolarsError> {
    CsvReader::from_path(csv_f_path)?.has_header(true).finish()
}

//exports the dataframe to parquet
fn export_to_parquet(df: &mut DataFrame, par_f_path: &str) -> Result<(), PolarsError> {
    let mut file = std::fs::File::create(par_f_path)?;
    ParquetWriter::new(&mut file).finish(df)?;
    Ok(())
}
