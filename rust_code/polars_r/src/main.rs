
use polars::prelude::*;

fn main() {

    //read the csv
    let csv_f_path = "/Users/matthewmartin/test_dummy_data/fd/fin_data_1.csv";
    let mut df = load_csv_to_df(csv_f_path)
        .unwrap_or_else(|e| {
            eprintln!("Error loading CSV: {}", e);
            std::process::exit(1)
        });
    
    // view the schema
    //println!("{:?}", df.schema());
    
    /*
        A few gotchas here:
            1) Polars Rust no longer supports just the direct dataframe grouping from the docs.
                Instead you have to use the lazy evaluator
            2) to make the result come back as a DataFrame and not a LazyFrame, invoke the collect() function
            3) Collect will return a dataframe + polars error if applicable:
                see https://docs.rs/polars/latest/polars/prelude/struct.LazyFrame.html#method.collect
                to do this right, you have to add the expect() at the end

    */
    //transform
    let mut df2 = df.clone().lazy()
        .group_by([col("FirstName")])
        .agg([
            col("LastName").count().alias("lm_cnt"),
            col("NetWorth").sum().alias("tot_net_worth")
        ])
        .collect()
        .expect("Error getting dataframe created")
    ;

    // Display the result
    println!("{:?}", df2);


    // export result to parquet
    let par_f_path = "/Users/matthewmartin/test_dummy_data/fd/rust_par.parquet";
    export_to_parquet(&mut df2, par_f_path);

}

//loads the CSV
fn load_csv_to_df(csv_f_path: &str) -> PolarsResult<DataFrame> {
    CsvReader::from_path(csv_f_path)?.has_header(true).finish()
}

// pushes the dataframe to a parquet file
fn export_to_parquet(df: &mut DataFrame, par_f_path: &str) {
    let mut file = std::fs::File::create(par_f_path).unwrap();
    ParquetWriter::new(&mut file).finish(df).unwrap();
}
