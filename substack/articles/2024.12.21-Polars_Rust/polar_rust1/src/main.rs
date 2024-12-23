mod gcs;
use std::env;
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // read raw csv data into dataframe
    let home_dir = env::var("HOME").expect("Error retrieving home directory");
    let csv_f_path = format!("{}/test_dummy_data/rust/data*.csv", home_dir);
    let lf = read_csv(&csv_f_path);

    println!("Read csv raw data into dataframe");

    //transform
    let mut df = lf
        .group_by([col("ref_dt")])
        .agg([
            col("first_name").count().alias("fm_cnt"),
            col("last_name").count().alias("lm_cnt"),
            col("net_worth").sum().alias("tot_net_worth")
        ])
        .collect()
        .expect("Error getting dataframe created")
    ;

    //sample top 5 rows
    println!("{}", df.head(Some(5)));

    //validate row count (should be 100k)
    let lf2 = read_csv(&csv_f_path);
    let df_cnt = lf2.collect().expect("Error getting total count");
    let total_rows = df_cnt.height();
    println!("{} total rows", total_rows);

    let curr_dir = env::var("PWD").expect("Error getting current directory");
    let par_f_path = format!("{}/tmp/data.parquet", curr_dir);
    
    export_to_parquet(&mut df, &par_f_path).expect("error exporting dataframe to parquet");

    let client = gcs::create_gcs_client().await;

    let bucket_nm = "matts-super-secret-rust-bucket-123";

    let gcs_key_path = "agg_dataset/data.parquet";
    gcs::upload_file_to_gcs(&client, &bucket_nm, &par_f_path, &gcs_key_path).await?;

    Ok(())
}

fn export_to_parquet(df: &mut DataFrame, par_f_path: &str) -> Result<(), PolarsError> {
    let mut file = std::fs::File::create(par_f_path)?;
    ParquetWriter::new(&mut file).finish(df)?;
    Ok(())
}

fn read_csv(file_path: &str) -> LazyFrame {
    LazyCsvReader::new(file_path)
        .with_has_header(true)
        .finish()
        .expect("Failed to read CSV file into LazyFrame")
}
