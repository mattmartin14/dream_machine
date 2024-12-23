
/*
    Author: Matt Martin
    Date: 12/19/24
    Desc: Polars/Rust/GCS Test
*/


use polars::prelude::*;
use std::error::Error;
use std::env;
use google_cloud_storage::client::{ClientConfig, Client};
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::http::Error;


#[tokio::main]
fn main() -> Result<(), Box<dyn Error>> {

    // let home_dir = env::var("HOME")?;
    // let csv_f_path = format!("{}/test_dummy_data/fd/fin_data_1.csv", home_dir);

    //read raw data
    //let df = load_csv_to_df(&csv_f_path)?;
    
    let lf = make_dummy_lf();
   
    //transform
    let mut df = lf
        .group_by([col("txn_dt")])
        .agg([
            col("last_nm").count().alias("lm_cnt"),
            col("tot_amt").sum().alias("tot_net_worth")
        ])
        .collect()
        .expect("Error getting dataframe created")
    ;

    //sample top 5 rows
    println!("{}", df.head(Some(5)));


    // export to parquet
    let curr_dir = env::var("PWD")?;
    let par_f_path = format!("{}/tmp/data.parquet", curr_dir);
    //println!("{}", par_f_path);
    export_to_parquet(&mut df, &par_f_path)?;

    // Load to GCS
    
    // let gcs_dir = format!("gs://{}/polars_rust_warehouse/",bucket);
    // let par_f_path = format!("{}/rust_par.parquet", gcs_dir);
    // export_to_parquet(&mut df, &par_f_path)?;
    
    // might need to make this stuff async: https://docs.rs/google-cloud-storage/latest/google_cloud_storage/

    upload_to_gcs(&par_f_path);

    Ok(())
}

// // import the csv
// fn load_csv_to_df(csv_f_path: &str) -> Result<DataFrame, PolarsError> {
//     CsvReader::from_path(csv_f_path)?.has_header(true).finish()
// }

fn make_dummy_lf() -> LazyFrame {
    let txn_dt = Series::new(
        "txn_dt".into(),
        [
            "2024-12-10", "2024-12-11", "2024-12-12", "2024-12-13", 
            "2024-12-14", "2024-12-15", "2024-12-16", "2024-12-17",
            "2024-12-18", "2024-12-19",
        ],
    );
    let tot_amt = Series::new(
        "tot_amt".into(), 
        [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
    );
    let last_nm = Series::new(
        "last_nm".into(), 
        [
            "Smith", "Johnson", "Williams", "Brown", "Jones", 
            "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
        ],
    );

    let df = DataFrame::new(vec![txn_dt.into(), tot_amt.into(), last_nm.into()]).unwrap();
    df.lazy()
}


//exports the dataframe to parquet
fn export_to_parquet(df: &mut DataFrame, par_f_path: &str) -> Result<(), PolarsError> {
    let mut file = std::fs::File::create(par_f_path)?;
    ParquetWriter::new(&mut file).finish(df)?;
    Ok(())
}

async fn upload_to_gcs(f_path: &str) -> Result<(), google_cloud_storage::http::Error> {

    //auth to gcs with default creds
    let config = ClientConfig::default().with_auth().await.unwrap();
    let client = Client::new(config);

    // Upload the file
    let bucket_nm = env::var("GCS_BUCKET");
    let upload_type = UploadType::Simple(Media::new(f_path));
    let uploaded = client.upload_object(&UploadObjectRequest {
        bucket: bucket_nm.to_string(),
        ..Default::default()
    }, "hello world".as_bytes(), &upload_type).await;

    Ok(())

}