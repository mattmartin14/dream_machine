use google_cloud_storage::client::Client;
use google_cloud_storage::client::ClientConfig;
use google_cloud_storage::http::Error;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use std::env;
use polars::prelude::*;
//use std::fs::File;
//use std::io::Read;
//use std::fs::read;

// need to add in polars to create the data frame
// save it locally
// then push to gcs
// wonder if we can pull in the delta package and write to delta lake format here?
// maybe also create a gcs bucket

#[tokio::main]
async fn main() -> Result<(), Error> {

    let curr_dir = env::var("PWD").expect("Error getting current directory");
    let par_f_path = format!("{}/tmp/data.parquet", curr_dir);
    let gcs_key_path = "testing/abc/blah2/data.parquet";

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
    
    export_to_parquet(&mut df, &par_f_path).expect("error exporting dataframe to parquet");

    let client = create_gcs_client().await;

    upload_file_to_gcs(&client, &par_f_path, &gcs_key_path).await?;

    Ok(())
}

async fn create_gcs_client() -> Client {
    let config = ClientConfig::default().with_auth().await.unwrap();
    Client::new(config)
}


async fn upload_file_to_gcs(client: &Client, local_f_path: &str, gcs_key_path: &str) -> Result<(), Error>{

    let bucket_nm = env::var("GCS_BUCKET").expect("Error retrieving bucket");

    // read the local file into byte stream to upload
    let buffer = std::fs::read(local_f_path).expect("Failed to read file");

    // Upload the file
    let upload_type = UploadType::Simple(Media::new(gcs_key_path.to_owned()));
    let _uploaded = client.upload_object(&UploadObjectRequest {
            bucket: bucket_nm,
            ..Default::default()
        }, 
        buffer, 
        &upload_type
    ).await;
    
    Ok(())

}

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

fn export_to_parquet(df: &mut DataFrame, par_f_path: &str) -> Result<(), PolarsError> {
    let mut file = std::fs::File::create(par_f_path)?;
    ParquetWriter::new(&mut file).finish(df)?;
    Ok(())
}
