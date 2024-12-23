use google_cloud_storage::client::Client;
use google_cloud_storage::client::ClientConfig;
use google_cloud_storage::http::Error;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::http::buckets::insert::{BucketCreationConfig, InsertBucketParam, InsertBucketRequest};
use std::env;
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Error> {

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

    let curr_dir = env::var("PWD").expect("Error getting current directory");
    let par_f_path = format!("{}/tmp/data.parquet", curr_dir);
    
    export_to_parquet(&mut df, &par_f_path).expect("error exporting dataframe to parquet");

    let client = create_gcs_client().await;

    let bucket_nm = "matts-super-secret-rust-bucket-123";

    create_gcs_bucket(&client, &bucket_nm).await?;

    let gcs_key_path = "agg_dataset/data.parquet";
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


async fn create_gcs_bucket(client: &Client, bucket_nm: &str) -> Result<(), Error>{
    
    
    let project_id = env::var("GOOGLE_CLOUD_PROJECT").expect("Error retrieving gcp project");

    let config = BucketCreationConfig {
        location: "US".to_string(),
        ..Default::default()
    };


    let _result = client.insert_bucket(&InsertBucketRequest {
        name: bucket_nm.to_string(),
        param: InsertBucketParam {
            project: project_id,
            ..Default::default()
        },
        bucket: config,
        ..Default::default()
    }).await?;
    
    Ok(())


}