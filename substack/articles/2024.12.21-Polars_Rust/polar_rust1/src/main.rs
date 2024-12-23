use google_cloud_storage::client::Client;
use google_cloud_storage::client::ClientConfig;
use google_cloud_storage::http::Error;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use std::env;
use std::fs::File;
use std::io::Read;

// need to add in polars to create the data frame
// save it locally
// then push to gcs
// wonder if we can pull in the delta package and write to delta lake format here?
// maybe also create a gcs bucket

#[tokio::main]
async fn main() -> Result<(), Error> {

    let curr_dir = env::var("PWD").expect("Error getting current directory");
    let par_f_path = format!("{}/tmp/data.parquet", curr_dir);
    let gcs_key_path = "testing/abc/blah/data.parquet";

    upload_file_to_gcs(&par_f_path, &gcs_key_path).await?;

    Ok(())
}

async fn upload_file_to_gcs(local_f_path: &str, gcs_key_path: &str) -> Result<(), Error>{

    // Create client.
    let config = ClientConfig::default().with_auth().await.unwrap();
    let client = Client::new(config);

    let bucket_nm = env::var("GCS_BUCKET").expect("Error retrieving bucket");

    // read the local file into byte stream to upload
    let mut f = File::open(local_f_path).expect("failed to open file");
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer).expect("error reading file");

    //stupid rust borrowring, or a skill issue on my part :P
    let owned_gcs_key_path = gcs_key_path.to_owned();

    // Upload the file
    let upload_type = UploadType::Simple(Media::new(owned_gcs_key_path));
    let _uploaded = client.upload_object(&UploadObjectRequest {
        bucket: bucket_nm,
        ..Default::default()
    }, buffer, &upload_type).await;
    

    Ok(())

}

