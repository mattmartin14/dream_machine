use google_cloud_storage::client::Client;
use google_cloud_storage::client::ClientConfig;
use google_cloud_storage::http::Error;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::http::buckets::insert::{BucketCreationConfig, InsertBucketParam, InsertBucketRequest};
use std::env;

pub async fn create_gcs_client() -> Client {
    let config = ClientConfig::default().with_auth().await.unwrap();
    Client::new(config)
}


pub async fn upload_file_to_gcs(client: &Client, bucket_nm: &str, local_f_path: &str, gcs_key_path: &str) -> Result<(), Error>{

    // read the local file into byte stream to upload
    let buffer = std::fs::read(local_f_path).expect("Failed to read file");

    // Upload the file
    let upload_type = UploadType::Simple(Media::new(gcs_key_path.to_owned()));
    let result = client.upload_object(&UploadObjectRequest {
            bucket: bucket_nm.to_string(),
            ..Default::default()
        }, 
        buffer, 
        &upload_type
    ).await;

    // Check the result and print the outcome
    match result {
        // Ok(upload_response) => {
        //     // Print the response on success
        //     println!("Upload successful: {:#?}", upload_response);
        // }
        Err(err) => {
            // Print the error in case of failure
            eprintln!("Upload failed: {:#?}", err);
            return Err(err); // Propagate the error
        }
        Ok(_) => {
            
        }

    }

    Ok(())

}

// creates a bucket but doesnt disable public access...going to stick with terraform for this since its easier
#[allow(dead_code)]
pub async fn create_gcs_bucket(client: &Client, bucket_nm: &str) -> Result<(), Error>{
    
    
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