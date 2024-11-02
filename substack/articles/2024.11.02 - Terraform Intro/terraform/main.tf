

module "gcs_bucket_east" {
  providers = {
    google = google
  }
  
  source = "./modules/gcs/"
  bucket_name = "test-gcs-matt-martin-super-secret-bucket-${local.prime_region}-123"
  region = local.prime_region
  tags = local.tags

}

module "gcs_bucket_west" {
  providers = {
    google = google.west
  }
  
  source = "./modules/gcs/"
  bucket_name = "test-gcs-matt-martin-super-secret-bucket-${local.sec_region}-123"
  region = local.sec_region
  tags = local.tags

}