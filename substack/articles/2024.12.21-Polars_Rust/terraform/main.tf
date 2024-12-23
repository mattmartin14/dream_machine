
module "gcs_bucket" {
  providers = {
    google = google
  }
  
  source = "./modules/gcs/"
  bucket_name = "matts-super-secret-rust-bucket-123"
  region = local.prime_region
  tags = local.tags

}
