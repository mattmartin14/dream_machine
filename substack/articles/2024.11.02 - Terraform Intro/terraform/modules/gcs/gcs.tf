
terraform {
    required_providers {
      google = {
        configuration_aliases = [ google ]
      }
    }
}

resource "google_storage_bucket" "gcs_bucket" {
    
  name     = var.bucket_name  
  location = var.region                     
  force_destroy = true          

  labels = merge(var.tags)

  storage_class = "STANDARD"      

  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }

  # this nukes any objects older than 30 days
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30                        
    }
  }

  #disable public access unless you want some cryptobro using your bucket for bitcoin storage
  public_access_prevention = "enforced"

}