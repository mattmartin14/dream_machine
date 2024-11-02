
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

  labels = merge(var.tags)

  
}