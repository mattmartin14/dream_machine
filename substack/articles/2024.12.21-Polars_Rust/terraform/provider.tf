terraform {
  required_version = ">= 1.5.0"  

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.80"  
    }
  }
}

provider "google" {
  region  = "us-east1"            
}
