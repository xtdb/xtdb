terraform {
  required_version = ">=1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.43.0, < 7"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5.0, < 4.0.0"
    }
  }
}
