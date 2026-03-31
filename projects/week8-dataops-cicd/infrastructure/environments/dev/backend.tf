terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "intricate-ward-459513-e1-terraform-state"
    prefix = "week8/environments/dev"
  }
}