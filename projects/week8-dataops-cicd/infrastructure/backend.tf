# =============================================================================
# Remote Backend Configuration
# Stores Terraform state in GCS instead of locally
# This means:
#   - State is shared (teammates see the same state)
#   - State is backed up (versioning enabled on the bucket)
#   - State is never lost if your laptop dies
# =============================================================================

terraform {
  # Minimum Terraform version required
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "intricate-ward-459513-e1-terraform-state"
    prefix = "week8/state"
  }
}