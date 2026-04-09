# =============================================================================
# RetailFlow — Root Terraform Configuration
# =============================================================================
# Infrastructure as Code for the entire RetailFlow data platform.
# Creates: 5 BigQuery datasets, 2 GCS buckets, Cloud Scheduler jobs,
#          IAM service account with least-privilege bindings.
#
# Usage:
#   terraform init
#   terraform plan -var-file="terraform.tfvars"
#   terraform apply -var-file="terraform.tfvars"
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# ── Provider ─────────────────────────────────────────────────────────────────

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── Local Values ─────────────────────────────────────────────────────────────

locals {
  common_labels = {
    project     = "retailflow"
    environment = var.environment
    managed_by  = "terraform"
    owner       = "zeeshan"
    week        = "10-11-capstone"
  }
}

# =============================================================================
# Enable Required GCP APIs
# =============================================================================

resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudfunctions.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "cloudbuild.googleapis.com",
    "aiplatform.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# =============================================================================
# Module: BigQuery — 5 Medallion Architecture Datasets
# =============================================================================

module "bigquery" {
  source = "./modules/bigquery"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment
  labels      = local.common_labels

  depends_on = [google_project_service.apis]
}

# =============================================================================
# Module: Cloud Storage — Bronze Data Lake + GE DataDocs
# =============================================================================

module "storage" {
  source = "./modules/storage"

  project_id          = var.project_id
  region              = var.region
  environment         = var.environment
  labels              = local.common_labels
  data_retention_days = var.data_retention_days

  depends_on = [google_project_service.apis]
}

# =============================================================================
# Module: Cloud Scheduler — Daily Ingestion Triggers
# =============================================================================

module "scheduler" {
  source = "./modules/scheduler"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment

  depends_on = [google_project_service.apis]
}

# =============================================================================
# Module: IAM — Service Account + Role Bindings
# =============================================================================

module "iam" {
  source = "./modules/iam"

  project_id  = var.project_id
  environment = var.environment

  depends_on = [google_project_service.apis]
}
