# =============================================================================
# Provider Configuration
# Tells Terraform which GCP project and region to use
# =============================================================================

provider "google" {
  project = var.project_id
  region  = var.region
}

# =============================================================================
# Local Values
# Computed values used across multiple resources
# Like variables but derived from other values
# =============================================================================

locals {
  # Common labels applied to every resource we create
  # Labels help you find, filter, and track costs in GCP
  common_labels = {
    environment = var.environment
    project     = "week8-dataops-cicd"
    managed_by  = "terraform"
    owner       = "zeeshan"
  }
}

# =============================================================================
# Enable Required GCP APIs
# Terraform can enable APIs just like gcloud services enable
# =============================================================================

resource "google_project_service" "bigquery_api" {
  project = var.project_id
  service = "bigquery.googleapis.com"

  # Don't disable the API if we destroy this Terraform resource
  # Other things might depend on it
  disable_on_destroy = false
}

resource "google_project_service" "storage_api" {
  project = var.project_id
  service = "storage.googleapis.com"

  disable_on_destroy = false
}

# =============================================================================
# GCS Bucket — Pipeline Data Storage
# =============================================================================

resource "google_storage_bucket" "pipeline_bucket" {
  name     = var.pipeline_bucket_name
  location = var.region
  project  = var.project_id

  # Prevent accidental deletion of a bucket that contains data
  force_destroy = false

  # Uniform bucket-level access — simpler, more secure than per-object ACLs
  uniform_bucket_level_access = true

  # Versioning — keep previous versions of objects
  versioning {
    enabled = true
  }

  # Lifecycle rule — automatically delete old objects to save cost
  lifecycle_rule {
    condition {
      age = var.data_retention_days
    }
    action {
      type = "Delete"
    }
  }

  # Lifecycle rule — delete old non-current versions after 7 days
  lifecycle_rule {
    condition {
      age                = 7
      with_state         = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  labels = local.common_labels

  # This resource depends on the Storage API being enabled first
  depends_on = [google_project_service.storage_api]
}

# =============================================================================
# BigQuery Dataset — Transformed Data
# =============================================================================

resource "google_bigquery_dataset" "pipeline_dataset" {
  dataset_id    = var.bigquery_dataset_id
  friendly_name = "Week 8 Pipeline Dataset (${var.environment})"
  description   = "Dataset for week8 DataOps CI/CD pipeline - managed by Terraform"
  location      = var.bigquery_location
  project       = var.project_id

  # Delete the dataset even if it contains tables (safe for dev)
  # Set to false in production
  delete_contents_on_destroy = var.environment == "dev" ? true : false

  labels = local.common_labels

  depends_on = [google_project_service.bigquery_api]
}

# =============================================================================
# BigQuery Table — Processed Orders
# Schema matches our ETL pipeline output exactly
# =============================================================================

resource "google_bigquery_table" "processed_orders" {
  dataset_id = google_bigquery_dataset.pipeline_dataset.dataset_id
  table_id   = "processed_orders"
  project    = var.project_id

  description = "Transformed order records from the week8 ETL pipeline"

  # Partition by processed_at date — makes queries cheaper and faster
  time_partitioning {
    type  = "DAY"
    field = "processed_at"
  }

  # Schema matches transform_order() output in etl_pipeline.py
  schema = jsonencode([
    {
      name        = "order_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Unique order identifier"
    },
    {
      name        = "customer_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Customer identifier"
    },
    {
      name        = "items"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Number of items in order"
    },
    {
      name        = "unit_price"
      type        = "FLOAT"
      mode        = "REQUIRED"
      description = "Price per unit in INR"
    },
    {
      name        = "total_value"
      type        = "FLOAT"
      mode        = "REQUIRED"
      description = "Total order value (items * unit_price)"
    },
    {
      name        = "is_high_value"
      type        = "BOOLEAN"
      mode        = "REQUIRED"
      description = "True if total_value >= HIGH_VALUE_THRESHOLD"
    },
    {
      name        = "status"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Order status in uppercase (COMPLETED, PENDING, CANCELLED)"
    },
    {
      name        = "processed_at"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "UTC timestamp when record was processed"
    },
    {
      name        = "environment"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Environment where pipeline ran (local, ci, production)"
    }
  ])

  depends_on = [google_bigquery_dataset.pipeline_dataset]
}