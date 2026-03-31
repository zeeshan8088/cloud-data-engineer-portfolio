locals {
  common_labels = {
    environment = var.environment
    project     = "week8-dataops-cicd"
    managed_by  = "terraform"
    owner       = "zeeshan"
  }
}

resource "google_project_service" "bigquery_api" {
  project            = var.project_id
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage_api" {
  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_storage_bucket" "pipeline_bucket" {
  name                        = var.pipeline_bucket_name
  location                    = var.region
  project                     = var.project_id
  force_destroy               = var.environment == "dev" ? true : false
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = var.data_retention_days
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age        = 7
      with_state = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  labels     = local.common_labels
  depends_on = [google_project_service.storage_api]
}

resource "google_bigquery_dataset" "pipeline_dataset" {
  dataset_id                 = var.bigquery_dataset_id
  friendly_name              = "Week 8 Pipeline Dataset (${var.environment})"
  description                = "Dataset for week8 DataOps CI/CD pipeline - managed by Terraform"
  location                   = var.bigquery_location
  project                    = var.project_id
  delete_contents_on_destroy = var.environment == "dev" ? true : false
  labels                     = local.common_labels
  depends_on                 = [google_project_service.bigquery_api]
}

resource "google_bigquery_table" "processed_orders" {
  dataset_id          = google_bigquery_dataset.pipeline_dataset.dataset_id
  table_id            = "processed_orders"
  project             = var.project_id
  description         = "Transformed order records from the week8 ETL pipeline"
  deletion_protection = var.enable_delete_protection

  time_partitioning {
    type  = "DAY"
    field = "processed_at"
  }

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