variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "asia-south1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, production)"
  type        = string

  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "Environment must be one of: dev, staging, production."
  }
}

variable "pipeline_bucket_name" {
  description = "Name of the GCS bucket for pipeline data"
  type        = string
}

variable "bigquery_dataset_id" {
  description = "BigQuery dataset ID for transformed data"
  type        = string
}

variable "bigquery_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "asia-south1"
}

variable "data_retention_days" {
  description = "Number of days to retain data in GCS before deletion"
  type        = number
  default     = 30
}

variable "enable_delete_protection" {
  description = "Prevent accidental deletion of BigQuery tables"
  type        = bool
  default     = false
}