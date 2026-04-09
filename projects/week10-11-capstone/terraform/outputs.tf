# =============================================================================
# RetailFlow — Outputs
# =============================================================================
# Printed after terraform apply — useful for CI/CD and other systems.
# =============================================================================

# ── BigQuery ─────────────────────────────────────────────────────────────────

output "bigquery_dataset_ids" {
  description = "Map of all BigQuery dataset IDs"
  value       = module.bigquery.dataset_ids
}

output "bigquery_bronze_dataset" {
  description = "Bronze dataset ID"
  value       = module.bigquery.dataset_ids["retailflow_bronze"]
}

output "bigquery_silver_dataset" {
  description = "Silver dataset ID"
  value       = module.bigquery.dataset_ids["retailflow_silver"]
}

output "bigquery_gold_dataset" {
  description = "Gold dataset ID"
  value       = module.bigquery.dataset_ids["retailflow_gold"]
}

# ── Cloud Storage ────────────────────────────────────────────────────────────

output "bronze_bucket_name" {
  description = "GCS Bronze data lake bucket name"
  value       = module.storage.bronze_bucket_name
}

output "bronze_bucket_url" {
  description = "GCS Bronze bucket URL"
  value       = module.storage.bronze_bucket_url
}

output "ge_docs_bucket_name" {
  description = "GCS bucket for Great Expectations DataDocs"
  value       = module.storage.ge_docs_bucket_name
}

# ── Cloud Scheduler ──────────────────────────────────────────────────────────

output "scheduler_job_name" {
  description = "Cloud Scheduler daily ingestion job name"
  value       = module.scheduler.job_name
}

# ── IAM ──────────────────────────────────────────────────────────────────────

output "pipeline_service_account_email" {
  description = "Service account email for pipeline operations"
  value       = module.iam.service_account_email
}

# ── Meta ─────────────────────────────────────────────────────────────────────

output "environment" {
  description = "Current deployment environment"
  value       = var.environment
}

output "project_id" {
  description = "GCP project ID"
  value       = var.project_id
}
