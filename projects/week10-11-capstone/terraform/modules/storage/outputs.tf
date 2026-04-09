# =============================================================================
# Storage Module — Outputs
# =============================================================================

output "bronze_bucket_name" {
  description = "Name of the Bronze data lake bucket"
  value       = google_storage_bucket.bronze.name
}

output "bronze_bucket_url" {
  description = "GCS URL of the Bronze data lake bucket"
  value       = "gs://${google_storage_bucket.bronze.name}"
}

output "ge_docs_bucket_name" {
  description = "Name of the GE DataDocs bucket"
  value       = google_storage_bucket.ge_docs.name
}

output "ge_docs_bucket_url" {
  description = "GCS URL of the GE DataDocs bucket"
  value       = "gs://${google_storage_bucket.ge_docs.name}"
}
