output "pipeline_bucket_name" {
  description = "Name of the GCS pipeline bucket"
  value       = google_storage_bucket.pipeline_bucket.name
}

output "pipeline_bucket_url" {
  description = "GCS URL of the pipeline bucket"
  value       = "gs://${google_storage_bucket.pipeline_bucket.name}"
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.pipeline_dataset.dataset_id
}

output "bigquery_table_id" {
  description = "Full BigQuery table ID"
  value       = "${var.project_id}.${google_bigquery_dataset.pipeline_dataset.dataset_id}.${google_bigquery_table.processed_orders.table_id}"
}

output "environment" {
  description = "Current deployment environment"
  value       = var.environment
}