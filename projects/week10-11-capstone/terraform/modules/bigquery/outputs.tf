# =============================================================================
# BigQuery Module — Outputs
# =============================================================================

output "dataset_ids" {
  description = "Map of dataset name → dataset ID"
  value       = { for k, v in google_bigquery_dataset.datasets : k => v.dataset_id }
}

output "dataset_self_links" {
  description = "Map of dataset name → self link"
  value       = { for k, v in google_bigquery_dataset.datasets : k => v.self_link }
}
