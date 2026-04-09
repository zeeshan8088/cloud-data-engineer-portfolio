# =============================================================================
# IAM Module — Outputs
# =============================================================================

output "service_account_email" {
  description = "Email of the RetailFlow pipeline service account"
  value       = google_service_account.pipeline_sa.email
}

output "service_account_id" {
  description = "Fully qualified ID of the service account"
  value       = google_service_account.pipeline_sa.id
}

output "service_account_name" {
  description = "Fully qualified name of the service account"
  value       = google_service_account.pipeline_sa.name
}
