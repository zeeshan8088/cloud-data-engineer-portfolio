# =============================================================================
# RetailFlow — Cloud Scheduler Module
# =============================================================================
# Creates a Cloud Scheduler job that triggers the daily ingestion pipeline.
# In production, this would invoke a Cloud Function or Airflow DAG trigger.
# =============================================================================

resource "google_cloud_scheduler_job" "daily_ingest" {
  name        = "retailflow-daily-ingest-${var.environment}"
  description = "Triggers RetailFlow data ingestion pipeline daily at midnight IST"
  project     = var.project_id
  region      = var.region

  # Midnight IST = 18:30 UTC (previous day)
  # Cron expression: minute hour day-of-month month day-of-week
  schedule  = "30 18 * * *"
  time_zone = "Asia/Kolkata"

  # Retry configuration — up to 3 retries with 10-minute backoff
  retry_config {
    retry_count          = 3
    min_backoff_duration = "600s"
    max_backoff_duration = "3600s"
    max_doublings        = 2
  }

  # HTTP target — placeholder URL (will be updated when Cloud Function is deployed)
  # For now, points to a non-operational endpoint to demonstrate the Terraform config.
  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-${var.project_id}.cloudfunctions.net/retailflow-ingest-trigger"

    headers = {
      Content-Type = "application/json"
    }

    body = base64encode(jsonencode({
      pipeline    = "retailflow"
      environment = var.environment
      trigger     = "cloud_scheduler"
    }))

    # Use OIDC token for authentication (service account identity)
    oidc_token {
      service_account_email = "retailflow-pipeline-sa@${var.project_id}.iam.gserviceaccount.com"
      audience              = "https://${var.region}-${var.project_id}.cloudfunctions.net/retailflow-ingest-trigger"
    }
  }
}
