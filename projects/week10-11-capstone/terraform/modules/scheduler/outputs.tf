# =============================================================================
# Scheduler Module — Outputs
# =============================================================================

output "job_name" {
  description = "Name of the Cloud Scheduler job"
  value       = google_cloud_scheduler_job.daily_ingest.name
}

output "job_schedule" {
  description = "Cron schedule of the job"
  value       = google_cloud_scheduler_job.daily_ingest.schedule
}
