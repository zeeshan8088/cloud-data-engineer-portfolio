# =============================================================================
# Scheduler Module — Variables
# =============================================================================

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for the scheduler job"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}
