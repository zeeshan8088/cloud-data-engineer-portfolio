# =============================================================================
# Storage Module — Variables
# =============================================================================

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCS bucket location"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "labels" {
  description = "Common labels to apply to all buckets"
  type        = map(string)
  default     = {}
}

variable "data_retention_days" {
  description = "Days to retain raw data before auto-deletion"
  type        = number
  default     = 30
}
