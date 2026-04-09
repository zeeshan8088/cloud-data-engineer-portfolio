# =============================================================================
# RetailFlow — Input Variables
# =============================================================================
# Actual values provided via terraform.tfvars
# =============================================================================

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
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "Environment must be one of: dev, staging, production."
  }
}

variable "data_retention_days" {
  description = "Number of days to retain raw data in GCS before auto-deletion"
  type        = number
  default     = 30

  validation {
    condition     = var.data_retention_days >= 7 && var.data_retention_days <= 365
    error_message = "Data retention must be between 7 and 365 days."
  }
}
