# =============================================================================
# RetailFlow — BigQuery Module
# =============================================================================
# Creates all 5 datasets for the Medallion Architecture:
#   Bronze  → append-only raw data
#   Silver  → dbt staging models (cleaned, typed, deduplicated)
#   Gold    → business-ready mart tables
#   Predictions → Vertex AI batch prediction outputs
#   Metadata    → pipeline run logs, GE results, lineage tracking
# =============================================================================

locals {
  datasets = {
    retailflow_bronze = {
      friendly_name = "RetailFlow Bronze — Raw Data"
      description   = "Append-only ingested data from all sources. Never transformed directly."
    }
    retailflow_silver = {
      friendly_name = "RetailFlow Silver — Staging"
      description   = "dbt staging models: cleaned, typed, deduplicated views of Bronze data."
    }
    retailflow_gold = {
      friendly_name = "RetailFlow Gold — Marts"
      description   = "Business-ready aggregated tables for dashboards and analytics."
    }
    retailflow_predictions = {
      friendly_name = "RetailFlow Predictions — AI/ML"
      description   = "Vertex AI AutoML batch prediction outputs and evaluation metrics."
    }
    retailflow_metadata = {
      friendly_name = "RetailFlow Metadata — Ops"
      description   = "Pipeline run logs, Great Expectations results, lineage tracking."
    }
  }
}

resource "google_bigquery_dataset" "datasets" {
  for_each = local.datasets

  dataset_id    = each.key
  friendly_name = "${each.value.friendly_name} (${var.environment})"
  description   = each.value.description
  location      = var.region
  project       = var.project_id

  # Safe for dev — prevents orphaned tables blocking terraform destroy
  delete_contents_on_destroy = var.environment == "dev" ? true : false

  # Default table expiration: none (we manage lifecycle ourselves)
  # default_table_expiration_ms = null

  labels = var.labels
}
