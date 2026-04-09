# =============================================================================
# RetailFlow — IAM Module
# =============================================================================
# Creates a dedicated service account for the RetailFlow pipeline and grants
# least-privilege IAM roles for BigQuery, Cloud Storage, Pub/Sub, and
# Cloud Functions.
#
# Principle: each pipeline gets its own service account rather than using
# the default compute SA.  This limits blast radius if credentials leak.
# =============================================================================

# ── Service Account ──────────────────────────────────────────────────────────

resource "google_service_account" "pipeline_sa" {
  account_id   = "retailflow-pipeline-sa"
  display_name = "RetailFlow Pipeline Service Account (${var.environment})"
  description  = "Dedicated SA for RetailFlow data pipeline — BigQuery, GCS, Pub/Sub, Cloud Functions"
  project      = var.project_id
}

# ── IAM Role Bindings ────────────────────────────────────────────────────────
# Each binding grants exactly ONE role to the service account.
# We use google_project_iam_member (additive) instead of
# google_project_iam_binding (authoritative) to avoid clobbering
# existing bindings on the project.

locals {
  pipeline_roles = [
    "roles/bigquery.dataEditor",    # Read/write BigQuery tables
    "roles/bigquery.jobUser",       # Run BigQuery jobs (queries, loads)
    "roles/storage.objectAdmin",    # Read/write/delete GCS objects
    "roles/pubsub.publisher",       # Publish clickstream events to Pub/Sub
    "roles/pubsub.subscriber",      # Subscribe to Pub/Sub topics
    "roles/cloudfunctions.invoker", # Invoke Cloud Functions via HTTP
    "roles/run.invoker",            # Invoke Cloud Run services (Gen2 functions)
    "roles/aiplatform.user",        # Submit Vertex AI training/prediction jobs
  ]
}

resource "google_project_iam_member" "pipeline_roles" {
  for_each = toset(local.pipeline_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
