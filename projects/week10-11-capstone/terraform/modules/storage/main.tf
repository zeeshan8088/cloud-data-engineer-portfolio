# =============================================================================
# RetailFlow — Cloud Storage Module
# =============================================================================
# Creates two GCS buckets:
#   1. Bronze Data Lake — raw ingested files (CSV, JSON) with versioning
#   2. GE DataDocs     — hosts Great Expectations HTML quality reports
# =============================================================================

# ── Bronze Data Lake Bucket ──────────────────────────────────────────────────

resource "google_storage_bucket" "bronze" {
  name     = "retailflow-bronze-${var.project_id}"
  location = var.region
  project  = var.project_id

  # Prevent accidental deletion of a bucket that contains production data
  force_destroy = var.environment == "dev" ? true : false

  # Uniform bucket-level access — simpler and more secure than per-object ACLs
  uniform_bucket_level_access = true

  # Versioning — keep previous versions so we can recover from accidental overwrites
  versioning {
    enabled = true
  }

  # Lifecycle: auto-delete raw files after retention period to control costs
  lifecycle_rule {
    condition {
      age = var.data_retention_days
    }
    action {
      type = "Delete"
    }
  }

  # Lifecycle: clean up old non-current (archived) versions after 7 days
  lifecycle_rule {
    condition {
      age        = 7
      with_state = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  labels = var.labels
}

# ── Great Expectations DataDocs Bucket ───────────────────────────────────────

resource "google_storage_bucket" "ge_docs" {
  name     = "retailflow-ge-docs-${var.project_id}"
  location = var.region
  project  = var.project_id

  force_destroy               = true
  uniform_bucket_level_access = true

  # DataDocs are regenerated on every pipeline run — no need for versioning
  versioning {
    enabled = false
  }

  # Auto-delete old reports after 14 days
  lifecycle_rule {
    condition {
      age = 14
    }
    action {
      type = "Delete"
    }
  }

  labels = var.labels
}
