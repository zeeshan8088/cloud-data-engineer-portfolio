# =============================================================================
# Cloud Monitoring — Pipeline Observability
# =============================================================================

# -----------------------------------------------------------------------------
# 1. Notification Channel — Email
# -----------------------------------------------------------------------------
resource "google_monitoring_notification_channel" "email" {
  display_name = "Pipeline Alerts - ${var.environment}"
  type         = "email"
  project      = var.project_id

  labels = {
    email_address = var.alert_email
  }

  force_delete = true
}

# -----------------------------------------------------------------------------
# 2. Alert Policy — ETL Pipeline Errors (Log-Matched)
# Directly watches logs for ERRORs - bypasses metric resource constraints
# -----------------------------------------------------------------------------
resource "google_monitoring_alert_policy" "pipeline_errors" {
  display_name = "Week8 ETL Pipeline Errors - ${var.environment}"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "ETL pipeline error log entries detected"

    condition_matched_log {
      filter = <<-EOT
        severity="ERROR"
        (
          textPayload=~"etl_pipeline|Pipeline|ETL" OR
          jsonPayload.message=~"etl_pipeline|Pipeline|ETL"
        )
      EOT
    }
  }

  alert_strategy {
    notification_rate_limit {
      period = "300s"
    }
  }

  notification_channels = [
    google_monitoring_notification_channel.email.name
  ]

  documentation {
    content   = <<-EOT
      ## ETL Pipeline Error Alert

      **Environment:** ${var.environment}
      **Project:** ${var.project_id}

      An error was detected in the Week 8 ETL pipeline.

      ### Immediate steps:
      1. Check Cloud Logging for recent ERROR entries
      2. Look for `Pipeline|ETL|etl_pipeline` in log messages
      3. Check the Docker container exit code
      4. Review recent CI/CD deployments for changes

      ### Useful links:
      - [Cloud Logging](https://console.cloud.google.com/logs/query?project=${var.project_id})
      - [Artifact Registry](https://console.cloud.google.com/artifacts?project=${var.project_id})
    EOT
    mime_type = "text/markdown"
  }

  user_labels = local.common_labels

  depends_on = [
    google_monitoring_notification_channel.email
  ]
}

# -----------------------------------------------------------------------------
# 3. Alert Policy — High Log Error Rate (Global Project-Wide)
# -----------------------------------------------------------------------------
resource "google_monitoring_alert_policy" "high_error_rate" {
  display_name = "Week8 Pipeline High Log Error Rate - ${var.environment}"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "Error log rate exceeds threshold"

    condition_threshold {
      filter = <<-EOT
        metric.type="logging.googleapis.com/log_entry_count"
        resource.type="global"
        metric.labels.severity="ERROR"
      EOT

      aggregations {
        alignment_period   = "600s"
        per_series_aligner = "ALIGN_RATE"
      }

      comparison      = "COMPARISON_GT"
      threshold_value = 0.1
      duration        = "300s"

      trigger {
        count = 1
      }
    }
  }

  notification_channels = [
    google_monitoring_notification_channel.email.name
  ]

  documentation {
    content   = "High error rate detected in project ${var.project_id} (${var.environment}). Check Cloud Logging immediately."
    mime_type = "text/markdown"
  }

  user_labels = local.common_labels
}

# -----------------------------------------------------------------------------
# 4. Monitoring Dashboard
# -----------------------------------------------------------------------------
resource "google_monitoring_dashboard" "pipeline_dashboard" {
  project        = var.project_id
  dashboard_json = jsonencode({
    displayName = "Week8 ETL Pipeline Dashboard - ${var.environment}"
    mosaicLayout = {
      columns = 12
      tiles = [
        {
          width  = 6
          height = 4
          widget = {
            title = "Project-Wide Error Rate (Log Count)"
            xyChart = {
              dataSets = [{
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"logging.googleapis.com/log_entry_count\" resource.type=\"global\" metric.label.severity=\"ERROR\""
                    aggregation = {
                      alignmentPeriod  = "300s"
                      perSeriesAligner = "ALIGN_RATE"
                    }
                  }
                }
                plotType = "LINE"
              }]
              yAxis = {
                label = "Errors/sec"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          xPos   = 6
          width  = 6
          height = 4
          widget = {
            title = "Total Log Entries (Pipeline Activity)"
            xyChart = {
              dataSets = [{
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"logging.googleapis.com/log_entry_count\" resource.type=\"global\""
                    aggregation = {
                      alignmentPeriod  = "300s"
                      perSeriesAligner = "ALIGN_RATE"
                    }
                  }
                }
                plotType = "LINE"
              }]
              yAxis = {
                label = "Log Entries/sec"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          yPos   = 4
          width  = 6
          height = 4
          widget = {
            title = "BigQuery Storage (bytes)"
            xyChart = {
              dataSets = [{
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"bigquery.googleapis.com/storage/stored_bytes\" resource.type=\"bigquery_dataset\""
                    aggregation = {
                      alignmentPeriod  = "3600s"
                      perSeriesAligner = "ALIGN_MEAN"
                    }
                  }
                }
                plotType = "LINE"
              }]
              yAxis = {
                label = "Bytes"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          xPos   = 6
          yPos   = 4
          width  = 6
          height = 4
          widget = {
            title = "GCS Pipeline Bucket - Object Count"
            xyChart = {
              dataSets = [{
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"storage.googleapis.com/storage/object_count\" resource.type=\"gcs_bucket\" resource.label.\"bucket_name\"=\"${var.pipeline_bucket_name}\""
                    aggregation = {
                      alignmentPeriod  = "3600s"
                      perSeriesAligner = "ALIGN_MEAN"
                    }
                  }
                }
                plotType = "LINE"
              }]
              yAxis = {
                label = "Object Count"
                scale = "LINEAR"
              }
            }
          }
        }
      ]
    }
  })
}