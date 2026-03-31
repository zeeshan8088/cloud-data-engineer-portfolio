module "pipeline" {
  source = "../../modules/pipeline"

  project_id               = var.project_id
  region                   = var.region
  environment              = "dev"
  pipeline_bucket_name     = "intricate-ward-459513-e1-week8-pipeline-dev-v2"
  bigquery_dataset_id      = "week8_pipeline_dev_v2"
  bigquery_location        = "asia-south1"
  data_retention_days      = 30
  enable_delete_protection = false
  alert_email              = "zeeshanyalakpalli@gmail.com"
}

output "pipeline_bucket_url" {
  value = module.pipeline.pipeline_bucket_url
}

output "bigquery_table_id" {
  value = module.pipeline.bigquery_table_id
}