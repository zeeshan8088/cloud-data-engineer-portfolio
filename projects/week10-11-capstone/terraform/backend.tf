# =============================================================================
# RetailFlow — Remote State Backend
# =============================================================================
# Uses the same GCS bucket as Week 8 with a separate prefix.
# Ensures Terraform state is stored centrally and supports team collaboration.
#
# First-time setup:
#   terraform init \
#     -backend-config="bucket=intricate-ward-459513-e1-terraform-state" \
#     -backend-config="prefix=retailflow/terraform/state"
# =============================================================================

terraform {
  backend "gcs" {
    bucket = "intricate-ward-459513-e1-terraform-state"
    prefix = "retailflow/terraform/state"
  }
}
