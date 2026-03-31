# Week 8: DataOps, CI/CD, Docker & Infrastructure as Code

## Overview
A production-grade DataOps setup for an e-commerce ETL pipeline, featuring
multi-stage Docker builds, GitHub Actions CI/CD, GCP Artifact Registry,
Terraform IaC, and Cloud Monitoring — all wired together automatically.

## Architecture
```
git push
    ↓
GitHub Actions CI Pipeline
    ├── Job 1: Lint (ruff) + Test (pytest, 86% coverage)
    ├── Job 2: Build & Push Docker → GCP Artifact Registry
    └── Job 3: Terraform Plan (infrastructure diff)

GCP Infrastructure (Terraform)
    ├── GCS Bucket       — pipeline data storage (versioned, lifecycle rules)
    ├── BigQuery Dataset — week8_pipeline_dev_v2
    ├── BigQuery Table   — processed_orders (DAY partitioned)
    └── Cloud Monitoring
        ├── Log-based metric  — ETL error counter
        ├── Alert policies    — email on errors
        └── Dashboard         — 4-panel pipeline health view
```

## Services Used
- **Docker** — multi-stage build (117MB image)
- **GitHub Actions** — CI/CD pipeline with 3 jobs
- **GCP Artifact Registry** — versioned Docker image storage
- **Workload Identity Federation** — keyless GCP authentication from CI
- **Terraform** — IaC with remote state in GCS, reusable modules
- **Cloud Monitoring** — log metrics, alert policies, dashboard
- **BigQuery** — partitioned table for processed orders
- **Cloud Storage** — pipeline data bucket with lifecycle rules

## Project Structure
```
week8-dataops-cicd/
├── etl/
│   ├── etl_pipeline.py        # ETL script (extract, transform, load)
│   ├── requirements.txt       # Pinned dependencies
│   └── tests/
│       └── test_etl_pipeline.py  # 12 unit tests, 86% coverage
├── infrastructure/
│   ├── modules/pipeline/      # Reusable Terraform module
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   └── monitoring.tf
│   └── environments/dev/      # Dev environment config
│       ├── main.tf
│       ├── backend.tf
│       ├── provider.tf
│       └── terraform.tfvars
├── .github/workflows/
│   └── ci.yml                 # 3-job CI pipeline
├── Dockerfile                 # Multi-stage build
├── docker-compose.yml         # Local development
└── Makefile                   # Developer shortcuts
```

## How to Run Locally
```bash
# Build and run the ETL pipeline in Docker
make build
make run

# Run tests
make test

# Run linter
make lint
```

## How to Deploy Infrastructure
```bash
cd infrastructure/environments/dev
terraform init
terraform plan
terraform apply
```

## CI/CD Pipeline
Every push to main automatically:
1. Lints code with ruff
2. Runs 12 pytest unit tests with 86% coverage
3. Builds Docker image with multi-stage build
4. Pushes image to GCP Artifact Registry (tagged with commit SHA)
5. Smoke tests the pushed image
6. Shows Terraform infrastructure plan

## Key Concepts Demonstrated
- **Multi-stage Docker builds** — 117MB vs 800MB+ single-stage
- **Workload Identity Federation** — no JSON keys, short-lived tokens
- **Terraform modules** — reusable infrastructure across environments
- **Remote state** — Terraform state in GCS, not local disk
- **GitOps** — infrastructure changes reviewed in CI before applying
- **Monitoring as code** — alerts and dashboards defined in Terraform
```

---

## Resume Bullets

Add these to your resume under Projects:
```
Week 8 — DataOps & CI/CD Pipeline | GCP, Docker, Terraform, GitHub Actions
- Built a 3-job GitHub Actions CI/CD pipeline that automatically lints, tests
  (86% coverage), builds a multi-stage Docker image (117MB), and pushes to GCP
  Artifact Registry on every commit using keyless Workload Identity Federation
- Provisioned GCP infrastructure (GCS bucket, BigQuery dataset + partitioned
  table, Cloud Monitoring alerts) using Terraform with reusable modules,
  remote state in GCS, and environment-based configuration
- Implemented Cloud Monitoring with log-based error metrics, email alert
  policies, and a 4-panel pipeline health dashboard — all defined as code