# RetailFlow — Intelligent Retail Data Platform

> A production-grade, end-to-end cloud data platform built on GCP — featuring
> Bronze/Silver/Gold warehouse architecture, dbt Core transformations,
> Great Expectations data quality gates, Vertex AI demand forecasting,
> and Airflow orchestration.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  FakeStore API · Faker CSV · Clickstream Simulator · Pub/Sub   │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER — GCS + BigQuery (append-only, audit columns)     │
│  raw_orders · raw_customers · raw_products · raw_clickstream    │
│  Partitioned by _ingested_date · clustered by key dimensions    │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  GREAT EXPECTATIONS — Quality Gates                             │
│  Schema · nulls · ranges · referential checks · fail-fast       │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  SILVER LAYER — dbt Core Staging Models                         │
│  stg_orders · stg_customers · stg_products · stg_clickstream   │
│  SCD Type 2 via MERGE · incremental models · dbt tests          │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  GOLD LAYER — dbt Core Marts                                    │
│  mart_sales_daily · mart_customer_ltv · mart_product_performance│
│  mart_funnel · Partitioned · Clustered · Materialized views     │
└───────────┬──────────────────────────────────┬──────────────────┘
            ▼                                  ▼
┌──────────────────────┐          ┌──────────────────────────┐
│  Vertex AI AutoML    │          │  Looker Studio Dashboard │
│  Demand forecasting  │          │  Revenue · LTV · Funnel  │
│  Batch predictions   │          │  Forecast comparison     │
└──────────────────────┘          └──────────────────────────┘
```

## Tech Stack

| Category | Tool | Purpose |
|---|---|---|
| Data Sources | FakeStore API, Faker, Event Simulator | Multi-source ingestion |
| Ingestion | Cloud Functions Gen2, Cloud Scheduler | Production-grade triggers |
| Raw Storage | GCS + BigQuery Bronze | Audit trail, replayability |
| Data Quality | Great Expectations + dbt tests | Quality gates |
| Transformation | dbt Core | Industry-standard transforms |
| Warehouse | BigQuery (Bronze/Silver/Gold) | SCD2, incremental, partitioned |
| AI/ML | Vertex AI Tabular AutoML | Demand forecasting |
| Dashboard | Looker Studio | Executive KPI dashboard |
| Orchestration | Apache Airflow | Full pipeline coordination |
| IaC | Terraform | All infra as code |
| CI/CD | GitHub Actions | dbt + GE on every PR |

## GCP Project

- **Project:** `intricate-ward-459513-e1`
- **Region:** `asia-south1`

## BigQuery Datasets

| Dataset | Layer | Purpose |
|---|---|---|
| `retailflow_bronze` | Raw | Append-only ingested data |
| `retailflow_silver` | Staging | dbt staging models |
| `retailflow_gold` | Marts | Business-ready aggregates |
| `retailflow_predictions` | AI | Vertex AI outputs |
| `retailflow_metadata` | Ops | Pipeline logs, GE results |

## Quick Start

### 1. Prerequisites

```bash
# Authenticate with GCP
gcloud auth application-default login
gcloud config set project intricate-ward-459513-e1
```

### 2. Create BigQuery Datasets

```bash
bq mk --location=asia-south1 retailflow_bronze
bq mk --location=asia-south1 retailflow_silver
bq mk --location=asia-south1 retailflow_gold
bq mk --location=asia-south1 retailflow_predictions
bq mk --location=asia-south1 retailflow_metadata
```

### 3. Create GCS Bucket

```bash
gsutil mb -l asia-south1 gs://retailflow-bronze-intricate-ward-459513-e1/
```

### 4. Install Dependencies

```bash
cd ingestion
pip install -r requirements.txt
```

### 5. Run Ingestion (Dry-Run)

```bash
set DRY_RUN=true
python fetch_orders.py
python generate_customers.py
python generate_products.py
python simulate_clickstream.py
```

### 6. Run Ingestion (GCP)

```bash
set DRY_RUN=false
python fetch_orders.py
python generate_customers.py
python generate_products.py
python simulate_clickstream.py
```

### 7. Verify Data

```bash
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM retailflow_bronze.raw_orders"
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM retailflow_bronze.raw_customers"
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM retailflow_bronze.raw_products"
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM retailflow_bronze.raw_clickstream"
```

## Project Structure

```
week10-11-capstone/
├── README.md
├── architecture/
├── ingestion/           ← Day 1: Data ingestion scripts
├── great_expectations/  ← Day 2: Data quality suites
├── dbt/                 ← Days 3-5: Transformations
├── vertex_ai/           ← Days 8-9: ML forecasting
├── airflow/             ← Day 6: Orchestration DAG
├── terraform/           ← Day 7: Infrastructure as Code
├── .github/workflows/   ← Day 7: CI/CD pipelines
└── looker_studio/       ← Day 10: Executive dashboard
```

## Author

**Zeeshan Yalakpalli** — CSE Final Year, Bengaluru
GitHub: [zeeshan8088](https://github.com/zeeshan8088)
