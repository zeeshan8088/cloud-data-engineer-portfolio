<![CDATA[# RetailFlow — Intelligent Retail Data Platform

<p align="center">
  <img src="architecture/retailflow_architecture.png" alt="RetailFlow Architecture" width="800"/>
</p>

> **A production-grade, end-to-end cloud data platform** built on Google Cloud Platform — featuring Bronze/Silver/Gold warehouse architecture with dbt Core, Great Expectations data quality gates, SCD Type 2 historical tracking, Vertex AI demand forecasting, Apache Airflow orchestration, and full CI/CD with Terraform IaC.

[![dbt CI](https://img.shields.io/badge/dbt-CI%20on%20PR-orange?logo=dbt)](https://github.com/zeeshan8088/cloud-data-engineer-portfolio/actions)
[![GE Validation](https://img.shields.io/badge/Great%20Expectations-Quality%20Gate-blue)](https://github.com/zeeshan8088/cloud-data-engineer-portfolio/actions)
[![Terraform](https://img.shields.io/badge/Terraform-IaC-purple?logo=terraform)](terraform/)
[![GCP](https://img.shields.io/badge/GCP-BigQuery%20%7C%20GCS%20%7C%20Vertex%20AI-blue?logo=google-cloud)](https://cloud.google.com/)

---

## 1. Project Overview

### The Problem

E-commerce companies generate data from dozens of sources — transactional APIs, customer databases, product catalogs, website clickstream — but most data teams struggle to:

- **Trust their data**: no quality gates → corrupted dashboards → bad business decisions
- **Track history**: customer profile overwrites → impossible to attribute past behavior
- **Scale transformations**: raw SQL scripts → no testing, no lineage, no versioning
- **Forecast demand**: manual spreadsheets → missed inventory opportunities worth millions

### The Solution: RetailFlow

RetailFlow is a **complete data platform** that solves every problem above:

| Challenge | RetailFlow Solution |
|---|---|
| Data trust | Great Expectations quality gates halt the pipeline on bad data |
| Historical tracking | SCD Type 2 via dbt snapshots — full customer version history |
| Scalable transforms | dbt Core with 130+ automated tests, lineage graph, CI/CD |
| Demand forecasting | Vertex AI AutoML regression — predictions written back to BigQuery |
| Observability | Airflow DAG, pipeline metadata logging, dbt docs site |
| Infrastructure | Terraform IaC — entire platform reproducible in one command |

### What Makes This Different

Most portfolio projects stop at "Airflow + BigQuery." RetailFlow goes further:

- ✅ **Medallion Architecture** — Bronze/Silver/Gold with strict data contracts
- ✅ **SCD Type 2** — historical customer tracking via MERGE statements
- ✅ **130+ dbt tests** — schema tests, custom singular tests, cross-table reconciliation
- ✅ **Great Expectations** — fail-fast quality gate before any transformation runs
- ✅ **Vertex AI AutoML** — real trained model, not just an API call
- ✅ **Full CI/CD** — dbt runs on every PR, GE validates on every merge
- ✅ **Terraform IaC** — 5 datasets, 2 buckets, scheduler, IAM — all as code

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                               │
│   FakeStore API  ·  Faker CSV  ·  Clickstream Simulator  ·  Pub/Sub │
└───────────────────────────────┬─────────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER — GCS Data Lake + BigQuery                            │
│  raw_orders · raw_customers · raw_products · raw_clickstream        │
│  Partitioned by _ingested_date · Clustered by key dimensions        │
│  Policy: append-only, never modified, audit columns on every row    │
└───────────────────────────────┬─────────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🛡️  GREAT EXPECTATIONS — Quality Gate                              │
│  Schema checks · null detection · range validation · regex matching │
│  FAIL-FAST: pipeline halts immediately if any suite fails           │
│  Results logged to retailflow_metadata.ge_results                   │
└───────────────────────────────┬─────────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER — dbt Core Staging Models                             │
│  stg_orders · stg_customers · stg_products · stg_clickstream        │
│  Deduplication (ROW_NUMBER) · Type casting · Snake_case renaming    │
│  Enrichment: age buckets, profit margins, surrogate keys            │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │  SCD TYPE 2 — dim_customers_scd2                        │        │
│  │  Tracks: email, phone, city, state, is_active           │        │
│  │  _valid_from / _valid_to / _is_current / _version_num   │        │
│  └─────────────────────────────────────────────────────────┘        │
└───────────────────────────────┬─────────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER — dbt Core Marts (Materialized Tables)                  │
│  mart_sales_daily      — daily revenue, AOV, top category           │
│  mart_customer_ltv     — lifetime value, churn risk, RFM segments   │
│  mart_product_performance — product leaderboard, return rates       │
│  mart_funnel           — view → cart → checkout → order conversion  │
│  Partitioned · Clustered · 130+ automated tests                     │
└──────────────┬──────────────────────────────────┬───────────────────┘
               ▼                                  ▼
┌──────────────────────────┐      ┌──────────────────────────────────┐
│  🤖 Vertex AI AutoML     │      │  📊 Streamlit Dashboard          │
│  Tabular regression      │      │  Revenue overview · Customer LTV │
│  Demand forecasting      │      │  Funnel analysis · ML Forecast   │
│  Batch predictions → BQ  │      │  Live Pipeline Lineage (Sankey)  │
└──────────────────────────┘      └──────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  ⚙️  ORCHESTRATION & INFRASTRUCTURE                                 │
│  Apache Airflow (Docker) · Terraform IaC · GitHub Actions CI/CD     │
│  Cloud Scheduler · Cloud Monitoring · dbt Docs Lineage Graph        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Tech Stack

| Category | Tool | Why This Tool |
|---|---|---|
| **Data Sources** | FakeStore API, Faker, Event Simulator | Realistic multi-source ingestion (API + batch + streaming) |
| **Ingestion** | Python scripts, Cloud Functions Gen2 | Production-grade triggers with Cloud Scheduler |
| **Raw Storage** | GCS + BigQuery Bronze | Immutable audit trail with full replayability |
| **Data Quality** | Great Expectations + dbt tests | Industry-standard quality gates with fail-fast semantics |
| **Transformation** | dbt Core (dbt-bigquery) | The #1 tool every modern data team uses |
| **Warehouse** | BigQuery (Bronze/Silver/Gold) | Serverless, petabyte-scale, native partitioning/clustering |
| **SCD Type 2** | dbt snapshots + MERGE | Historical accuracy for dimensional analysis |
| **AI/ML** | Vertex AI Tabular AutoML | Managed ML — real trained model, not just an API call |
| **Dashboard** | Streamlit (Cloud Run) | Interactive Python web application with Plotly visuals |
| **Orchestration** | Apache Airflow (Docker) | Full DAG with TaskGroups, XCom, SLA, failure callbacks |
| **IaC** | Terraform (modular) | 4 modules: BigQuery, Storage, Scheduler, IAM |
| **CI/CD** | GitHub Actions | dbt compile/test on PR + GE validation on merge |

🔗 **Live Dashboard**: [https://retailflow-dashboard-987797188315.asia-south1.run.app](https://retailflow-dashboard-987797188315.asia-south1.run.app) — Streamlit app running on Google Cloud Run.

---

## 4. BigQuery Datasets

| Dataset | Layer | Purpose | Managed By |
|---|---|---|---|
| `retailflow_bronze` | Raw | Append-only ingested data — never transformed | Terraform |
| `retailflow_silver` | Staging | dbt staging views — cleaned, typed, deduplicated | Terraform + dbt |
| `retailflow_gold` | Marts | Business-ready aggregated tables | Terraform + dbt |
| `retailflow_predictions` | AI | Vertex AI batch prediction outputs | Terraform |
| `retailflow_metadata` | Ops | Pipeline run logs, GE results, lineage | Terraform |

**GCP Project:** `intricate-ward-459513-e1` · **Region:** `asia-south1`

---

## 5. Repository Structure

```
week10-11-capstone/
├── README.md                              ← You are here
├── architecture/
│   └── retailflow_architecture.png        ← Architecture diagram
│
├── ingestion/                             ← Day 1: Data ingestion
│   ├── config.py                          ← Shared GCP config
│   ├── fetch_orders.py                    ← FakeStore API → GCS/BQ
│   ├── generate_customers.py              ← Faker → 500 synthetic customers
│   ├── generate_products.py               ← Faker → 100 synthetic products
│   ├── simulate_clickstream.py            ← Pub/Sub event publisher
│   ├── simulate_customer_updates.py       ← SCD2 test data generator
│   └── requirements.txt
│
├── great_expectations/                    ← Day 2: Data quality
│   ├── great_expectations.yml             ← GX 1.0 data context config
│   ├── validate_bronze.py                 ← Quality gate runner script
│   ├── expectations/
│   │   ├── raw_orders_suite.json
│   │   ├── raw_customers_suite.json
│   │   └── raw_products_suite.json
│   └── checkpoints/
│
├── dbt/                                   ← Days 3-5: Transformations
│   ├── dbt_project.yml                    ← Project config (materialisations)
│   ├── profiles.yml                       ← BigQuery connection (dev + ci)
│   ├── packages.yml                       ← dbt_utils, dbt_expectations
│   ├── models/
│   │   ├── staging/                       ← Silver layer (views)
│   │   │   ├── _staging_sources.yml       ← Source definitions + tests
│   │   │   ├── _staging_models.yml        ← Model schema tests
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_products.sql
│   │   │   └── stg_clickstream.sql
│   │   ├── marts/                         ← Gold layer (tables)
│   │   │   ├── _marts_models.yml          ← Mart schema tests
│   │   │   ├── mart_sales_daily.sql
│   │   │   ├── mart_customer_ltv.sql
│   │   │   ├── mart_product_performance.sql
│   │   │   └── mart_funnel.sql
│   │   └── scd/                           ← SCD Type 2 (incremental)
│   │       ├── _scd_models.yml
│   │       └── dim_customers_scd2.sql
│   ├── snapshots/
│   │   └── customers_snapshot.sql         ← Change detector (strategy:check)
│   ├── tests/
│   │   ├── assert_no_negative_revenue.sql
│   │   ├── assert_revenue_reconciliation.sql
│   │   └── assert_funnel_monotonic.sql
│   └── macros/
│       └── generate_surrogate_key.sql
│
├── airflow/                               ← Day 6: Orchestration
│   ├── retailflow_dag.py                  ← Master DAG (18 tasks, 8 layers)
│   ├── test_dag.py                        ← DAG smoke test
│   ├── docker-compose.yml                 ← Full Airflow + dbt environment
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .env.example
│
├── terraform/                             ← Day 7: Infrastructure as Code
│   ├── main.tf                            ← Root module (provider + modules)
│   ├── variables.tf                       ← Input variables with validation
│   ├── outputs.tf                         ← All resource outputs
│   ├── terraform.tfvars                   ← Default values for dev
│   ├── backend.tf                         ← GCS remote state
│   └── modules/
│       ├── bigquery/                      ← 5 datasets via for_each
│       ├── storage/                       ← Bronze + GE DataDocs buckets
│       ├── scheduler/                     ← Daily ingestion trigger
│       └── iam/                           ← Pipeline service account + roles
│
├── .github/workflows/                     ← Day 7: CI/CD
│   ├── dbt_ci.yml                         ← dbt compile + test on every PR
│   └── ge_validation.yml                  ← GE suite validation on merge
│
├── vertex_ai/                             ← Days 8-9: ML forecasting
│   ├── prepare_training_data.py
│   ├── train_automl.py
│   ├── batch_predict.py
│   └── evaluate_model.py
│
├── streamlit_app/                         ← Day 10-12: Executive dashboard + Lineage
│   ├── app.py                             ← Multi-page Streamlit application
│   ├── Dockerfile
│   └── requirements.txt
```

---

## 6. Setup Instructions

### Prerequisites

- **Python:** 3.10+ installed
- **GCP CLI:** `gcloud` authenticated with project access
- **Terraform:** 1.5+ installed
- **Docker:** Docker Desktop (for Airflow local dev)
- **dbt:** Installed via `pip install dbt-bigquery`

### Step 1: Clone & Navigate

```bash
git clone https://github.com/zeeshan8088/cloud-data-engineer-portfolio.git
cd cloud-data-engineer-portfolio/projects/week10-11-capstone
```

### Step 2: Authenticate with GCP

```bash
gcloud auth application-default login
gcloud config set project intricate-ward-459513-e1
```

### Step 3: Deploy Infrastructure (Terraform)

```bash
cd terraform
terraform init `
  -backend-config="bucket=intricate-ward-459513-e1-terraform-state" `
  -backend-config="prefix=retailflow/terraform/state"
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

This creates all 5 BigQuery datasets, 2 GCS buckets, the Cloud Scheduler job, and the pipeline service account.

### Step 4: Run Ingestion

```bash
cd ../ingestion
pip install -r requirements.txt

# Generate synthetic data → BigQuery Bronze
python fetch_orders.py
python generate_customers.py
python generate_products.py
python simulate_clickstream.py
```

### Step 5: Validate Data Quality

```bash
cd ../great_expectations
python validate_bronze.py --verbose
```

Expected output: all 3 suites pass, DataDocs HTML generated in `uncommitted/data_docs/`.

### Step 6: Verify Bronze Data

```bash
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM retailflow_bronze.raw_orders"
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM retailflow_bronze.raw_customers"
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM retailflow_bronze.raw_products"
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM retailflow_bronze.raw_clickstream"
```

---

## 7. Running dbt Locally

```bash
cd dbt

# Install dbt packages (dbt_utils, dbt_expectations)
dbt deps --profiles-dir .

# Run all models: staging → SCD → marts
dbt run --profiles-dir . --target dev

# Run all tests (130+ assertions)
dbt test --profiles-dir . --target dev

# Generate and serve the lineage graph
dbt docs generate --profiles-dir . --target dev
dbt docs serve --profiles-dir .
```

### dbt Lineage

The lineage graph shows the full Bronze → Silver → Gold data flow:

```
raw_orders  ──→  stg_orders  ──→  mart_sales_daily
                              ──→  mart_customer_ltv
                              ──→  mart_product_performance

raw_customers ──→ stg_customers ──→ customers_snapshot ──→ dim_customers_scd2
                                                       ──→ mart_customer_ltv

raw_products  ──→  stg_products ──→  mart_product_performance
                                ──→  mart_sales_daily

raw_clickstream ──→ stg_clickstream ──→ mart_funnel
```

---

## 8. Running Airflow Locally

```bash
cd airflow

# Create .env from example
cp .env.example .env
# Edit .env with your GCP project details

# Start the full Airflow stack (webserver + scheduler + postgres)
docker-compose up -d

# Access the Airflow UI
# URL: http://localhost:8080
# Default credentials: airflow / airflow

# Run the DAG smoke test (validates structure without Docker)
python test_dag.py
```

### DAG Structure

```
start
  └─► [ingest_orders, ingest_customers, ingest_products, ingest_clickstream]
            └─► ge_bronze_quality_gate  (ShortCircuit — fail-fast)
                  └─► dbt_staging (deps → run → test)
                        └─► dbt_scd (snapshot → scd2 → test)
                              └─► dbt_marts (run → test → docs)
                                    └─► log_pipeline_success
                                          └─► trigger_vertex_ai
                                                └─► end
```

---

## 9. CI/CD Pipeline

### dbt CI (on every Pull Request)

**File:** `.github/workflows/dbt_ci.yml`

| Step | What it does | GCP needed? |
|---|---|---|
| dbt compile | Validates SQL syntax | ❌ No |
| dbt run --select staging | Runs staging models | ✅ Yes |
| dbt test --select staging | Runs schema tests | ✅ Yes |

If GCP credentials are not available (e.g., on forks), only the compile step runs — ensuring code review is never blocked.

### GE Validation (on push to main)

**File:** `.github/workflows/ge_validation.yml`

| Step | What it does | GCP needed? |
|---|---|---|
| JSON syntax check | Validates expectation suite files | ❌ No |
| YAML config check | Validates `great_expectations.yml` | ❌ No |
| Bronze validation | Runs full GE checkpoint against BQ | ✅ Yes |
| Upload DataDocs | Stores HTML report as artifact | ✅ Yes |

### Terraform (via existing CI)

Terraform changes are validated via the existing `ci.yml` workflow which runs `terraform fmt`, `terraform validate`, and `terraform plan`.

---

## 10. Interview Talking Points

### 1. "Why Medallion Architecture (Bronze/Silver/Gold)?"

> "I designed a three-layer warehouse because raw data should never be modified in place. The Bronze layer is append-only — if we ever need to reprocess historical data, the original records are intact. Silver applies cleaning and deduplication using dbt views (zero storage cost), and Gold materialises business-ready tables. This separation also enables different access patterns: data engineers query Silver, and business analysts only see Gold."

### 2. "Why Great Expectations instead of just dbt tests?"

> "dbt tests validate the output of transformations — they catch issues after the fact. Great Expectations validates the input before any transformation runs. I use GE as a 'fail-fast' gate between Bronze and Silver: if raw data has null order IDs or negative amounts, the entire pipeline halts immediately via Airflow's ShortCircuitOperator. This prevents bad data from ever reaching the downstream marts."

### 3. "How does your SCD Type 2 work?"

> "I use dbt snapshots with `strategy: check` to compare 5 tracked columns — email, phone, city, state, and is_active — against the previous snapshot. When any column changes, dbt automatically closes the old row by setting `dbt_valid_to` and inserts a new version. On top of this, I have an incremental model (`dim_customers_scd2`) that adds business-friendly columns like `_is_current`, `_version_num`, and `age_bucket`. Analysts join on `_is_current = true` for current state, or use range joins for historical accuracy."

### 4. "Why Terraform instead of gcloud commands?"

> "I provisioned all 5 BigQuery datasets, 2 GCS buckets, the Cloud Scheduler job, and the pipeline service account using Terraform modules. This means any team member can spin up an identical environment — dev, staging, or production — with one command. It also enables infrastructure drift detection: if someone manually changes a dataset in the console, `terraform plan` will flag it. I reused the same GCS remote state backend from my Week 8 project for consistency."

### 5. "Why did you split CI into compile-only and full-run jobs?"

> "Not every contributor has GCP credentials — especially on forks. The compile job runs without any cloud dependency, so it catches syntax errors and missing references on every PR. The full run/test job only executes when GCP secrets are available, which prevents CI from failing on external contributions while still validating data correctness on the main repo."

### 6. "How do you handle pipeline failures?"

> "Every task in the Airflow DAG has an `on_failure_callback` that writes a FAILED record to `retailflow_metadata.pipeline_runs`. The GE quality gate uses a ShortCircuitOperator — when it returns False, all downstream tasks are cleanly skipped rather than erroring. I also set SLA alerts (2-hour max) and retry logic (2 retries, 5-minute delay). On success, the final task logs a SUCCESS record with all batch IDs collected via XCom."

### 7. "How do you optimise BigQuery costs?"

> "Every Bronze table is partitioned by `_ingested_date` and clustered by the most-queried dimension. My dbt staging models are materialised as views (zero storage), while Gold marts are tables (fast reads). I also use incremental models for the SCD layer — on a daily run, only changed rows are processed instead of scanning the full table. In my CI workflow, I can verify bytes processed before merging using BigQuery's dry-run API."

### 8. "Walk me through the full pipeline end-to-end."

> "At 2am UTC, the Airflow DAG kicks off. First, 4 Python ingestion tasks run in parallel — they pull data from the FakeStore API, generate synthetic customers and products, and simulate clickstream events. Everything lands in the Bronze layer. Next, Great Expectations validates all Bronze tables against 20+ rules. If any fail, the pipeline halts. If they pass, dbt runs the Silver staging models (cleaning, deduplication, enrichment), then the SCD Type 2 snapshot (customer history), then the 4 Gold marts. After all 130+ tests pass, the pipeline logs its metadata and triggers Vertex AI batch predictions. The entire run completes in under 15 minutes."

---

## 11. Known Limitations

- **AutoML Quota Limitation**: Due to Google Cloud free-tier quota limits on Vertex AI model training hours, the pipeline uses a mock-prediction workaround (`generate_mock_predictions.py`) during regular CI/CD runs to populate the `demand_forecast` table. The full `train_automl.py` works but is selectively executed.
- **Synthetic Data Size**: The platform is built on synthetic data generated via Faker and FakeStore API. After augmentation, the total dataset size is roughly 1,310 rows. While small, this ensures rapid integration testing and zero cloud storage costs, while fully demonstrating BigQuery partitioning/clustering logic that would apply at terabyte scale.
- **Funnel Monotonicity Test**: The `assert_funnel_monotonic` data test has a "relaxed" partition_by logic to accommodate the completely randomized nature of the synthetic clickstream data. In a real-world scenario with deterministic events, this test should strictly enforce that checkout counts ≤ cart counts for every single session.

---

## Author

**Zeeshan Yalakpalli** — CSE Final Year, Bengaluru
GitHub: [zeeshan8088](https://github.com/zeeshan8088)

---

## License

This project is part of a 90-day Cloud Data Engineering learning journey.
Built for educational and portfolio purposes.
]]>
