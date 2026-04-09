# Day 6 — Airflow Master DAG

## What's in this folder

```
airflow/
├── retailflow_dag.py      ← THE master DAG — all pipeline layers wired together
├── test_dag.py            ← Smoke test: validate DAG structure locally (no Airflow needed)
├── docker-compose.yml     ← Run Airflow locally with Docker (webserver + scheduler + worker)
├── Dockerfile             ← Custom Airflow 2.8 image with dbt + GE pre-installed
├── requirements.txt       ← Python packages installed in the Docker image
├── .env.example           ← Copy to .env and fill in your GCP values
└── README.md              ← This file
```

---

## Pipeline Architecture

```
start
  │
  ├─[parallel]──────────────────────────────────────────────────────┐
  │  ingest_orders       (FakeStore API → BQ raw_orders)            │
  │  ingest_customers    (Faker → BQ raw_customers)                 │
  │  ingest_products     (Faker → BQ raw_products)                  │
  │  ingest_clickstream  (Pub/Sub sim → BQ raw_clickstream)         │
  └──────────────────────────────────────────────────────────────────┘
  │
  ▼
ge_bronze_quality_gate          ← ShortCircuit: FAIL = skip everything downstream
  │
  ▼
dbt_staging/
  dbt_deps → dbt_run_staging → dbt_test_staging
  │
  ▼
dbt_scd/
  dbt_snapshot → dbt_run_scd2 → dbt_test_scd
  │
  ▼
dbt_marts/
  dbt_run_marts → dbt_test_marts → dbt_docs_generate
  │
  ▼
log_pipeline_success            ← Writes to retailflow_metadata.pipeline_runs
  │
  ▼
trigger_vertex_ai               ← Placeholder (Day 9: real AutoML batch predict)
  │
  ▼
end
```

**Key production features:**
| Feature | Implementation |
|---|---|
| Schedule | `0 2 * * *` — 2am UTC = 7:30am IST |
| Retries | 2 retries, 5-min delay on every task |
| SLA | 2-hour window — logs warning on miss |
| Fail-fast quality gate | `ShortCircuitOperator` wraps GE validator |
| Failure audit trail | `on_failure_callback` → BigQuery metadata table |
| XCom | `batch_id` flows ingestion → downstream |
| Max active runs | 1 — prevents overlapping pipeline runs |

---

## Option A: Run the DAG Locally (Docker — Recommended)

### Prerequisites
- Docker Desktop installed and running
- GCP credentials (`gcloud auth application-default login` already done)
- `docker-compose` v2.x

### Steps

```powershell
# 1. Go to the airflow folder
cd projects\week10-11-capstone\airflow

# 2. Copy environment template
Copy-Item .env.example .env
# Edit .env if needed (GOOGLE_APPLICATION_CREDENTIALS path)

# 3. First-time DB init (run once only)
docker-compose up airflow-init

# 4. Start all Airflow services in background
docker-compose up -d

# 5. Open the Airflow UI
start http://localhost:8080
# Login: admin / retailflow123
```

### Trigger the DAG manually

```powershell
# Via UI: click "retailflow_master" → toggle ON → click ▶ Trigger DAG

# Or via CLI inside the container:
docker-compose exec airflow-scheduler airflow dags trigger retailflow_master
```

### Watch logs live

```powershell
docker-compose exec airflow-scheduler airflow tasks logs retailflow_master ge_bronze_quality_gate
```

### Stop Airflow

```powershell
docker-compose down          # stop containers (keeps data)
docker-compose down -v       # stop + delete volumes (clean slate)
```

---

## Option B: Validate DAG Structure Without Docker

If you just want to verify the DAG file is correct before setting up Docker:

```powershell
# From the project root
cd projects\week10-11-capstone

# Install just the Airflow core (no providers needed for smoke test)
pip install apache-airflow==2.8.4 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.10.txt"

# Run the smoke test
python airflow\test_dag.py
```

Expected output:
```
===================================================================
RetailFlow DAG Smoke Test
===================================================================

[1/5] Importing DAG module...
  ✅ PASS | DAG module imports cleanly
...
[5/5] Checking default_args...
  ✅ PASS | retries == 2
  ✅ PASS | retry_delay == 5 minutes
  ...
===================================================================
✅ ALL CHECKS PASSED (43 assertions)
   Your DAG is structurally correct and ready to run in Airflow.
===================================================================
```

---

## Option C: Deploy to Cloud Composer (Production)

```powershell
# Upload the DAG to your Composer environment's GCS DAGs bucket
# Replace COMPOSER_BUCKET with your actual bucket name
gsutil cp airflow\retailflow_dag.py gs://COMPOSER_BUCKET/dags/

# Set environment variables in Composer:
gcloud composer environments update retailflow-composer `
  --location asia-south1 `
  --update-env-variables GCP_PROJECT=intricate-ward-459513-e1,BQ_DATASET_META=retailflow_metadata,RETAILFLOW_HOME=/home/airflow/gcs/data

# Upload the project code to the data folder
gsutil -m cp -r ingestion great_expectations dbt `
  gs://COMPOSER_BUCKET/data/
```

---

## Failure Testing

Deliberately break the pipeline to verify the quality gate works:

```powershell
# 1. In BigQuery console, run:
#    INSERT INTO retailflow_bronze.raw_orders VALUES(-1, ...)
#    (or use the poisoned data test from Day 2)

# 2. Trigger the DAG
docker-compose exec airflow-scheduler airflow dags trigger retailflow_master

# 3. Expected behaviour:
#    ✅ ingest_orders, ingest_customers, ingest_products, ingest_clickstream → SUCCESS
#    ❌ ge_bronze_quality_gate → FAILED (returns False)
#    ⏭️  dbt_staging, dbt_scd, dbt_marts, log_lineage, trigger_vertex → SKIPPED
#    GE DataDocs report shows exactly which rows failed
```

---

## Interview Talking Points

> **"Walk me through your Airflow orchestration design."**

"I designed a 7-layer DAG that mirrors the medallion architecture. The first layer runs all 4 ingestion sources in parallel — orders, customers, products, and clickstream — which gives us maximum throughput. After ingestion, a ShortCircuitOperator wraps the Great Expectations validator: if any Bronze table fails its data quality checks, the entire downstream pipeline is skipped automatically. This is the 'fail-fast' pattern — it's far better to halt a pipeline than to let bad data silently propagate into Gold marts that executives are looking at.

The dbt layers run sequentially: staging first, then SCD Type 2 snapshot, then Gold mart models with full test suites at each layer. A final Python task writes a success record to a BigQuery metadata table, giving us a full audit trail of every pipeline run. I also implemented retry logic, SLA monitoring at the 2-hour mark, and a failure callback that writes to the same metadata table — so we have one source of truth for pipeline health regardless of success or failure."
