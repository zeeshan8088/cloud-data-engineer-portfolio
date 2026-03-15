# 🔍 Week 7 — AI-Powered Data Quality Monitor

An end-to-end, production-grade data quality pipeline that automatically
detects anomalies in e-commerce orders and uses **Gemini 2.5** (Google's
latest LLM) to generate plain-English root cause explanations — all
running on a fully automated schedule in GCP.

---

## 🏗️ Architecture
```
E-commerce Orders (CSV / GCS)
         │
         ▼
┌─────────────────────┐
│  detect_anomalies   │  Rule-based checks: negative amounts,
│  .py                │  future timestamps, duplicates, missing IDs
└────────┬────────────┘
         │  14 anomalies detected
         ▼
┌─────────────────────┐
│  llm_summarizer.py  │  Calls Gemini 2.5 Flash API for each anomaly
│  (Gemini 2.5)       │  → plain-English root cause + action items
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  bq_writer.py       │  Writes all results to BigQuery
│  (BigQuery)         │  with run_id for pipeline lineage tracking
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  dashboard.py       │  Live Streamlit dashboard — dark theme,
│  (Streamlit)        │  Plotly charts, Gemini explanations per order
└─────────────────────┘

Automated via:
Cloud Scheduler → Cloud Run Job (Docker) → runs daily at 6 AM IST
```

---

## 🚀 Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| AI / LLM | Gemini 2.5 Flash (Google AI Studio API) |
| Data Warehouse | BigQuery (`ecommerce_quality_monitor.anomaly_reports`) |
| Orchestration | Cloud Run Jobs + Cloud Scheduler |
| Containerisation | Docker → Artifact Registry |
| Data Storage | Google Cloud Storage (GCS) |
| Dashboard | Streamlit + Plotly |
| Credentials | Application Default Credentials + env vars |

---

## 📁 Project Structure
```
week7-ai-quality-monitor/
├── src/
│   ├── generate_data.py       # Generates 200 e-commerce orders (15 anomalous)
│   ├── detect_anomalies.py    # Rule-based anomaly detection (5 anomaly types)
│   ├── llm_summarizer.py      # Gemini 2.5 API integration
│   └── bq_writer.py           # BigQuery writer with schema + idempotent setup
├── data/
│   └── sample_orders.csv      # Generated dataset (200 orders)
├── dashboard.py               # Streamlit live dashboard
├── pipeline.py                # Main pipeline coordinator
├── Dockerfile                 # Container definition for Cloud Run
├── requirements.txt
└── README.md
```

---

## 🔎 Anomaly Types Detected

| Type | Description | Real-world Cause |
|------|-------------|-----------------|
| `NEGATIVE_AMOUNT` | order_amount < 0 | Misclassified refund/return |
| `ZERO_AMOUNT_HIGH_QTY` | amount=0, qty>100 | Test/ghost order |
| `FUTURE_TIMESTAMP` | order_date > today | Date parsing bug |
| `MISSING_CUSTOMER_ID` | customer_id is null | Guest checkout / ETL failure |
| `DUPLICATE_ORDER_ID` | same ID appears 2+ times | Pipeline retry / double ingestion |

---

## 🤖 Sample Gemini Output

**Anomaly detected:**
> Order ID ORD-0042 appears 3 times in the dataset.

**Gemini 2.5 explains:**
> *"This strongly indicates a lack of idempotency in the data pipeline,
> where a retry mechanism or faulty ingestion process led to the same
> order record being inserted multiple times. A data engineer should
> implement deduplication logic at the ingestion layer and clean up
> existing duplicate records."*

---

## 📊 BigQuery Schema
```sql
Table: intricate-ward-459513-e1.ecommerce_quality_monitor.anomaly_reports

run_id             STRING     -- Unique pipeline run ID (e.g. run_20260315_040651_5789d1)
order_id           STRING     -- Affected order
anomaly_type       STRING     -- Category of anomaly
description        STRING     -- Plain-English issue description
gemini_explanation STRING     -- Gemini root cause analysis
detected_at        TIMESTAMP  -- When the pipeline ran
```

**Useful queries:**
```sql
-- Anomaly breakdown by type
SELECT anomaly_type, COUNT(*) as total
FROM `ecommerce_quality_monitor.anomaly_reports`
GROUP BY anomaly_type
ORDER BY total DESC;

-- Compare pipeline runs over time
SELECT run_id, COUNT(*) as total, MIN(detected_at) as run_time
FROM `ecommerce_quality_monitor.anomaly_reports`
GROUP BY run_id
ORDER BY run_time DESC;
```

---

## ⚙️ How to Run Locally
```bash
# 1. Clone and set up environment
git clone https://github.com/YOUR_USERNAME/cloud-data-engineer-portfolio
cd projects/week7-ai-quality-monitor
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2. Set environment variables
cp .env.example .env
# Add your GEMINI_API_KEY to .env

# 3. Generate sample data
python src/generate_data.py

# 4. Run full pipeline
python pipeline.py

# 5. Launch dashboard
streamlit run dashboard.py
```

---

## ☁️ Cloud Deployment
```bash
# Build and push Docker image
docker build -t ai-quality-monitor .
docker tag ai-quality-monitor \
  us-central1-docker.pkg.dev/PROJECT_ID/ai-quality-monitor/pipeline:latest
docker push \
  us-central1-docker.pkg.dev/PROJECT_ID/ai-quality-monitor/pipeline:latest

# Deploy Cloud Run Job
gcloud run jobs create ai-quality-monitor-job \
  --image=us-central1-docker.pkg.dev/PROJECT_ID/ai-quality-monitor/pipeline:latest \
  --region=us-central1

# Schedule daily at 6 AM IST
gcloud scheduler jobs create http ai-quality-monitor-schedule \
  --schedule="30 0 * * *" \
  --location=us-central1
```

---

## 📈 Key Results

- ✅ **14 anomalies detected** across 5 categories from 200 orders
- ✅ **Gemini 2.5** generates actionable root cause analysis per anomaly
- ✅ **BigQuery** stores results with full pipeline lineage (`run_id`)
- ✅ **Cloud Run Job** runs the pipeline serverlessly in ~2 minutes
- ✅ **Cloud Scheduler** triggers automatically every day at 6 AM IST
- ✅ **Streamlit dashboard** visualises trends across multiple pipeline runs

---

## 🧠 Concepts Demonstrated

- AI/LLM integration into data pipelines
- Rule-based data quality framework
- Idempotent BigQuery table setup
- Pipeline lineage tracking with `run_id`
- Fault-tolerant pipeline design (`try/except` per anomaly)
- Rate limiting for API calls
- Serverless container deployment on GCP
- Environment variable management (never hardcode secrets)