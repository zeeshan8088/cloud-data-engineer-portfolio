# LinkedIn Post — RetailFlow Launch

---

🚀 I just built a production-grade retail data platform from scratch — as a fresher.

Over the past 14 days, I designed and deployed RetailFlow: an end-to-end cloud data platform on Google Cloud that ingests raw e-commerce data, enforces automated quality gates, transforms it through a Bronze → Silver → Gold warehouse, and forecasts demand using AI.

Here's what powers it:
• dbt Core — 10 models, 129 automated tests, SCD Type 2 historical tracking
• Great Expectations — fail-fast data quality gates before any transformation runs
• Vertex AI AutoML — tabular regression for demand forecasting
• Apache Airflow — 18-task orchestrated DAG with retry logic & SLA monitoring
• Streamlit on Cloud Run — 5-page interactive dashboard with live data lineage
• Terraform IaC — entire infrastructure codified across 4 modules

📊 Live Dashboard: https://retailflow-dashboard-987797188315.asia-south1.run.app

💻 Full source code: https://github.com/zeeshan8088/cloud-data-engineer-portfolio

This isn't a tutorial follow-along. Every architectural decision — from choosing SCD Type 2 for customer history to building cross-table revenue reconciliation tests — was made deliberately and documented.

If you're hiring data engineers who can build, not just talk — let's connect.

#DataEngineering #BigQuery #dbt #GCP #VertexAI #Airflow #CloudRun #Terraform #PortfolioProject #OpenToWork
