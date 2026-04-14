# RetailFlow — Interview Preparation Guide

> **30 Questions & Detailed Model Answers**
> Covering Architecture, dbt & Data Modelling, Data Quality & Testing, BigQuery Cost Optimisation, and Pipeline Orchestration.
> Every answer references specific RetailFlow implementation details.

---

## Section 1: Architecture Decisions (6 Questions)

### Q1. Why did you choose the Medallion Architecture (Bronze / Silver / Gold) for RetailFlow?

**A:** "I chose the Medallion Architecture because it enforces strict separation of concerns across the data lifecycle. In RetailFlow, the Bronze layer (`retailflow_bronze`) stores raw, append-only data — tables like `raw_orders`, `raw_customers`, `raw_products`, and `raw_clickstream` — exactly as they arrive from the FakeStore API and Faker generators. This guarantees an immutable audit trail: if a downstream transformation introduces a bug, I can always replay from the original Bronze files stored in GCS.

The Silver layer (`retailflow_silver`) contains dbt staging views — `stg_orders`, `stg_customers`, `stg_products`, `stg_clickstream` — which apply deduplication via `ROW_NUMBER()`, type casting, snake_case standardisation, and business enrichments like `age_bucket` and `profit_margin`. Because these are materialised as views, they incur zero storage cost.

The Gold layer (`retailflow_gold`) contains materialised tables — `mart_sales_daily`, `mart_customer_ltv`, `mart_product_performance`, `mart_funnel`, and `mart_demand_forecast` — purpose-built for business consumption. This three-tier approach means data engineers operate on Silver, business analysts query Gold, and nobody ever touches Bronze directly."

---

### Q2. Why did you store raw data in both GCS and BigQuery simultaneously?

**A:** "RetailFlow performs a dual-write during ingestion. Every Python ingestion script (e.g., `fetch_orders.py`, `generate_customers.py`) writes raw CSV/JSON files to the GCS Bronze bucket (`retailflow-bronze-...`) and simultaneously loads the structured data into BigQuery Bronze tables.

The GCS files serve as a cheap, immutable backup — a true Data Lake layer. If someone accidentally truncates `raw_orders` in BigQuery, I can reload from the GCS files. BigQuery, on the other hand, provides immediate SQL queryability. The combined approach gives us both durability and accessibility. For cost control, I configured a 30-day lifecycle policy on the GCS bucket via Terraform, so raw files older than 30 days are automatically deleted."

---

### Q3. How did you handle streaming vs. batch data in your architecture?

**A:** "RetailFlow ingests four data sources. Three are batch: `fetch_orders.py` pulls from the FakeStore API, `generate_customers.py` and `generate_products.py` produce synthetic CSV data. These run on a daily schedule.

The fourth source — `simulate_clickstream.py` — is a streaming source that publishes JSON event payloads to Google Cloud Pub/Sub. Clickstream events (page_view, add_to_cart, checkout, purchase) happen at high velocity, so using Pub/Sub decouples the event producer from the storage backend. Downstream consumers can subscribe at their own pace. This is critical in a real Black Friday scenario where the web servers must never block on database writes.

In the current pipeline, Airflow orchestrates both batch ingestion and the clickstream simulator in a single DAG run, with all four ingestion tasks running in parallel to minimise wall-clock time."

---

### Q4. Why did you choose Streamlit over Looker Studio or Tableau for the dashboard?

**A:** "I chose Streamlit for three reasons. First, it keeps the entire platform in Python — the same language as my ingestion scripts, Great Expectations validator, and Vertex AI pipeline — which simplifies CI/CD and dependency management. Second, Streamlit gives me full control over custom visualisations: I built a Plotly Sankey diagram for data lineage tracking (Page 5 of the dashboard), which would be extremely difficult in Looker Studio. Third, Streamlit containerises cleanly with Docker and deploys to Cloud Run for serverless scaling. The live dashboard at `https://retailflow-dashboard-987797188315.asia-south1.run.app` serves five pages — Revenue Overview, Customer Analytics, Funnel Analysis, Demand Forecast, and Pipeline Lineage — directly from the BigQuery Gold layer."

---

### Q5. What drove the decision to use Terraform instead of manual gcloud commands for infrastructure?

**A:** "Manual `gcloud` commands are fine for experimentation, but they are not reproducible, not version-controlled, and not auditable. In RetailFlow, I codified the entire infrastructure using modular Terraform: four modules (`bigquery`, `storage`, `scheduler`, `iam`) managing 25+ resources. This includes all 5 BigQuery datasets (`retailflow_bronze` through `retailflow_metadata`), 2 GCS buckets with lifecycle policies, a Cloud Scheduler cron job for daily pipeline triggers, and a least-privilege service account (`retailflow-pipeline-sa`) with only the IAM roles it needs.

The key benefit is environment parity: `terraform apply` produces an identical setup whether targeting dev, staging, or production. If someone makes a manual change in the GCP console, `terraform plan` detects the drift immediately. The Terraform state is stored in a GCS remote backend, making it team-safe."

---

### Q6. Why did you implement SCD Type 2 for customers and not for products or orders?

**A:** "SCD Type 2 is specifically designed for slowly changing dimension attributes — fields that change infrequently but where historical accuracy matters. In RetailFlow, customer attributes like `email`, `phone`, `city`, `state`, and `is_active` change over time (a customer relocates, changes email, deactivates their account). If I simply overwrote these values (SCD Type 1), I would lose the ability to answer historical questions like 'Which city was this customer in when they placed Order #1234?'

Orders, by contrast, are transactional facts — they don't change once placed. Products could theoretically change (price updates, category reclassification), but for this platform, product changes are handled by simply ingesting the latest catalog daily into the Bronze layer. The SCD2 implementation uses `dbt snapshot` with `strategy: check` on those 5 tracked columns, and the downstream `dim_customers_scd2` incremental model adds `_is_current`, `_version_num`, `_valid_from`, and `_valid_to` for analyst-friendly querying."

---

## Section 2: dbt and Data Modelling (6 Questions)

### Q7. Walk me through how you structured your dbt project.

**A:** "The RetailFlow dbt project follows a strict folder-based materialisation convention defined in `dbt_project.yml`. There are three model directories:

1. **`models/staging/`** — Silver layer. Contains 4 staging models (`stg_orders`, `stg_customers`, `stg_products`, `stg_clickstream`) materialised as **views**. Each model deduplicates using `ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY _ingested_at DESC)`, casts types, standardises naming, and adds enrichments.

2. **`models/scd/`** — SCD Type 2. Contains `dim_customers_scd2`, materialised as an **incremental** model. It processes only new/changed rows from the `customers_snapshot` by filtering on `dbt_updated_at > max(_dbt_updated_at)`.

3. **`models/marts/`** — Gold layer. Contains 5 mart models materialised as **tables**: `mart_sales_daily`, `mart_customer_ltv`, `mart_product_performance`, `mart_funnel`, and `mart_demand_forecast`.

I also have `snapshots/customers_snapshot.sql` for SCD2 change detection, `tests/` for 3 custom singular tests, and `macros/generate_surrogate_key.sql` for deterministic hashing. The project uses two packages: `dbt_utils` and `dbt_expectations`."

---

### Q8. Why did you materialise Silver models as views and Gold models as tables?

**A:** "It's a cost-vs-performance tradeoff. Silver staging models apply cleaning logic but aren't directly queried by business analysts — they're intermediate transformations. Materialising them as views means zero storage cost: the SQL runs on-the-fly when downstream models reference them. Since the raw data already exists in Bronze, duplicating it physically would be wasteful.

Gold mart models, however, are the terminal query layer. Business analysts, Streamlit dashboards, and Looker reports all hit Gold tables directly. Materialising them as physical tables in BigQuery means queries return instantly without re-executing the entire transformation chain. For `mart_sales_daily` alone, this avoids re-joining `stg_orders` with `stg_products` and re-aggregating across 1,820 order rows on every dashboard refresh."

---

### Q9. Explain how deduplication works in your staging models.

**A:** "Every staging model uses a CTE with `ROW_NUMBER()` to handle potential duplicate ingestion batches. For example, in `stg_orders.sql`:

```sql
ROW_NUMBER() OVER (
  PARTITION BY order_id
  ORDER BY _ingested_at DESC
) AS _row_num
```

This partitions by the natural key (`order_id`) and orders by ingestion timestamp descending, so the most recent version of each record gets `_row_num = 1`. The subsequent CTE filters with `WHERE _row_num = 1`. This pattern is critical because our ingestion scripts are designed for idempotent re-runs — if a script runs twice for the same date, the Bronze table receives duplicate rows. The staging layer silently resolves this, ensuring downstream marts never see duplicates. I verified this works correctly: the Bronze layer has 1,820 raw order rows, and after deduplication in `stg_orders`, the count is consistent."

---

### Q10. How does your `mart_customer_ltv` model work?

**A:** "The Customer Lifetime Value mart joins `stg_orders` with `dim_customers_scd2` (filtered to `_is_current = true`) and computes several RFM-inspired metrics per customer:

- **Total spend** (`total_revenue`): Sum of all order amounts.
- **Order count**: Number of distinct orders.
- **Average order value**: Total spend divided by order count.
- **Recency**: Days since the customer's last order.
- **Churn risk**: Classified as `high_risk` (>90 days inactive), `at_risk` (>60 days), `warming` (>30 days), or `active`.
- **LTV segment**: Categorised as `VIP` (top 10% by spend), `High` (top 25%), `Medium` (top 50%), or `Low`.

This enables the marketing team to segment customers for campaigns. For instance, a VIP customer flagged as `at_risk` would be a high-priority target for a re-engagement email."

---

### Q11. What is the `customers_snapshot` and how does it differ from `dim_customers_scd2`?

**A:** "The `customers_snapshot` is dbt's internal change-detection mechanism — a `dbt snapshot` with `strategy: check` on 5 columns (`email`, `phone`, `city`, `state`, `is_active`). On every run, dbt compares the current Bronze source against the existing snapshot rows. If any tracked column changes for a given `customer_id`, dbt closes the old row (sets `dbt_valid_to`) and inserts a new row (with `dbt_valid_from` = current timestamp).

The snapshot's output uses dbt's internal column conventions (`dbt_valid_from`, `dbt_scd_id`, `dbt_updated_at`), which aren't analyst-friendly. So I built `dim_customers_scd2` as a clean incremental model on top of it. This model renames columns to `_valid_from` / `_valid_to`, adds `_is_current` (boolean), `_version_num` (V1, V2, V3...), `age_bucket`, and a `customer_version_sk` surrogate key unique per version. Analysts only ever query `dim_customers_scd2`, never the raw snapshot."

---

### Q12. How do you generate surrogate keys in your dbt models?

**A:** "I wrote a custom macro `generate_surrogate_key.sql` that produces deterministic MD5 hashes from concatenated input columns. For example, in `stg_orders`, the surrogate key is generated from the `order_id` and `_ingested_at` columns. In `dim_customers_scd2`, the `customer_version_sk` is hashed from `customer_id` combined with `dbt_valid_from`, ensuring each historical version has a globally unique key.

I chose MD5 over `dbt_utils.generate_surrogate_key()` initially for transparency, but the project also includes `dbt_utils` as a dependency for other utilities like `accepted_range` tests. The key design principle is determinism: re-running the model with the same input always produces the same surrogate key, making incremental loads idempotent."

---

## Section 3: Data Quality and Testing (6 Questions)

### Q13. How did you implement data quality gates in RetailFlow?

**A:** "RetailFlow has a two-tier quality strategy:

**Tier 1 — Great Expectations (Pre-transformation):** Before any dbt model runs, `validate_bronze.py` executes three expectation suites against `raw_orders`, `raw_customers`, and `raw_products`. These check that `email` matches a regex pattern, `total_amount > 0`, `status` is in the accepted set (`pending`, `shipped`, `delivered`, `cancelled`), and `age` is between 18 and 100. If ANY expectation fails, the script exits with code 1, and Airflow's `ShortCircuitOperator` halts the entire downstream pipeline. Results are logged to `retailflow_metadata.ge_results`.

**Tier 2 — dbt tests (Post-transformation):** After dbt models build, 129 automated tests run. These include schema tests (`unique`, `not_null`, `accepted_values`, `relationships`) and 3 custom singular tests. This two-tier approach catches bad data at the gate (GE) AND validates transformation correctness (dbt)."

---

### Q14. What are the 3 custom singular tests you wrote, and why?

**A:** "Each custom test validates a complex business rule that schema tests cannot express:

1. **`assert_no_negative_revenue.sql`**: Queries `mart_sales_daily` and fails if any row has `total_revenue < 0`. Negative revenue would indicate a bug in the aggregation logic or corrupted source data.

2. **`assert_revenue_reconciliation.sql`**: Compares the sum of `total_amount` from `stg_orders` (filtered to non-cancelled orders) against the sum of `total_revenue` from `mart_sales_daily`. If they differ by more than ₹1 (tolerance for floating-point rounding), the test fails. This cross-table reconciliation ensures the Gold mart accurately represents the Silver source — I caught and fixed a real bug on Day 5 where aggressive BigQuery partitioning was silently dropping 28 days of data.

3. **`assert_funnel_monotonic.sql`**: Validates that the conversion funnel in `mart_funnel` is logically sane — total purchases cannot exceed total events. I relaxed the strict monotonicity check (views ≥ carts ≥ checkouts ≥ purchases) because synthetic clickstream data is fully randomised, but in production this would be strict."

---

### Q15. Tell me about a real data quality bug you caught and fixed during the build.

**A:** "On Day 5, after building the Gold marts, 3 out of 129 dbt tests failed:

**Bug 1 — Revenue Reconciliation Failure:** The `assert_revenue_reconciliation` test found that `mart_sales_daily` was missing 28 days of revenue compared to `stg_orders`. Root cause: I had configured aggressive BigQuery partitioning with a filter on `order_date` that was silently dropping rows outside the partition boundary. Fix: I removed the overly restrictive partition filter and let all 82 days of historical data flow through.

**Bug 2 — Null Average Order Value:** On days where every order happened to be cancelled, the `avg_order_value` calculation divided by zero, producing NULL. The `not_null` schema test caught this. Fix: I wrapped the division in `COALESCE(SAFE_DIVIDE(total_revenue, completed_orders), 0)`.

**Bug 3 — Funnel Monotonicity:** Synthetic clickstream data randomly generated more checkout events than add_to_cart events. Fix: Relaxed the test to a sanity check appropriate for synthetic data.

All 129 tests passed after these fixes."

---

### Q16. How do you test data quality in CI/CD before code reaches production?

**A:** "I built two GitHub Actions workflows:

1. **`dbt_ci.yml`** — Triggers on every Pull Request. It installs dbt, runs `dbt compile` to catch SQL syntax errors without GCP credentials, and optionally runs `dbt run` + `dbt test` against a CI target if GCP secrets are available. This prevents broken SQL from ever being merged.

2. **`ge_validation.yml`** — Triggers on push to `main`. It validates the JSON syntax of all expectation suite files, checks the YAML configuration, and runs the full Bronze validation against BigQuery. The DataDocs HTML report is uploaded as a GitHub Actions artifact for debugging.

This means every code change goes through automated quality gates before it can affect the production pipeline."

---

### Q17. What is the difference between Great Expectations and dbt tests in your pipeline?

**A:** "They serve fundamentally different purposes in the pipeline timeline:

**Great Expectations operates on input data (pre-transformation).** It validates the raw Bronze tables before any dbt model runs. Think of it as the security guard at the front door. If `raw_orders` has an order with `total_amount = -500` or `status = 'INVALID_STATUS'`, GE catches it before it can corrupt the Silver and Gold layers. In Airflow, the GE task uses a `ShortCircuitOperator` — if it fails, all downstream tasks are cleanly skipped.

**dbt tests operate on output data (post-transformation).** After dbt builds the staging views and Gold marts, 129 tests verify that the transformations worked correctly: surrogate keys are unique, expected columns are not null, range constraints are satisfied, and cross-table reconciliation checks pass.

Together, they create a 'defence in depth' strategy — bad data can't get in (GE), and transformation bugs can't get out (dbt)."

---

### Q18. How did you prove your data quality system actually works?

**A:** "I performed a deliberate 'Poisoned Data Test' on Day 2. I injected a row into `raw_orders` with `total_amount = -500.0`, `status = 'INVALID_STATUS'`, and `payment_method = 'bitcoin'`. When I re-ran `validate_bronze.py`, Great Expectations immediately detected exactly 4 violations, flagged the run as FAILED, wrote the failure to `retailflow_metadata.ge_results`, and generated an HTML DataDocs report detailing the exact row and columns that failed.

On Day 4, I proved SCD2 correctness by deliberately changing 5 customers' attributes (email, city, is_active, phone) and re-running `dbt snapshot`. The verification script confirmed all 5 customers had V1 (HISTORY) and V2 (CURRENT) rows with correct `_valid_from` / `_valid_to` timestamps. Then all 73 tests passed (at that point in the build)."

---

## Section 4: BigQuery Cost Optimisation (6 Questions)

### Q19. How did you optimise BigQuery query costs in RetailFlow?

**A:** "I implemented four layers of cost optimisation:

1. **Partitioning:** `mart_sales_daily` is partitioned by `order_date`, and `mart_funnel` is partitioned by `event_date`. When the Streamlit dashboard queries 'yesterday's sales', BigQuery prunes all irrelevant date partitions, scanning megabytes instead of the full table.

2. **Clustering:** Tables are clustered by high-cardinality filter columns — `top_category` on `mart_sales_daily`, `event_type` on `mart_funnel`. This further narrows the data scanned within a partition.

3. **View materialisation for Silver:** All staging models (`stg_orders`, `stg_customers`, `stg_products`, `stg_clickstream`) are views, not tables. This eliminates storage duplication — the SQL executes on-the-fly only when referenced by Gold models.

4. **Incremental models for SCD:** `dim_customers_scd2` uses `is_incremental()` to process only changed rows. On a typical daily run, this means processing <1% of the customer base rather than rebuilding the full history table."

---

### Q20. Explain what partitioning and clustering mean and how they work together.

**A:** "**Partitioning** splits a table into physical segments by a column value — in RetailFlow, we partition by date. BigQuery stores each partition separately. When a query includes `WHERE order_date = '2026-04-10'`, BigQuery reads only that one partition and completely ignores the rest. On `mart_sales_daily` with 82 days of data, this means scanning 1/82nd of the data.

**Clustering** sorts the data within each partition by specified columns. In `mart_sales_daily`, we cluster by `top_category`. So within the April 10th partition, all 'Electronics' rows are physically adjacent, all 'Clothing' rows are adjacent, etc. A query filtering by `top_category = 'Electronics'` can skip over the other category blocks entirely.

Together, partitioning provides coarse-grained pruning (which date?) and clustering provides fine-grained pruning (which category within that date?). The combined effect in RetailFlow reduced estimated scan sizes by up to 90% for typical dashboard queries."

---

### Q21. What is a Materialized View and how did you use it?

**A:** "A Materialized View is a pre-computed, cached query result that BigQuery automatically keeps in sync with the underlying table. I created `mv_daily_category_revenue` to cache daily category-level revenue aggregations. The Streamlit dashboard's Revenue Overview page hits this materialised view instead of re-running the aggregation every time.

**The important gotcha I encountered:** BigQuery does not support materialised views over logical views that use analytic window functions. My Silver staging models use `ROW_NUMBER()` for deduplication, so I could not build the MV directly on `stg_orders`. The solution: I built the materialised view on top of the Gold layer's `mart_sales_daily` (which is already a physical table), bypassing the window function limitation entirely. This architecture decision is a real-world BigQuery insight that demonstrates hands-on experience."

---

### Q22. How do you prevent analysts from accidentally running expensive queries?

**A:** "I built a Python utility called `estimate_query_cost.py` that uses BigQuery's Dry-Run API. When an analyst or engineer writes a SQL query, they can run it through this script before executing. The dry-run tells BigQuery to parse and plan the query without actually executing it, returning the exact number of bytes that would be processed. The script then calculates the estimated cost in USD ($6.25 per TB is BigQuery's on-demand pricing).

For example, if an analyst writes a query without a partition filter on `mart_sales_daily`, the dry-run might show '500 MB scanned, $0.003.' But if they accidentally query `raw_clickstream` without any filter, it might show '50 GB scanned, $0.31.' The team can set internal thresholds (e.g., reject anything over 10 GB) as a guardrail."

---

### Q23. Why did you choose on-demand pricing over flat-rate BigQuery pricing?

**A:** "RetailFlow's total dataset size is approximately 1,820 Bronze rows across all four tables, which is deliberately small for a portfolio project. At this scale, on-demand pricing is dramatically cheaper — our total monthly BigQuery bill is well under $1. Flat-rate pricing (BigQuery Reservations) makes financial sense when an organisation processes petabytes monthly with predictable workloads. For RetailFlow, the partitioning and clustering optimisations I implemented would deliver significant cost savings at scale while remaining on on-demand pricing, which is the more common setup in mid-tier data teams.

If this platform scaled to millions of daily orders, I would evaluate flat-rate Editions (Standard/Enterprise) based on the average slot utilisation shown in BigQuery's INFORMATION_SCHEMA.JOBS monitoring views."

---

### Q24. What happens to your GCS storage costs over time?

**A:** "I configured a 30-day lifecycle policy on the GCS Bronze bucket via Terraform. Raw CSV and JSON files older than 30 days are automatically deleted. This prevents unbounded storage growth — in a production environment ingesting daily, the bucket would level off at approximately 30 days' worth of raw files.

The rationale is that after 30 days, the data has been safely loaded into BigQuery Bronze (which has its own backup mechanisms), validated by Great Expectations, and transformed through the full dbt pipeline. Keeping the raw files beyond that point adds cost without practical value. If regulatory requirements demanded longer retention, I would modify the lifecycle rule or move aged files to a Nearline/Coldline storage class (which is 60-80% cheaper than Standard) rather than deleting them."

---

## Section 5: Full Pipeline and Orchestration (6 Questions)

### Q25. Walk me through the full RetailFlow pipeline end-to-end.

**A:** "The pipeline is orchestrated by an Apache Airflow DAG (`retailflow_dag.py`) with 18 tasks across 8 logical layers:

1. **Ingestion (Parallel):** 4 `PythonOperator` tasks run simultaneously — `fetch_orders.py` (FakeStore API → 220 orders), `generate_customers.py` (Faker → 500 customers), `generate_products.py` (Faker → 100 products), and `simulate_clickstream.py` (1,000+ clickstream events via Pub/Sub). All write to both GCS Bronze bucket and BigQuery Bronze tables.

2. **Quality Gate:** `validate_bronze.py` runs three Great Expectations suites (12+ expectations). Uses `ShortCircuitOperator` — if it returns False, all downstream tasks are skipped cleanly.

3. **dbt Staging:** Runs `dbt deps` → `dbt run --select staging` → `dbt test --select staging`. Builds 4 Silver views with deduplication, type casting, and enrichments.

4. **dbt SCD:** Runs `dbt snapshot` (change detection on 5 columns) → `dbt run --select scd` (builds `dim_customers_scd2`) → tests.

5. **dbt Marts:** Runs `dbt run --select marts` (builds 5 Gold tables) → `dbt test` (129 total tests). Validates revenue reconciliation, negative revenue, and funnel logic.

6. **Metadata Logging:** Writes a SUCCESS/FAILURE record to `retailflow_metadata.pipeline_runs` with batch IDs collected via XCom.

7. **Vertex AI Prediction:** Triggers `batch_predict.py` to send current product data to the trained AutoML model and write demand forecasts to `retailflow_predictions.demand_forecast`.

8. **End:** DAG completes. Total runtime: ~15 minutes."

---

### Q26. How does Airflow handle failures in your pipeline?

**A:** "I implemented multiple failure-handling mechanisms:

- **ShortCircuitOperator for GE:** If the Great Expectations quality gate fails, the operator returns `False`, and Airflow cleanly skips ALL downstream tasks (dbt staging, SCD, marts, ML predictions). No bad data reaches Silver or Gold.

- **`on_failure_callback`:** Every task has a callback function that writes a `FAILED` record to `retailflow_metadata.pipeline_runs` with the task name, exception message, and timestamp. This creates an auditable failure history.

- **Retry logic:** Tasks are configured with `retries=2` and `retry_delay=timedelta(minutes=5)`. Transient failures (like a brief BigQuery API timeout) are automatically retried before the task is marked failed.

- **SLA alerts:** The DAG has a 2-hour SLA. If the full pipeline exceeds this window, Airflow triggers an alert. Since our typical runtime is ~15 minutes, hitting the SLA indicates a serious infrastructure issue.

- **Idempotent design:** Every ingestion script generates a unique `batch_id`. If a script runs twice, the deduplication in the Silver layer eliminates the duplicates, so re-running the DAG is always safe."

---

### Q27. Why did you containerise Airflow with Docker instead of using Cloud Composer?

**A:** "Cloud Composer is Google's managed Airflow service and is the right choice for production teams. However, it costs approximately $300-500/month for a small environment, which is prohibitive for a portfolio project. Docker Compose gives me the identical Airflow experience — webserver, scheduler, PostgreSQL metadata DB — running locally for free.

My `docker-compose.yml` defines four services: the Airflow webserver (port 8080), the scheduler, the PostgreSQL backend, and a volume-mounted DAG directory. The `Dockerfile` installs dbt-bigquery, Great Expectations, and all Python dependencies into the Airflow image. This means the exact same DAG code would work on Cloud Composer with zero modifications — only the infrastructure provisioning changes."

---

### Q28. How did you integrate Vertex AI into the Airflow pipeline?

**A:** "On Day 9, I replaced the placeholder AI tasks in the DAG with real Python operators:

1. **Model Evaluation:** After the Gold marts finish building, Airflow calls `evaluate_model.py`, which queries the Vertex AI API for the trained AutoML model's RMSE metric and logs it to `retailflow_metadata`.

2. **Batch Prediction:** `batch_predict.py` reads the latest product features from `mart_product_performance`, sends them to the Vertex AI Batch Prediction API, and writes the forecast output (predicted `next_week_units_sold` per product) to `retailflow_predictions.demand_forecast` in BigQuery.

3. **DAG dependency:** The Vertex AI tasks are placed AFTER the `dbt_marts` TaskGroup, ensuring predictions always use the freshest Gold data. If dbt tests fail, the AI tasks never execute.

Due to AutoML quota limits on the free tier, the production DAG uses `generate_mock_predictions.py` for daily runs, while the full `train_automl.py` is triggered manually when retraining is needed (approximately monthly)."

---

### Q29. What is data lineage and how did you implement it?

**A:** "Data lineage tracks the journey of data from source to destination — how many rows entered each transformation step, how many exited, and what happened in between.

In RetailFlow, I built a lineage tracking system with three components:

1. **Metadata Table:** `retailflow_metadata.lineage` in BigQuery stores records with columns for `task_name`, `source_table`, `target_table`, `transformation_type`, `rows_in`, `rows_out`, and `run_timestamp`.

2. **Logger Script:** `log_lineage.py` is called by Airflow after each pipeline step. It queries the source and target table row counts and writes a lineage record. For example: `stg_orders` reads 1,820 rows from `raw_orders` and outputs 1,820 rows (after deduplication).

3. **Visual Dashboard:** Page 5 of the Streamlit app displays a Plotly Sankey diagram. The width of each flow arrow represents the volume of data moving between pipeline stages. Users can visually trace how raw Bronze data narrows through Silver cleaning, Gold aggregation, and finally into AI predictions.

This gives both engineers and stakeholders full transparency into pipeline health."

---

### Q30. If you had to scale RetailFlow to 100x the current data volume, what would you change?

**A:** "Several architectural changes would be needed:

1. **Streaming ingestion:** Replace the batch `simulate_clickstream.py` with a real Pub/Sub → Dataflow pipeline for true real-time clickstream processing, writing directly to BigQuery streaming buffer.

2. **Cloud Composer:** Move from Docker Compose Airflow to Google Cloud Composer for managed scheduling, auto-scaling workers, and built-in monitoring.

3. **dbt Cloud:** Migrate from dbt Core to dbt Cloud for managed scheduling, job queuing, and the dbt Semantic Layer — especially important with multiple data engineers contributing models.

4. **Partitioning at scale:** At 100x volume (~182,000 daily orders), the current daily partitioning remains efficient. But I would add partition expiration policies (e.g., expire partitions older than 2 years) and enforce partition filters as required in BigQuery table settings to prevent full table scans.

5. **Vertex AI Endpoints:** Replace batch prediction with a Vertex AI Online Endpoint for real-time demand scoring, with predictions cached in a Redis layer for sub-millisecond dashboard response times.

6. **Data contracts:** Implement formal schema contracts between the ingestion team and the transformation team using tools like `soda-core` or Protocol Buffers, so upstream schema changes don't silently break downstream models.

The core Medallion Architecture, dbt transformation layer, and quality gate strategy would remain unchanged — they're designed to scale."

---

> **Prepared by Zeeshan Yalakpalli** — CSE Final Year, Bengaluru
> RetailFlow Capstone Project — 14-Day Build
> GitHub: [zeeshan8088/cloud-data-engineer-portfolio](https://github.com/zeeshan8088/cloud-data-engineer-portfolio)
