"""
RetailFlow — Master Orchestration DAG
======================================
Day 6 of the RetailFlow Intelligent Retail Data Platform capstone.

Pipeline layers (executed in strict dependency order):
  1. INGEST      → Pull raw data from all 4 sources into Bronze (BQ + GCS)
  2. QUALITY     → Great Expectations checkpoint: halt if Bronze quality fails
  3. STAGING     → dbt staging models (Silver layer): clean + type + dedup
  4. SCD         → dbt snapshot + dim_customers_scd2 (SCD Type 2)
  5. MARTS       → dbt Gold mart models: daily sales, LTV, product, funnel
  6. TEST        → dbt test --select tag:gold (all mart assertions)
  7. METADATA    → Write pipeline_runs lineage row to retailflow_metadata
  8. AI_TRIGGER  → Placeholder for Vertex AI batch predict (Day 9)

Schedule: daily at 02:00 UTC (07:30 IST)
Owner:    zeeshan | GCP: intricate-ward-459513-e1

Key Airflow features demonstrated:
  - TaskGroups for clean UI organisation
  - XCom: batch_id flows from ingestion → all downstream tasks
  - on_failure_callback: writes failure record to BQ metadata
  - SLA miss callback: sends alert when pipeline exceeds 2 hours
  - retries=2, retry_delay=5 min on every task
  - execution_timeout per-task to catch hung processes
"""

import json
import logging
import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger("retailflow.dag")

# ── Path resolution ───────────────────────────────────────────────────────────
# When running in Docker (local dev) the project is mounted at:
#   /opt/airflow/retailflow/
# When running in Cloud Composer, set RETAILFLOW_HOME env var to the correct path.
RETAILFLOW_HOME = Path(
    os.environ.get(
        "RETAILFLOW_HOME",
        "/opt/airflow/retailflow",   # Docker default
    )
)
INGESTION_DIR   = RETAILFLOW_HOME / "ingestion"
GE_DIR          = RETAILFLOW_HOME / "great_expectations"
DBT_DIR         = RETAILFLOW_HOME / "dbt"

# ── GCP ──────────────────────────────────────────────────────────────────────
GCP_PROJECT      = os.environ.get("GCP_PROJECT",      "intricate-ward-459513-e1")
BQ_DATASET_META  = os.environ.get("BQ_DATASET_META",  "retailflow_metadata")

# ── dbt binary path ───────────────────────────────────────────────────────────
DBT_BIN = os.environ.get("DBT_BIN", "dbt")   # override in .env if needed

# ── Shared dbt bash prefix ───────────────────────────────────────────────────
# Every dbt BashOperator uses this prefix so paths are explicit.
DBT_PREFIX = (
    f"cd {DBT_DIR} && "
    f"{DBT_BIN} --no-use-colors"
)


# ─────────────────────────────────────────────────────────────────────────────
#  CALLBACKS
# ─────────────────────────────────────────────────────────────────────────────

def _log_failure_to_bq(context: dict):
    """
    on_failure_callback — writes one row to retailflow_metadata.pipeline_runs
    whenever ANY task in the DAG fails.

    Airflow passes 'context' automatically.  We pull the important bits and
    insert a FAILED record so the metadata table always has a full audit trail.
    """
    try:
        from google.cloud import bigquery as bq_client_lib

        client = bq_client_lib.Client(project=GCP_PROJECT)
        table_id = f"{GCP_PROJECT}.{BQ_DATASET_META}.pipeline_runs"

        task_instance = context.get("task_instance")
        dag_run = context.get("dag_run")

        row = {
            "run_id":         str(dag_run.run_id) if dag_run else "unknown",
            "dag_id":         context.get("dag").dag_id,
            "task_id":        str(task_instance.task_id) if task_instance else "unknown",
            "status":         "FAILED",
            "started_at":     (task_instance.start_date.isoformat()
                               if task_instance and task_instance.start_date else None),
            "ended_at":       datetime.now(timezone.utc).isoformat(),
            "batch_id":       (task_instance.xcom_pull(key="batch_id")
                               if task_instance else None),
            "error_message":  str(context.get("exception", ""))[:1024],
            "logged_at":      datetime.now(timezone.utc).isoformat(),
        }

        schema = [
            bq_client_lib.SchemaField("run_id",        "STRING"),
            bq_client_lib.SchemaField("dag_id",        "STRING"),
            bq_client_lib.SchemaField("task_id",       "STRING"),
            bq_client_lib.SchemaField("status",        "STRING"),
            bq_client_lib.SchemaField("started_at",    "STRING"),
            bq_client_lib.SchemaField("ended_at",      "STRING"),
            bq_client_lib.SchemaField("batch_id",      "STRING"),
            bq_client_lib.SchemaField("error_message", "STRING"),
            bq_client_lib.SchemaField("logged_at",     "STRING"),
        ]
        job_config = bq_client_lib.LoadJobConfig(
            schema=schema,
            write_disposition=bq_client_lib.WriteDisposition.WRITE_APPEND,
            create_disposition=bq_client_lib.CreateDisposition.CREATE_IF_NEEDED,
        )
        job = client.load_table_from_json([row], table_id, job_config=job_config)
        job.result()
        logger.info(f"Failure logged to {table_id} for task {row['task_id']}")
    except Exception as exc:
        # Never let the callback crash Airflow — just log and move on.
        logger.error(f"Could not log failure to BQ: {exc}")


def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Called when any task misses its SLA window (2 hours).
    In production: send Slack/Email alert.  Here: log to console.
    """
    logger.warning(
        "⚠️  SLA MISS DETECTED | DAG: %s | Missed tasks: %s",
        dag.dag_id,
        [t.task_id for t in blocking_task_list],
    )


# ─────────────────────────────────────────────────────────────────────────────
#  DEFAULT ARGS
# ─────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner":             "zeeshan",
    "depends_on_past":   False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "on_failure_callback": _log_failure_to_bq,
    "email_on_failure":  False,   # Set to True + configure SMTP for production
    "email_on_retry":    False,
    "execution_timeout": timedelta(hours=1),
}


# ─────────────────────────────────────────────────────────────────────────────
#  TASK CALLABLES  (PythonOperator functions)
# ─────────────────────────────────────────────────────────────────────────────

def _run_ingestion_script(script_name: str, **context) -> dict:
    """
    Generic wrapper — runs an ingestion script as a subprocess and
    pushes its batch_id to XCom so downstream tasks can reference it.

    Using subprocess instead of importing directly so the Airflow worker
    doesn't need all ingestion dependencies installed system-wide.
    The env is inherited, so GCP_PROJECT etc. flow through.
    """
    script_path = INGESTION_DIR / script_name
    env = {**os.environ, "PYTHONPATH": str(INGESTION_DIR)}

    logger.info("Running ingestion script: %s", script_path)
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        env=env,
        timeout=1800,  # 30-min hard cap per ingestion script
    )

    if result.stdout:
        logger.info("STDOUT:\n%s", result.stdout[-3000:])   # last 3k chars
    if result.stderr:
        logger.warning("STDERR:\n%s", result.stderr[-3000:])

    if result.returncode != 0:
        raise RuntimeError(
            f"Ingestion script FAILED (exit {result.returncode}): {script_name}\n"
            f"STDERR: {result.stderr[-1000:]}"
        )

    # Try to extract batch_id from stdout (scripts log it as JSON at the end)
    batch_id = None
    for line in result.stdout.splitlines():
        if "batch_id" in line:
            try:
                # Scripts emit: Complete: {"status": "success", "batch_id": "..."}
                payload = json.loads(line.split("Complete: ", 1)[-1])
                batch_id = payload.get("batch_id")
                break
            except (json.JSONDecodeError, IndexError):
                pass

    if batch_id:
        context["ti"].xcom_push(key="batch_id", value=batch_id)
        logger.info("Pushed batch_id=%s to XCom", batch_id)

    return {"script": script_name, "batch_id": batch_id, "returncode": result.returncode}


def _ingest_orders(**context):
    return _run_ingestion_script("fetch_orders.py", **context)


def _ingest_customers(**context):
    return _run_ingestion_script("generate_customers.py", **context)


def _ingest_products(**context):
    return _run_ingestion_script("generate_products.py", **context)


def _ingest_clickstream(**context):
    return _run_ingestion_script("simulate_clickstream.py", **context)


def _run_ge_validation(**context) -> bool:
    """
    Runs the Great Expectations validate_bronze.py script.

    Returns True if all suites pass (pipeline continues).
    Returns False — which causes ShortCircuitOperator to skip ALL downstream
    tasks — if any GE suite fails.  This is the "fail-fast" quality gate.
    """
    script_path = GE_DIR / "validate_bronze.py"
    env = {**os.environ, "PYTHONPATH": str(GE_DIR)}

    logger.info("Running GE validation: %s", script_path)
    result = subprocess.run(
        [sys.executable, str(script_path), "--verbose"],
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
    )

    if result.stdout:
        logger.info("GE STDOUT:\n%s", result.stdout[-5000:])
    if result.stderr:
        logger.warning("GE STDERR:\n%s", result.stderr[-2000:])

    passed = result.returncode == 0

    if passed:
        logger.info("✅ GE Quality Gate: ALL SUITES PASSED — pipeline continues")
    else:
        logger.error(
            "❌ GE Quality Gate: SUITES FAILED — skipping all downstream tasks!\n"
            "Check DataDocs report at: great_expectations/uncommitted/data_docs/"
        )

    return passed   # ShortCircuitOperator uses this to continue or skip


def _log_pipeline_success(**context):
    """
    Final task: write a SUCCESS row to retailflow_metadata.pipeline_runs.
    Also collects batch_ids from XCom across all ingestion tasks.
    """
    try:
        from google.cloud import bigquery as bq_lib

        client = bq_lib.Client(project=GCP_PROJECT)
        table_id = f"{GCP_PROJECT}.{BQ_DATASET_META}.pipeline_runs"

        dag_run = context["dag_run"]
        ti      = context["ti"]

        # Collect batch_ids from all ingestion tasks via XCom
        batch_ids = []
        for task_name in ["ingest_orders", "ingest_customers",
                          "ingest_products", "ingest_clickstream"]:
            try:
                b = ti.xcom_pull(task_ids=f"ingestion.{task_name}", key="batch_id")
                if b:
                    batch_ids.append(b)
            except Exception:
                pass

        row = {
            "run_id":         str(dag_run.run_id),
            "dag_id":         context["dag"].dag_id,
            "task_id":        "log_pipeline_success",
            "status":         "SUCCESS",
            "started_at":     dag_run.start_date.isoformat() if dag_run.start_date else None,
            "ended_at":       datetime.now(timezone.utc).isoformat(),
            "batch_id":       ",".join(batch_ids) if batch_ids else None,
            "error_message":  None,
            "logged_at":      datetime.now(timezone.utc).isoformat(),
        }

        schema = [
            bq_lib.SchemaField("run_id",        "STRING"),
            bq_lib.SchemaField("dag_id",        "STRING"),
            bq_lib.SchemaField("task_id",       "STRING"),
            bq_lib.SchemaField("status",        "STRING"),
            bq_lib.SchemaField("started_at",    "STRING"),
            bq_lib.SchemaField("ended_at",      "STRING"),
            bq_lib.SchemaField("batch_id",      "STRING"),
            bq_lib.SchemaField("error_message", "STRING"),
            bq_lib.SchemaField("logged_at",     "STRING"),
        ]
        job_cfg = bq_lib.LoadJobConfig(
            schema=schema,
            write_disposition=bq_lib.WriteDisposition.WRITE_APPEND,
            create_disposition=bq_lib.CreateDisposition.CREATE_IF_NEEDED,
        )
        job = client.load_table_from_json([row], table_id, job_config=job_cfg)
        job.result()
        logger.info("✅ Pipeline SUCCESS logged to %s | run_id=%s", table_id, row["run_id"])

    except Exception as exc:
        logger.error("Could not log success to BQ (non-fatal): %s", exc)


def _trigger_vertex_ai_placeholder(**context):
    """
    Day 9 placeholder for Vertex AI batch prediction trigger.
    On Day 9 this is replaced with the real VertexAI API call.
    """
    logger.info(
        "🤖 Vertex AI trigger placeholder — will be wired on Day 9.\n"
        "   For now: verifying retailflow_gold mart tables are accessible..."
    )
    # Lightweight BQ check so the task does real work
    from google.cloud import bigquery as bq_lib
    client = bq_lib.Client(project=GCP_PROJECT)
    for table in ["mart_sales_daily", "mart_customer_ltv",
                  "mart_product_performance", "mart_funnel"]:
        q = (f"SELECT COUNT(*) as cnt FROM "
             f"`{GCP_PROJECT}.retailflow_gold.{table}` LIMIT 1")
        rows = list(client.query(q).result())
        cnt = rows[0]["cnt"]
        logger.info("  ✅ %s — %d rows ready for AI features", table, cnt)

    return {"status": "placeholder_ok", "day": 9}


# ─────────────────────────────────────────────────────────────────────────────
#  DAG DEFINITION
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="retailflow_master",
    default_args=default_args,
    description=(
        "RetailFlow E2E Pipeline: "
        "Bronze Ingestion → GE Quality Gate → dbt Silver → dbt Gold → AI"
    ),
    schedule_interval="0 2 * * *",          # 02:00 UTC = 07:30 IST daily
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,                       # Never allow parallel DAG runs
    tags=["retailflow", "capstone", "production"],
    sla_miss_callback=_sla_miss_callback,
    doc_md=__doc__,
) as dag:

    # ── Start sentinel ────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ─────────────────────────────────────────────────────────────────────────
    #  LAYER 1: INGESTION  (all 4 run in parallel)
    # ─────────────────────────────────────────────────────────────────────────
    with TaskGroup("ingestion", tooltip="Bronze layer — raw data ingestion") as tg_ingest:

        ingest_orders = PythonOperator(
            task_id="ingest_orders",
            python_callable=_ingest_orders,
            sla=timedelta(minutes=30),
            doc_md=(
                "Fetches 220 orders from FakeStore API + generates synthetic data.\n"
                "Writes to GCS Bronze zone and BigQuery raw_orders.\n"
                "Pushes batch_id to XCom."
            ),
        )

        ingest_customers = PythonOperator(
            task_id="ingest_customers",
            python_callable=_ingest_customers,
            sla=timedelta(minutes=20),
            doc_md=(
                "Generates 500 synthetic customers using Faker.\n"
                "Writes to GCS Bronze zone and BigQuery raw_customers."
            ),
        )

        ingest_products = PythonOperator(
            task_id="ingest_products",
            python_callable=_ingest_products,
            sla=timedelta(minutes=20),
            doc_md=(
                "Generates 100 synthetic products with realistic pricing.\n"
                "Writes to GCS Bronze zone and BigQuery raw_products."
            ),
        )

        ingest_clickstream = PythonOperator(
            task_id="ingest_clickstream",
            python_callable=_ingest_clickstream,
            sla=timedelta(minutes=30),
            doc_md=(
                "Simulates 1000 clickstream events (view, add_to_cart, checkout).\n"
                "Publishes to Pub/Sub and writes to BigQuery raw_clickstream."
            ),
        )

        # All 4 run in parallel — no dependencies within the group
        [ingest_orders, ingest_customers, ingest_products, ingest_clickstream]

    # ─────────────────────────────────────────────────────────────────────────
    #  LAYER 2: DATA QUALITY GATE  (ShortCircuit — skips all downstream if FAIL)
    # ─────────────────────────────────────────────────────────────────────────
    ge_quality_gate = ShortCircuitOperator(
        task_id="ge_bronze_quality_gate",
        python_callable=_run_ge_validation,
        sla=timedelta(minutes=20),
        doc_md=(
            "Runs Great Expectations validate_bronze.py against all 3 Bronze tables.\n"
            "EXIT CODE 0 → pipeline continues.\n"
            "EXIT CODE 1 → SHORT CIRCUITS: all downstream tasks are SKIPPED.\n"
            "Results logged to retailflow_metadata.ge_results + DataDocs HTML report."
        ),
    )

    # ─────────────────────────────────────────────────────────────────────────
    #  LAYER 3: dbt STAGING  (Silver layer — all 4 models in parallel via dbt)
    # ─────────────────────────────────────────────────────────────────────────
    with TaskGroup("dbt_staging", tooltip="dbt — Silver staging layer") as tg_staging:

        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"{DBT_PREFIX} deps --profiles-dir {DBT_DIR}",
            doc_md="Install dbt packages (dbt_utils, dbt_expectations).",
        )

        dbt_staging_run = BashOperator(
            task_id="dbt_run_staging",
            bash_command=(
                f"{DBT_PREFIX} run "
                f"--profiles-dir {DBT_DIR} "
                f"--select tag:staging "
                f"--target dev"
            ),
            sla=timedelta(minutes=30),
            doc_md=(
                "Runs all Silver staging models:\n"
                "  stg_orders, stg_customers, stg_products, stg_clickstream\n"
                "Materialized as BigQuery VIEWS in retailflow_silver dataset."
            ),
        )

        dbt_staging_test = BashOperator(
            task_id="dbt_test_staging",
            bash_command=(
                f"{DBT_PREFIX} test "
                f"--profiles-dir {DBT_DIR} "
                f"--select tag:staging "
                f"--target dev"
            ),
            sla=timedelta(minutes=20),
            doc_md="Run all schema + singular tests on staging models.",
        )

        dbt_deps >> dbt_staging_run >> dbt_staging_test

    # ─────────────────────────────────────────────────────────────────────────
    #  LAYER 4: dbt SCD TYPE 2  (snapshot + incremental dim)
    # ─────────────────────────────────────────────────────────────────────────
    with TaskGroup("dbt_scd", tooltip="dbt — SCD Type 2 customer dimension") as tg_scd:

        dbt_snapshot = BashOperator(
            task_id="dbt_snapshot",
            bash_command=(
                f"{DBT_PREFIX} snapshot "
                f"--profiles-dir {DBT_DIR} "
                f"--select customers_snapshot "
                f"--target dev"
            ),
            sla=timedelta(minutes=15),
            doc_md=(
                "Runs customers_snapshot — compares Bronze raw_customers against\n"
                "the existing snapshot. Closes old records, inserts new versions\n"
                "when email / phone / city / state / is_active changes.\n"
                "Result: retailflow_silver.customers_snapshot"
            ),
        )

        dbt_scd2_run = BashOperator(
            task_id="dbt_run_scd2",
            bash_command=(
                f"{DBT_PREFIX} run "
                f"--profiles-dir {DBT_DIR} "
                f"--select dim_customers_scd2 "
                f"--target dev"
            ),
            sla=timedelta(minutes=15),
            doc_md=(
                "Builds dim_customers_scd2 incrementally on top of customers_snapshot.\n"
                "Adds _valid_from, _valid_to, _is_current, _version_num, age_bucket.\n"
                "Result: retailflow_silver.dim_customers_scd2"
            ),
        )

        dbt_scd_test = BashOperator(
            task_id="dbt_test_scd",
            bash_command=(
                f"{DBT_PREFIX} test "
                f"--profiles-dir {DBT_DIR} "
                f"--select tag:scd "
                f"--target dev"
            ),
            doc_md="Run schema tests on SCD layer — unique surrogate keys, no nulls.",
        )

        dbt_snapshot >> dbt_scd2_run >> dbt_scd_test

    # ─────────────────────────────────────────────────────────────────────────
    #  LAYER 5: dbt GOLD MARTS  (4 mart models in one dbt run)
    # ─────────────────────────────────────────────────────────────────────────
    with TaskGroup("dbt_marts", tooltip="dbt — Gold mart layer") as tg_marts:

        dbt_marts_run = BashOperator(
            task_id="dbt_run_marts",
            bash_command=(
                f"{DBT_PREFIX} run "
                f"--profiles-dir {DBT_DIR} "
                f"--select tag:mart "
                f"--target dev"
            ),
            sla=timedelta(minutes=30),
            doc_md=(
                "Builds all 4 Gold mart tables in retailflow_gold:\n"
                "  mart_sales_daily     — daily revenue KPIs\n"
                "  mart_customer_ltv    — LTV, churn risk, RFM scores\n"
                "  mart_product_performance — product leaderboard\n"
                "  mart_funnel          — website conversion funnel"
            ),
        )

        dbt_marts_test = BashOperator(
            task_id="dbt_test_marts",
            bash_command=(
                f"{DBT_PREFIX} test "
                f"--profiles-dir {DBT_DIR} "
                f"--select tag:gold "
                f"--target dev"
            ),
            sla=timedelta(minutes=20),
            doc_md=(
                "Full test suite on Gold layer including:\n"
                "  - Column schema tests (not_null, unique, accepted_values)\n"
                "  - Revenue reconciliation singular test\n"
                "  - Funnel monotonic sanity test\n"
                "  - No negative revenue assertion"
            ),
        )

        dbt_docs_generate = BashOperator(
            task_id="dbt_docs_generate",
            bash_command=(
                f"{DBT_PREFIX} docs generate "
                f"--profiles-dir {DBT_DIR} "
                f"--target dev"
            ),
            doc_md=(
                "Regenerates dbt docs site (catalog.json + manifest.json).\n"
                "Run `dbt docs serve` locally to explore the lineage graph.\n"
                "On Day 7 this will be published to GitHub Pages."
            ),
        )

        dbt_marts_run >> dbt_marts_test >> dbt_docs_generate

    # ─────────────────────────────────────────────────────────────────────────
    #  LAYER 6: PIPELINE METADATA LOGGING
    # ─────────────────────────────────────────────────────────────────────────
    log_lineage = PythonOperator(
        task_id="log_pipeline_success",
        python_callable=_log_pipeline_success,
        doc_md=(
            "Writes SUCCESS record to retailflow_metadata.pipeline_runs.\n"
            "Captures: run_id, dag_id, all batch_ids, start/end times.\n"
            "Queried by the Day 12 Streamlit lineage graph app."
        ),
    )

    # ─────────────────────────────────────────────────────────────────────────
    #  LAYER 7: VERTEX AI TRIGGER  (placeholder until Day 9)
    # ─────────────────────────────────────────────────────────────────────────
    trigger_vertex = PythonOperator(
        task_id="trigger_vertex_ai",
        python_callable=_trigger_vertex_ai_placeholder,
        doc_md=(
            "Day 9 placeholder — currently validates Gold table row counts.\n"
            "On Day 9: replaced with real Vertex AI AutoML batch prediction trigger.\n"
            "Reads from mart_product_performance → predicts next_week_units_sold."
        ),
    )

    # ── End sentinel ──────────────────────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ─────────────────────────────────────────────────────────────────────────
    #  DEPENDENCY GRAPH
    # ─────────────────────────────────────────────────────────────────────────
    #
    #  start
    #    └─► [ingest_orders, ingest_customers, ingest_products, ingest_clickstream]  (parallel)
    #              └─► ge_bronze_quality_gate  (ShortCircuit — fail-fast gate)
    #                    └─► dbt_staging (deps → run → test)
    #                          └─► dbt_scd (snapshot → scd2 → test)
    #                                └─► dbt_marts (run → test → docs)
    #                                      └─► log_lineage
    #                                            └─► trigger_vertex_ai
    #                                                  └─► end
    #
    start >> tg_ingest >> ge_quality_gate >> tg_staging >> tg_scd >> tg_marts
    tg_marts >> log_lineage >> trigger_vertex >> end
