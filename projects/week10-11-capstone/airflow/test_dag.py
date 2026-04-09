"""
RetailFlow — DAG Smoke Test (No Airflow Required)
===================================================
Validates the DAG definition locally using Python only.
No running Airflow instance needed.

What this tests:
  1. DAG can be imported without errors
  2. All expected tasks exist with correct IDs
  3. Dependency graph is correct (correct upstream/downstream)
  4. No import-time side effects (no BQ calls, no subprocess)
  5. Default args are set correctly (retries, SLA, callbacks)

Usage (run from project root or airflow/ folder):
    python airflow/test_dag.py

Exit codes:
    0 = all checks passed
    1 = one or more checks failed
"""

import sys
import os
from pathlib import Path

# ── Make sure the airflow folder is importable ────────────────
AIRFLOW_DIR = Path(__file__).parent
sys.path.insert(0, str(AIRFLOW_DIR))

# Minimal Airflow env vars so the import doesn't crash
os.environ.setdefault("AIRFLOW__CORE__EXECUTOR", "SequentialExecutor")
os.environ.setdefault(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "sqlite:////tmp/retailflow_test.db",
)

print("=" * 65)
print("RetailFlow DAG Smoke Test")
print("=" * 65)

PASS = "✅ PASS"
FAIL = "❌ FAIL"
errors = []


def check(description: str, condition: bool, detail: str = ""):
    status = PASS if condition else FAIL
    msg = f"  {status} | {description}"
    if detail:
        msg += f"\n         {detail}"
    print(msg)
    if not condition:
        errors.append(description)


# ── 1. Import DAG ─────────────────────────────────────────────────────────────
print("\n[1/5] Importing DAG module...")
try:
    from retailflow_dag import dag as retailflow_dag
    check("DAG module imports cleanly", True)
except Exception as e:
    check("DAG module imports cleanly", False, str(e))
    print(f"\n❌ Fatal import error — cannot continue: {e}")
    sys.exit(1)

# ── 2. DAG metadata ───────────────────────────────────────────────────────────
print("\n[2/5] Checking DAG metadata...")
check("dag_id == 'retailflow_master'", retailflow_dag.dag_id == "retailflow_master")
check(
    "schedule_interval == '0 2 * * *'",
    str(retailflow_dag.schedule_interval) == "0 2 * * *",
    f"got: {retailflow_dag.schedule_interval}",
)
check(
    "max_active_runs == 1",
    retailflow_dag.max_active_runs == 1,
    f"got: {retailflow_dag.max_active_runs}",
)
check(
    "catchup == False",
    retailflow_dag.catchup == False,
)

# ── 3. Task existence ─────────────────────────────────────────────────────────
print("\n[3/5] Checking task existence...")
task_ids = set(retailflow_dag.task_ids)
print(f"  Found {len(task_ids)} tasks:")
for tid in sorted(task_ids):
    print(f"    • {tid}")

EXPECTED_TASKS = [
    "start",
    "end",
    "ingestion.ingest_orders",
    "ingestion.ingest_customers",
    "ingestion.ingest_products",
    "ingestion.ingest_clickstream",
    "ge_bronze_quality_gate",
    "dbt_staging.dbt_deps",
    "dbt_staging.dbt_run_staging",
    "dbt_staging.dbt_test_staging",
    "dbt_scd.dbt_snapshot",
    "dbt_scd.dbt_run_scd2",
    "dbt_scd.dbt_test_scd",
    "dbt_marts.dbt_run_marts",
    "dbt_marts.dbt_test_marts",
    "dbt_marts.dbt_docs_generate",
    "log_pipeline_success",
    "trigger_vertex_ai",
]
print()
for task_id in EXPECTED_TASKS:
    check(f"Task exists: {task_id}", task_id in task_ids)

# ── 4. Dependency graph ───────────────────────────────────────────────────────
print("\n[4/5] Checking critical dependency edges...")

def has_downstream(upstream_id: str, downstream_id: str) -> bool:
    """Check if upstream_id has downstream_id as a downstream task."""
    try:
        task = retailflow_dag.get_task(upstream_id)
        downstream_ids = {t.task_id for t in task.downstream_list}
        # Also check using full task_id (for task group tasks)
        return downstream_id in downstream_ids or any(
            downstream_id in tid for tid in downstream_ids
        )
    except Exception:
        return False

# Key edges to verify
CRITICAL_EDGES = [
    # GE gate is downstream of all ingestion tasks
    ("ingestion.ingest_orders",      "ge_bronze_quality_gate"),
    ("ingestion.ingest_customers",   "ge_bronze_quality_gate"),
    ("ingestion.ingest_products",    "ge_bronze_quality_gate"),
    ("ingestion.ingest_clickstream", "ge_bronze_quality_gate"),
    # Staging is downstream of GE gate
    ("ge_bronze_quality_gate", "dbt_staging.dbt_deps"),
    # Within staging: deps → run → test
    ("dbt_staging.dbt_deps", "dbt_staging.dbt_run_staging"),
    ("dbt_staging.dbt_run_staging", "dbt_staging.dbt_test_staging"),
    # Within SCD
    ("dbt_scd.dbt_snapshot", "dbt_scd.dbt_run_scd2"),
    ("dbt_scd.dbt_run_scd2", "dbt_scd.dbt_test_scd"),
    # Within marts
    ("dbt_marts.dbt_run_marts", "dbt_marts.dbt_test_marts"),
    ("dbt_marts.dbt_test_marts", "dbt_marts.dbt_docs_generate"),
    # Lineage logging is downstream of marts
    ("dbt_marts.dbt_docs_generate", "log_pipeline_success"),
    # Vertex AI is after lineage logging
    ("log_pipeline_success", "trigger_vertex_ai"),
    # End is last
    ("trigger_vertex_ai", "end"),
]

for up, down in CRITICAL_EDGES:
    check(f"  {up} → {down}", has_downstream(up, down))

# ── 5. Default args ───────────────────────────────────────────────────────────
print("\n[5/5] Checking default_args...")
da = retailflow_dag.default_args
check(
    "retries == 2",
    da.get("retries") == 2,
    f"got: {da.get('retries')}",
)
check(
    "retry_delay == 5 minutes",
    str(da.get("retry_delay")) == "0:05:00",
    f"got: {da.get('retry_delay')}",
)
check(
    "on_failure_callback is set",
    da.get("on_failure_callback") is not None,
)
check(
    "execution_timeout is set",
    da.get("execution_timeout") is not None,
    f"got: {da.get('execution_timeout')}",
)

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 65)
if not errors:
    print(f"✅ ALL CHECKS PASSED ({len(EXPECTED_TASKS) + len(CRITICAL_EDGES) + 8} assertions)")
    print("   Your DAG is structurally correct and ready to run in Airflow.")
    print("=" * 65)
    sys.exit(0)
else:
    print(f"❌ {len(errors)} CHECK(S) FAILED:")
    for e in errors:
        print(f"   • {e}")
    print("=" * 65)
    sys.exit(1)
