"""
============================================================
RetailFlow — Lineage Logger
============================================================
Inserts a single lineage row into retailflow_metadata.lineage.

Usage:
    python log_lineage.py \
        --task_name "stg_orders" \
        --source_table "raw_orders" \
        --target_table "stg_orders" \
        --transform_type "staging" \
        --rows_in 1000 \
        --rows_out 980 \
        --pipeline_run_id "run-20260413-001"
============================================================
"""

import argparse
import uuid
import datetime
from google.cloud import bigquery

PROJECT_ID = "intricate-ward-459513-e1"
DATASET = "retailflow_metadata"
TABLE = "lineage"
FULL_TABLE = f"{PROJECT_ID}.{DATASET}.{TABLE}"


def log_lineage(
    task_name: str,
    source_table: str,
    target_table: str,
    transform_type: str,
    rows_in: int,
    rows_out: int,
    pipeline_run_id: str,
) -> None:
    """Insert one lineage record into BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)

    row = {
        "run_id": str(uuid.uuid4()),
        "task_name": task_name,
        "source_table": source_table,
        "target_table": target_table,
        "transform_type": transform_type,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "pipeline_run_id": pipeline_run_id,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    errors = client.insert_rows_json(FULL_TABLE, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")
    print(
        f"[OK] Logged lineage: {source_table} -> {target_table} "
        f"({rows_in}->{rows_out} rows) [task={task_name}]"
    )


def main():
    parser = argparse.ArgumentParser(description="Log a lineage hop to BigQuery")
    parser.add_argument("--task_name", required=True)
    parser.add_argument("--source_table", required=True)
    parser.add_argument("--target_table", required=True)
    parser.add_argument("--transform_type", required=True)
    parser.add_argument("--rows_in", required=True, type=int)
    parser.add_argument("--rows_out", required=True, type=int)
    parser.add_argument("--pipeline_run_id", required=True)
    args = parser.parse_args()

    log_lineage(
        task_name=args.task_name,
        source_table=args.source_table,
        target_table=args.target_table,
        transform_type=args.transform_type,
        rows_in=args.rows_in,
        rows_out=args.rows_out,
        pipeline_run_id=args.pipeline_run_id,
    )


if __name__ == "__main__":
    main()
