# src/bq_writer.py
# ----------------------------------------------------------
# Purpose: Write anomaly results to BigQuery.
#
# This is the "filing clerk" module — it takes completed
# anomaly + explanation records and writes them to the
# ecommerce_quality_monitor.anomaly_reports table.
#
# It creates the table automatically on first run if it
# doesn't exist yet — no manual table creation needed.
# ----------------------------------------------------------

import uuid
from datetime import datetime, timezone
from google.cloud import bigquery

# ── Config ────────────────────────────────────────────────
PROJECT_ID = "intricate-ward-459513-e1"
DATASET_ID = "ecommerce_quality_monitor"
TABLE_ID   = "anomaly_reports"
FULL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# ── Schema definition ─────────────────────────────────────
# This tells BigQuery exactly what columns our table has
# and what type of data each column holds.
# Think of this as designing the form before printing copies.
SCHEMA = [
    bigquery.SchemaField("run_id",             "STRING",    description="Unique pipeline run identifier"),
    bigquery.SchemaField("order_id",           "STRING",    description="Order ID with the anomaly"),
    bigquery.SchemaField("anomaly_type",       "STRING",    description="Category of anomaly detected"),
    bigquery.SchemaField("description",        "STRING",    description="Plain-English description of the issue"),
    bigquery.SchemaField("gemini_explanation", "STRING",    description="Gemini AI root cause analysis"),
    bigquery.SchemaField("detected_at",        "TIMESTAMP", description="When the pipeline run occurred"),
]


# ── Table creator ─────────────────────────────────────────
def ensure_table_exists(client: bigquery.Client):
    """
    Create the anomaly_reports table if it doesn't exist.
    If it already exists, do nothing.

    Think of this like: "create the filing drawer if it's
    not there yet — but don't wipe it if it already exists."
    This is called idempotent setup — safe to run repeatedly.
    """
    table_ref = bigquery.Table(FULL_TABLE, schema=SCHEMA)

    try:
        client.get_table(FULL_TABLE)
        # Table exists — nothing to do
    except Exception:
        # Table doesn't exist — create it
        client.create_table(table_ref)
        print(f"  ✅ Created table: {FULL_TABLE}")


# ── Row builder ───────────────────────────────────────────
def build_rows(anomalies_with_explanations: list[dict], run_id: str) -> list[dict]:
    """
    Convert our internal anomaly dicts into BigQuery row dicts.

    Each row must match the schema exactly — same field names,
    compatible data types.
    """
    detected_at = datetime.now(timezone.utc).isoformat()

    rows = []
    for item in anomalies_with_explanations:
        rows.append({
            "run_id":             run_id,
            "order_id":           item["anomaly"]["order_id"],
            "anomaly_type":       item["anomaly"]["anomaly_type"],
            "description":        item["anomaly"]["description"],
            "gemini_explanation": item["explanation"],
            "detected_at":        detected_at,
        })
    return rows


# ── Main writer function ──────────────────────────────────
def write_to_bigquery(anomalies_with_explanations: list[dict]) -> str:
    """
    Write all anomaly results to BigQuery in one batch.

    Args:
        anomalies_with_explanations: list of dicts, each with:
            - "anomaly": the anomaly dict from detect_anomalies
            - "explanation": the Gemini explanation string

    Returns:
        run_id: the unique ID used for this pipeline run
                (useful for logging and debugging)
    """

    # Generate a unique run ID for this pipeline execution
    # Think of it like a batch number on a shipment —
    # every run gets its own ID so you can query by run later
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    print(f"\n📤 Writing {len(anomalies_with_explanations)} rows to BigQuery...")
    print(f"   Run ID: {run_id}")
    print(f"   Table:  {FULL_TABLE}")

    # Initialise BigQuery client
    # Uses your gcloud Application Default Credentials
    # automatically — same credentials from Week 1
    client = bigquery.Client(project=PROJECT_ID)

    # Ensure table exists (creates it on first run)
    ensure_table_exists(client)

    # Build rows in BigQuery format
    rows = build_rows(anomalies_with_explanations, run_id)

    # Insert rows — BigQuery's insert_rows_json accepts a
    # list of dicts and handles the rest
    errors = client.insert_rows_json(FULL_TABLE, rows)

    if errors:
        print(f"  ⚠️  BigQuery insert errors: {errors}")
    else:
        print(f"  ✅ Successfully wrote {len(rows)} rows to BigQuery")

    return run_id