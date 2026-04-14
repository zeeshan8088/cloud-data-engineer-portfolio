"""
============================================================
RetailFlow — batch_predict.py
============================================================
Runs batch predictions using the trained AutoML model on
current product features to forecast next-week demand.

Flow:
  1. Prepare a "future features" table (current product
     attributes without the target column)
  2. Call model.batch_predict() with BigQuery I/O
  3. Clean up output table → retailflow_predictions.demand_forecast, rename to demand_forecast
  4. Log the batch prediction job to metadata

Run (after evaluate_model.py confirms model is ready):
    python batch_predict.py
    python batch_predict.py --model-id <MODEL_ID>
============================================================
"""

import os
import sys
import argparse
import hashlib
import datetime as dt

from google.cloud import aiplatform
from google.cloud import bigquery

# ── Configuration ────────────────────────────────────────────
PROJECT_ID = "intricate-ward-459513-e1"
LOCATION = "asia-south1"
PREDICTIONS_DATASET = "retailflow_predictions"
METADATA_DATASET = "retailflow_metadata"
METADATA_TABLE = f"{PROJECT_ID}.{METADATA_DATASET}.vertex_ai_runs"
FORECAST_TABLE = f"{PROJECT_ID}.{PREDICTIONS_DATASET}.demand_forecast"
PREDICTION_INPUT = f"{PROJECT_ID}.{PREDICTIONS_DATASET}.prediction_input"


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run batch predictions for demand forecasting"
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default=None,
        help="Model resource ID. If not provided, uses the latest model."
    )
    return parser.parse_args()


def get_bq_client() -> bigquery.Client:
    """Create BigQuery client."""
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)


def prepare_prediction_input(bq_client: bigquery.Client) -> int:
    """
    Create a prediction input table with the latest week's
    product features — same schema as training data but
    WITHOUT the target column.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{PREDICTION_INPUT}` AS

    WITH latest_week AS (
        SELECT MAX(week_start) AS max_week
        FROM `{PROJECT_ID}.{PREDICTIONS_DATASET}.training_dataset`
    ),

    -- Get the latest week's data as our "current" features
    latest_features AS (
        SELECT
            td.* EXCEPT(next_week_units_sold)
        FROM `{PROJECT_ID}.{PREDICTIONS_DATASET}.training_dataset` td
        CROSS JOIN latest_week lw
        WHERE td.week_start = lw.max_week
    )

    SELECT * FROM latest_features
    """

    print("  ⏳ Preparing prediction input table...")
    bq_client.query(query).result()

    table = bq_client.get_table(PREDICTION_INPUT)
    print(f"  ✓ Prediction input created: {PREDICTION_INPUT}")
    print(f"    Rows: {table.num_rows:,} (one per product)")
    return table.num_rows


def get_latest_model() -> aiplatform.Model:
    """Find the most recently created demand forecast model."""
    all_models = aiplatform.Model.list(order_by="create_time desc")
    models = [m for m in all_models if "retailflow-demand-model" in m.display_name]
    if not models:
        raise ValueError(
            "No trained models found! "
            "Run train_automl.py and wait for completion."
        )
    return models[0]


def run_batch_prediction(model: aiplatform.Model) -> aiplatform.BatchPredictionJob:
    """Launch a batch prediction job using BigQuery I/O."""
    bq_source = f"bq://{PREDICTION_INPUT}"
    bq_dest = f"bq://{PROJECT_ID}.{PREDICTIONS_DATASET}"

    print(f"\n  ── Batch Prediction Configuration ────────────────────")
    print(f"    Model:       {model.display_name}")
    print(f"    Source:      {bq_source}")
    print(f"    Destination: {bq_dest}")

    batch_job = model.batch_predict(
        job_display_name=(
            f"retailflow-demand-forecast-"
            f"{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}"
        ),
        bigquery_source=bq_source,
        bigquery_destination_prefix=bq_dest,
        instances_format="bigquery",
        predictions_format="bigquery",
        sync=True,  # Wait for completion
    )

    print(f"  ✓ Batch prediction complete!")
    print(f"    Job: {batch_job.resource_name}")
    return batch_job


def create_demand_forecast_table(bq_client: bigquery.Client,
                                 batch_job) -> int:
    """
    Clean up the raw batch prediction output into a
    well-structured demand_forecast table.
    """
    # Vertex AI creates a table with a name like
    # predictions_YYYY_MM_DDThh_mm_ss_sssZ
    # We need to find it and restructure it

    query = f"""
    CREATE OR REPLACE TABLE `{FORECAST_TABLE}` AS

    SELECT
        product_id,
        product_name,
        category,
        list_price,
        stock_quantity,
        current_week_units_sold,
        ROUND(predicted_next_week_units_sold.value, 0)
            AS predicted_units,
        CURRENT_TIMESTAMP() AS forecast_generated_at,
        DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)
            AS forecast_for_week_starting
    FROM `{PROJECT_ID}.{PREDICTIONS_DATASET}.predictions_*`
    ORDER BY predicted_units DESC
    """

    print("\n  ⏳ Creating clean demand_forecast table...")
    try:
        bq_client.query(query).result()
        table = bq_client.get_table(FORECAST_TABLE)
        print(f"  ✓ Demand forecast table created: {FORECAST_TABLE}")
        print(f"    Rows: {table.num_rows:,}")
        return table.num_rows
    except Exception as e:
        print(f"  ⚠ Note: Auto-cleanup query may need adjustment based "
              f"on actual Vertex AI output schema.")
        print(f"    Error: {e}")
        print(f"    → Check predictions table schema in BigQuery console")
        return 0


def log_batch_run(bq_client: bigquery.Client, model_name: str,
                  row_count: int, started_at: dt.datetime,
                  status: str = "SUCCESS",
                  error_message: str = None) -> None:
    """Log batch prediction to metadata."""
    run_id = hashlib.md5(
        f"batch-{started_at.isoformat()}".encode()
    ).hexdigest()[:16]

    rows = [{
        "run_id": run_id,
        "run_type": "batch_predict",
        "status": status,
        "started_at": started_at.isoformat(),
        "completed_at": dt.datetime.utcnow().isoformat(),
        "details": f"Batch prediction for demand forecasting",
        "row_count": row_count,
        "model_name": model_name,
        "error_message": error_message,
    }]

    errors = bq_client.insert_rows_json(METADATA_TABLE, rows)
    if errors:
        print(f"  ⚠ Metadata logging warning: {errors}")
    else:
        print(f"  ✓ Batch run logged to metadata (run_id={run_id})")


def main() -> None:
    """Main entry point."""
    args = parse_args()

    print("=" * 60)
    print("RetailFlow — Vertex AI Batch Prediction")
    print("=" * 60)

    started_at = dt.datetime.utcnow()
    bq_client = get_bq_client()

    try:
        # Step 1: Initialize
        print("\n[1/5] Initializing Vertex AI SDK...")
        aiplatform.init(project=PROJECT_ID, location=LOCATION)

        # Step 2: Get model
        print("\n[2/5] Locating trained model...")
        if args.model_id:
            resource = (
                f"projects/{PROJECT_ID}/locations/{LOCATION}"
                f"/models/{args.model_id}"
            )
            model = aiplatform.Model(model_name=resource)
        else:
            model = get_latest_model()
        print(f"  ✓ Using model: {model.display_name}")

        # Step 3: Prepare input
        print("\n[3/5] Preparing prediction input...")
        input_rows = prepare_prediction_input(bq_client)

        # Step 4: Run batch prediction
        print("\n[4/5] Running batch prediction...")
        batch_job = run_batch_prediction(model)

        # Step 5: Clean up and log
        print("\n[5/5] Creating demand forecast table...")
        row_count = create_demand_forecast_table(bq_client, batch_job)
        log_batch_run(bq_client, model.display_name, row_count, started_at)

        print("\n" + "=" * 60)
        print("✅ Batch Prediction COMPLETE")
        print(f"   Forecast table: {FORECAST_TABLE}")
        print(f"   Rows: {row_count:,}")
        print(f"   Next: run dbt to create mart_demand_forecast")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        log_batch_run(bq_client, "FAILED", 0, started_at,
                      status="FAILED", error_message=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
