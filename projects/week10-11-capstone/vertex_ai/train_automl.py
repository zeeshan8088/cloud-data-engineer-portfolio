"""
============================================================
RetailFlow — train_automl.py
============================================================
Creates a Vertex AI TabularDataset from BigQuery, then
launches an AutoML Tabular regression training job.

Target:     next_week_units_sold (regression)
Objective:  minimize-rmse
Budget:     1,000 milli-node-hours (1 node-hour, minimum)

The training job runs asynchronously (~1–2 hours).
After it completes, use evaluate_model.py to pull metrics.

Run:
    python train_automl.py
    python train_automl.py --budget 2000   # optional override

Note: This will consume real GCP credits (~$3–6 USD at
      1,000 milli-node-hours).
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
TRAINING_TABLE = f"{PROJECT_ID}.{PREDICTIONS_DATASET}.training_dataset"
METADATA_TABLE = f"{PROJECT_ID}.{METADATA_DATASET}.vertex_ai_runs"

# AutoML configuration
TARGET_COLUMN = "next_week_units_sold"
OPTIMIZATION_TYPE = "regression"
OPTIMIZATION_OBJECTIVE = "minimize-rmse"
DEFAULT_BUDGET = 1000  # milli-node-hours (1 node-hour)

# Columns to EXCLUDE from training (identifiers, not features)
EXCLUDE_COLUMNS = [
    "product_name",    # Free text — not useful for tabular AutoML
    "week_start",      # Temporal identifier, not a feature
]


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Launch Vertex AI AutoML Tabular training job"
    )
    parser.add_argument(
        "--budget",
        type=int,
        default=DEFAULT_BUDGET,
        help=f"Training budget in milli-node-hours (default: {DEFAULT_BUDGET})"
    )
    parser.add_argument(
        "--display-name",
        type=str,
        default=None,
        help="Custom display name for the training job"
    )
    return parser.parse_args()


def get_bq_client() -> bigquery.Client:
    """Create BigQuery client."""
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)


def validate_training_data(bq_client: bigquery.Client) -> int:
    """Verify training data exists and is valid before training."""
    query = f"""
    SELECT
        COUNT(*)                             AS total_rows,
        COUNTIF({TARGET_COLUMN} IS NULL)     AS null_targets,
        COUNT(DISTINCT product_id)           AS unique_products
    FROM `{TRAINING_TABLE}`
    """
    result = list(bq_client.query(query).result())[0]

    if result.total_rows == 0:
        raise ValueError(
            f"Training table is empty! Run prepare_training_data.py first."
        )
    if result.null_targets > 0:
        raise ValueError(
            f"Found {result.null_targets} NULL target values. "
            f"Data preparation may have failed."
        )

    print(f"  ✓ Training data validated:")
    print(f"    Rows: {result.total_rows:,}")
    print(f"    Products: {result.unique_products}")
    print(f"    Null targets: {result.null_targets}")

    return result.total_rows


def create_vertex_dataset() -> aiplatform.TabularDataset:
    """Create a Vertex AI Managed Dataset from the BigQuery table."""
    bq_source = f"bq://{TRAINING_TABLE}"

    print(f"  ⏳ Creating Vertex AI Dataset from: {bq_source}")
    dataset = aiplatform.TabularDataset.create(
        display_name="retailflow-demand-forecast-training",
        bq_source=bq_source,
    )

    print(f"  ✓ Dataset created: {dataset.resource_name}")
    return dataset


def launch_training(dataset: aiplatform.TabularDataset,
                    budget: int,
                    display_name: str = None) -> tuple:
    """Launch an AutoML Tabular regression training job."""

    job_name = display_name or (
        f"retailflow-demand-forecast-"
        f"{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}"
    )

    print(f"\n  ── AutoML Training Configuration ─────────────────────")
    print(f"    Job name:           {job_name}")
    print(f"    Target column:      {TARGET_COLUMN}")
    print(f"    Prediction type:    {OPTIMIZATION_TYPE}")
    print(f"    Objective:          {OPTIMIZATION_OBJECTIVE}")
    print(f"    Budget:             {budget} milli-node-hours "
          f"({budget / 1000:.1f} node-hours)")
    print(f"    Excluded columns:   {EXCLUDE_COLUMNS}")
    print(f"    Split:              80% train / 10% val / 10% test")

    # Define the training job
    job = aiplatform.AutoMLTabularTrainingJob(
        display_name=job_name,
        optimization_prediction_type=OPTIMIZATION_TYPE,
        optimization_objective=OPTIMIZATION_OBJECTIVE,
    )

    print(f"\n  🚀 Launching training job (this is asynchronous)...")

    # Run the training — sync=False means we don't wait
    model = job.run(
        dataset=dataset,
        target_column=TARGET_COLUMN,
        budget_milli_node_hours=budget,
        model_display_name=f"retailflow-demand-model-{dt.datetime.now().strftime('%Y%m%d')}",
        training_fraction_split=0.8,
        validation_fraction_split=0.1,
        test_fraction_split=0.1,
        sync=False,              # Don't block — return immediately
    )

    return job, model


def log_training_run(bq_client: bigquery.Client, job_name: str,
                     dataset_name: str, budget: int,
                     status: str, started_at: dt.datetime,
                     error_message: str = None) -> None:
    """Log training launch to metadata table."""
    run_id = hashlib.md5(
        f"train-{started_at.isoformat()}".encode()
    ).hexdigest()[:16]

    rows = [{
        "run_id": run_id,
        "run_type": "train_automl",
        "status": status,
        "started_at": started_at.isoformat(),
        "completed_at": dt.datetime.utcnow().isoformat(),
        "details": (
            f"AutoML Tabular Regression | "
            f"Budget: {budget} milli-node-hours | "
            f"Target: {TARGET_COLUMN} | "
            f"Dataset: {dataset_name}"
        ),
        "row_count": None,
        "model_name": job_name,
        "error_message": error_message,
    }]

    errors = bq_client.insert_rows_json(METADATA_TABLE, rows)
    if errors:
        print(f"  ⚠ Metadata logging warning: {errors}")
    else:
        print(f"  ✓ Training run logged to metadata (run_id={run_id})")


def main() -> None:
    """Main entry point."""
    args = parse_args()

    print("=" * 60)
    print("RetailFlow — Vertex AI AutoML Tabular Training")
    print("=" * 60)

    started_at = dt.datetime.utcnow()
    bq_client = get_bq_client()

    try:
        # Step 1: Initialize Vertex AI SDK
        print("\n[1/5] Initializing Vertex AI SDK...")
        aiplatform.init(project=PROJECT_ID, location=LOCATION)
        print(f"  ✓ SDK initialized (project={PROJECT_ID}, "
              f"location={LOCATION})")

        # Step 2: Validate training data
        print("\n[2/5] Validating training data...")
        validate_training_data(bq_client)

        # Step 3: Create Vertex AI Managed Dataset
        print("\n[3/5] Creating Vertex AI Managed Dataset...")
        dataset = create_vertex_dataset()

        # Step 4: Launch AutoML training
        print("\n[4/5] Launching AutoML training job...")
        job, model = launch_training(
            dataset=dataset,
            budget=args.budget,
            display_name=args.display_name,
        )

        # Step 5: Log and report
        print("\n[5/5] Logging training metadata...")
        log_training_run(
            bq_client,
            job_name=job.display_name,
            dataset_name=dataset.display_name,
            budget=args.budget,
            status="LAUNCHED",
            started_at=started_at,
        )

        print("\n" + "=" * 60)
        print("🚀 AutoML Training Job LAUNCHED")
        print(f"   Job display name:  {job.display_name}")
        print(f"   Job resource:      {job.resource_name}")
        print(f"   Budget:            {args.budget} milli-node-hours")
        print(f"   Expected duration: ~1–2 hours")
        print(f"")
        print(f"   Monitor in console:")
        print(f"   https://console.cloud.google.com/vertex-ai/"
              f"training/training-pipelines?project={PROJECT_ID}")
        print(f"")
        print(f"   After training completes, run:")
        print(f"   python evaluate_model.py")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        log_training_run(
            bq_client,
            job_name="FAILED",
            dataset_name="N/A",
            budget=args.budget,
            status="FAILED",
            started_at=started_at,
            error_message=str(e),
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
