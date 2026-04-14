"""
============================================================
RetailFlow — evaluate_model.py
============================================================
After AutoML training completes (Day 9), this script pulls
evaluation metrics from the trained model and logs them to
the metadata table.

Metrics retrieved:
  - RMSE  (Root Mean Squared Error)
  - MAE   (Mean Absolute Error)
  - R²    (R-squared / Coefficient of Determination)
  - MAPE  (Mean Absolute Percentage Error)

Run (after training completes):
    python evaluate_model.py
    python evaluate_model.py --model-id <MODEL_ID>
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
METADATA_DATASET = "retailflow_metadata"
METADATA_TABLE = f"{PROJECT_ID}.{METADATA_DATASET}.vertex_ai_runs"


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Evaluate a trained Vertex AI AutoML model"
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default=None,
        help="Model resource ID. If not provided, uses the latest model."
    )
    return parser.parse_args()


def get_latest_model() -> aiplatform.Model:
    """Find the most recently created model in the project."""
    all_models = aiplatform.Model.list(order_by="create_time desc")
    models = [m for m in all_models if "retailflow-demand-model" in m.display_name]
    if not models:
        raise ValueError(
            "No trained models found! "
            "Wait for train_automl.py job to complete first."
        )
    model = models[0]
    print(f"  ✓ Found latest model: {model.display_name}")
    print(f"    Resource: {model.resource_name}")
    print(f"    Created:  {model.create_time}")
    return model


def get_model_by_id(model_id: str) -> aiplatform.Model:
    """Retrieve a specific model by its resource ID."""
    resource_name = (
        f"projects/{PROJECT_ID}/locations/{LOCATION}/models/{model_id}"
    )
    model = aiplatform.Model(model_name=resource_name)
    print(f"  ✓ Found model: {model.display_name}")
    return model


def extract_metrics(model: aiplatform.Model) -> dict:
    """Pull evaluation metrics from the trained model."""
    evaluations = list(model.list_model_evaluations())

    if not evaluations:
        raise ValueError(
            "No evaluations found. Model may still be training."
        )

    eval_obj = evaluations[0]
    metrics = eval_obj.metrics

    # Extract regression-specific metrics
    result = {
        "rmse": metrics.get("rootMeanSquaredError"),
        "mae": metrics.get("meanAbsoluteError"),
        "r_squared": metrics.get("rSquared"),
        "mape": metrics.get("meanAbsolutePercentageError"),
        "rmse_log": metrics.get("rootMeanSquaredLogError"),
    }

    return result


def print_evaluation_report(metrics: dict, model: aiplatform.Model) -> None:
    """Print a formatted evaluation report."""
    print("\n" + "=" * 60)
    print("    VERTEX AI MODEL EVALUATION REPORT")
    print("=" * 60)
    print(f"  Model:      {model.display_name}")
    print(f"  Created:    {model.create_time}")
    print(f"  Resource:   {model.resource_name}")
    print("-" * 60)
    print(f"  RMSE:       {metrics.get('rmse', 'N/A'):<12}"
          f"  (lower is better)")
    print(f"  MAE:        {metrics.get('mae', 'N/A'):<12}"
          f"  (lower is better)")
    print(f"  R²:         {metrics.get('r_squared', 'N/A'):<12}"
          f"  (closer to 1.0 is better)")
    print(f"  MAPE:       {metrics.get('mape', 'N/A'):<12}"
          f"  (lower is better)")
    print(f"  RMSLE:      {metrics.get('rmse_log', 'N/A'):<12}"
          f"  (lower is better)")
    print("=" * 60)

    # Interpret R²
    r2 = metrics.get("r_squared")
    if r2 is not None:
        if r2 >= 0.8:
            print("  📊 Interpretation: EXCELLENT model fit")
        elif r2 >= 0.5:
            print("  📊 Interpretation: GOOD model fit")
        elif r2 >= 0.2:
            print("  📊 Interpretation: MODERATE model fit")
        else:
            print("  📊 Interpretation: WEAK model fit "
                  "(expected for small synthetic data)")
    print()


def log_evaluation(bq_client: bigquery.Client, model: aiplatform.Model,
                   metrics: dict, started_at: dt.datetime) -> None:
    """Log evaluation results to metadata table."""
    run_id = hashlib.md5(
        f"evaluate-{started_at.isoformat()}".encode()
    ).hexdigest()[:16]

    details = (
        f"RMSE={metrics.get('rmse')}, "
        f"MAE={metrics.get('mae')}, "
        f"R²={metrics.get('r_squared')}, "
        f"MAPE={metrics.get('mape')}"
    )

    rows = [{
        "run_id": run_id,
        "run_type": "evaluate_model",
        "status": "SUCCESS",
        "started_at": started_at.isoformat(),
        "completed_at": dt.datetime.utcnow().isoformat(),
        "details": details,
        "row_count": None,
        "model_name": model.display_name,
        "error_message": None,
    }]

    errors = bq_client.insert_rows_json(METADATA_TABLE, rows)
    if errors:
        print(f"  ⚠ Metadata logging warning: {errors}")
    else:
        print(f"  ✓ Evaluation logged to metadata (run_id={run_id})")


def main() -> None:
    """Main entry point."""
    args = parse_args()

    print("=" * 60)
    print("RetailFlow — Vertex AI Model Evaluation")
    print("=" * 60)

    started_at = dt.datetime.utcnow()

    try:
        # Step 1: Initialize
        print("\n[1/4] Initializing Vertex AI SDK...")
        aiplatform.init(project=PROJECT_ID, location=LOCATION)
        bq_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

        # Step 2: Find the model
        print("\n[2/4] Locating trained model...")
        if args.model_id:
            model = get_model_by_id(args.model_id)
        else:
            model = get_latest_model()

        # Step 3: Extract metrics
        print("\n[3/4] Extracting evaluation metrics...")
        metrics = extract_metrics(model)
        print_evaluation_report(metrics, model)

        # Step 4: Log to metadata
        print("[4/4] Logging evaluation results...")
        log_evaluation(bq_client, model, metrics, started_at)

        print("✅ Evaluation complete!\n")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        print("\n💡 If training hasn't finished yet, check status at:")
        print(f"   https://console.cloud.google.com/vertex-ai/"
              f"training/training-pipelines?project={PROJECT_ID}")
        sys.exit(1)


if __name__ == "__main__":
    main()
