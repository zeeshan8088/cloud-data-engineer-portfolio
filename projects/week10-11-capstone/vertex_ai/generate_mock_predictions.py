"""
============================================================
RetailFlow - generate_mock_predictions.py
============================================================
Creates a mock demand_forecast table in BigQuery by querying
the Gold layer mart_product_performance and generating
synthetic predictions.

This is used as a fallback when Vertex AI AutoML training
cannot complete due to GCP quota limits. The mock predictions
simulate realistic demand forecasting output.

Writes to: retailflow_predictions.demand_forecast

Run:
    python generate_mock_predictions.py
============================================================
"""

import os
import sys
import random
import datetime as dt

from google.cloud import bigquery

# ── Configuration ────────────────────────────────────────────
PROJECT_ID = "intricate-ward-459513-e1"
LOCATION = "asia-south1"
GOLD_DATASET = "retailflow_gold"
PREDICTIONS_DATASET = "retailflow_predictions"
SOURCE_TABLE = f"{PROJECT_ID}.{GOLD_DATASET}.mart_product_performance"
DEST_TABLE = f"{PROJECT_ID}.{PREDICTIONS_DATASET}.demand_forecast"


def get_client() -> bigquery.Client:
    """Create an authenticated BigQuery client."""
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)


def ensure_predictions_dataset(client: bigquery.Client) -> None:
    """Create the retailflow_predictions dataset if it doesn't exist."""
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, PREDICTIONS_DATASET)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = LOCATION
    try:
        client.get_dataset(dataset_ref)
        print(f"  [OK] Dataset already exists: {PREDICTIONS_DATASET}")
    except Exception:
        client.create_dataset(dataset, exists_ok=True)
        print(f"  [OK] Created dataset: {PREDICTIONS_DATASET}")


def fetch_product_data(client: bigquery.Client) -> list:
    """Query mart_product_performance for current product data."""
    query = f"""
    SELECT
        product_id,
        product_name,
        category,
        units_sold
    FROM `{SOURCE_TABLE}`
    WHERE units_sold > 0
    ORDER BY product_id
    """
    print("  ... Querying mart_product_performance...")
    results = client.query(query).result()
    rows = list(results)
    print(f"  [OK] Fetched {len(rows)} products with sales data")
    return rows


def generate_predictions(products: list) -> list:
    """
    Generate mock predictions for each product.

    - predicted_next_week_units_sold: current_units_sold * random(0.8, 1.3), rounded
    - prediction_confidence: random float between 0.72 and 0.95
    - prediction_date: today's date
    """
    random.seed(42)  # Reproducible results
    prediction_date = dt.date.today().isoformat()
    predictions = []

    for product in products:
        current_units = product.units_sold
        factor = random.uniform(0.8, 1.3)
        predicted_units = round(current_units * factor)
        confidence = round(random.uniform(0.72, 0.95), 4)

        predictions.append({
            "product_id": product.product_id,
            "product_name": product.product_name,
            "category": product.category,
            "current_units_sold": current_units,
            "predicted_next_week_units_sold": predicted_units,
            "prediction_confidence": confidence,
            "prediction_date": prediction_date,
        })

    print(f"  [OK] Generated {len(predictions)} mock predictions")
    return predictions


def write_to_bigquery(client: bigquery.Client, predictions: list) -> int:
    """Write predictions to BigQuery using WRITE_TRUNCATE."""
    schema = [
        bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("current_units_sold", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("predicted_next_week_units_sold", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("prediction_confidence", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("prediction_date", "DATE", mode="REQUIRED"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    print("  ... Writing predictions to BigQuery...")
    job = client.load_table_from_json(
        predictions,
        DEST_TABLE,
        job_config=job_config,
    )
    job.result()  # Wait for completion

    # Verify row count
    table = client.get_table(DEST_TABLE)
    row_count = table.num_rows
    print(f"  [OK] Table written: {DEST_TABLE}")
    print(f"  [OK] Rows: {row_count}")
    return row_count


def print_sample(client: bigquery.Client) -> None:
    """Print a sample of the predictions for verification."""
    query = f"""
    SELECT *
    FROM `{DEST_TABLE}`
    ORDER BY predicted_next_week_units_sold DESC
    LIMIT 10
    """
    print("\n  -- Sample Predictions (Top 10 by predicted units) --")
    results = client.query(query).result()
    for row in results:
        print(
            f"    Product {row.product_id:>3d} | "
            f"{row.product_name[:25]:25s} | "
            f"{row.category:15s} | "
            f"Current: {row.current_units_sold:>4d} -> "
            f"Predicted: {row.predicted_next_week_units_sold:>4d} | "
            f"Confidence: {row.prediction_confidence:.2%}"
        )


def main() -> None:
    """Main entry point."""
    print("=" * 60)
    print("RetailFlow - Generate Mock Demand Forecast Predictions")
    print("=" * 60)

    client = get_client()

    try:
        # Step 1: Ensure predictions dataset exists
        print("\n[1/4] Ensuring predictions dataset exists...")
        ensure_predictions_dataset(client)

        # Step 2: Fetch product data from Gold mart
        print("\n[2/4] Fetching product data from Gold layer...")
        products = fetch_product_data(client)

        if not products:
            print("  [FAIL] No products found in mart_product_performance!")
            sys.exit(1)

        # Step 3: Generate mock predictions
        print("\n[3/4] Generating mock predictions...")
        predictions = generate_predictions(products)

        # Step 4: Write to BigQuery
        print("\n[4/4] Writing to BigQuery...")
        row_count = write_to_bigquery(client, predictions)

        # Print sample
        print_sample(client)

        print("\n" + "=" * 60)
        print("[SUCCESS] Mock demand forecast generation COMPLETE")
        print(f"   Table: {DEST_TABLE}")
        print(f"   Rows:  {row_count}")
        print("   Ready for: Streamlit Dashboard")
        print("=" * 60)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
