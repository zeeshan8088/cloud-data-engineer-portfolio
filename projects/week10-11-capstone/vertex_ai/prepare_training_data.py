"""
============================================================
RetailFlow — prepare_training_data.py
============================================================
Pulls from Gold mart tables, engineers features for demand
forecasting, and writes a training-ready table to:

    retailflow_predictions.training_dataset

Target variable: next_week_units_sold (regression)
Grain:           one row per product × week

Features come from:
  - mart_product_performance  (static product attributes)
  - stg_orders                (weekly sales aggregation)

Run:
    python prepare_training_data.py
============================================================
"""

import os
import sys
import hashlib
import datetime as dt

from google.cloud import bigquery

# ── Configuration ────────────────────────────────────────────
PROJECT_ID = "intricate-ward-459513-e1"
LOCATION = "asia-south1"
PREDICTIONS_DATASET = "retailflow_predictions"
METADATA_DATASET = "retailflow_metadata"
TRAINING_TABLE = f"{PROJECT_ID}.{PREDICTIONS_DATASET}.training_dataset"
METADATA_TABLE = f"{PROJECT_ID}.{METADATA_DATASET}.vertex_ai_runs"


def get_client() -> bigquery.Client:
    """Create an authenticated BigQuery client."""
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)


def ensure_metadata_table(client: bigquery.Client) -> None:
    """Create the vertex_ai_runs metadata table if it doesn't exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{METADATA_TABLE}` (
        run_id          STRING     NOT NULL,
        run_type        STRING     NOT NULL,
        status          STRING     NOT NULL,
        started_at      TIMESTAMP  NOT NULL,
        completed_at    TIMESTAMP,
        details         STRING,
        row_count       INT64,
        model_name      STRING,
        error_message   STRING
    )
    """
    client.query(ddl).result()
    print(f"  ✓ Metadata table ready: {METADATA_TABLE}")


def build_training_query() -> str:
    """
    Build the SQL that creates the training dataset.

    Strategy:
    ---------
    1. Aggregate stg_orders into weekly product-level sales
       (ISO week boundaries for consistency).
    2. Join static product features from mart_product_performance.
    3. Use LEAD() window function to create the target variable:
       next_week_units_sold = units sold in the FOLLOWING week.
    4. Drop the last week (where target is NULL — we can't know
       the future yet).
    """
    return f"""
    WITH
    -- ── Step 1: Weekly product sales from staging orders ─────
    weekly_product_sales AS (
        SELECT
            product_id,
            DATE_TRUNC(order_date, WEEK(MONDAY))          AS week_start,
            SUM(CASE WHEN order_status != 'cancelled'
                     THEN quantity ELSE 0 END)             AS units_sold,
            COUNT(DISTINCT CASE WHEN order_status != 'cancelled'
                               THEN order_id END)          AS order_count,
            ROUND(SUM(CASE WHEN order_status != 'cancelled'
                           THEN total_amount ELSE 0 END), 2)
                                                           AS weekly_revenue,
            COUNT(DISTINCT customer_id)                    AS unique_buyers,
            ROUND(AVG(CASE WHEN order_status != 'cancelled'
                           THEN unit_price END), 2)        AS avg_selling_price
        FROM `{PROJECT_ID}.retailflow_silver.stg_orders`
        GROUP BY product_id, week_start
    ),

    -- ── Step 2: Add the target via LEAD() ───────────────────
    with_target AS (
        SELECT
            *,
            LEAD(units_sold) OVER (
                PARTITION BY product_id
                ORDER BY week_start
            ) AS next_week_units_sold
        FROM weekly_product_sales
    ),

    -- ── Step 3: Static product features ─────────────────────
    product_features AS (
        SELECT
            product_id,
            product_name,
            category,
            list_price,
            cost_price,
            margin_pct,
            stock_quantity,
            rating,
            review_count,
            units_sold        AS lifetime_units_sold,
            revenue           AS lifetime_revenue,
            rank_in_category,
            overall_revenue_rank,
            return_rate,
            active_sale_days
        FROM `{PROJECT_ID}.retailflow_gold.mart_product_performance`
    ),

    -- ── Step 4: Platform-level weekly context ───────────────
    weekly_platform AS (
        SELECT
            DATE_TRUNC(order_date, WEEK(MONDAY))           AS week_start,
            ROUND(SUM(total_revenue), 2)                   AS platform_weekly_revenue,
            SUM(order_count)                               AS platform_weekly_orders,
            ROUND(AVG(total_revenue), 2)                   AS platform_avg_daily_revenue
        FROM `{PROJECT_ID}.retailflow_gold.mart_sales_daily`
        GROUP BY week_start
    ),

    -- ── Step 5: Join everything together ────────────────────
    training_data AS (
        SELECT
            -- Identifiers
            wt.product_id,
            pf.product_name,
            wt.week_start,

            -- Static product features
            pf.category,
            pf.list_price,
            pf.cost_price,
            pf.margin_pct,
            pf.stock_quantity,
            pf.rating,
            pf.review_count,
            pf.lifetime_units_sold,
            pf.lifetime_revenue,
            pf.rank_in_category,
            pf.overall_revenue_rank,
            pf.return_rate,
            pf.active_sale_days,

            -- Weekly product sales features (current week)
            wt.units_sold          AS current_week_units_sold,
            wt.order_count         AS current_week_orders,
            wt.weekly_revenue      AS current_week_revenue,
            wt.unique_buyers       AS current_week_buyers,
            wt.avg_selling_price,

            -- Platform context features
            COALESCE(wp.platform_weekly_revenue, 0)
                                   AS platform_weekly_revenue,
            COALESCE(wp.platform_weekly_orders, 0)
                                   AS platform_weekly_orders,
            COALESCE(wp.platform_avg_daily_revenue, 0)
                                   AS platform_avg_daily_revenue,

            -- Derived features
            ROUND(SAFE_DIVIDE(wt.weekly_revenue,
                  NULLIF(wp.platform_weekly_revenue, 0)) * 100, 4)
                                   AS pct_of_platform_revenue,

            -- ═══ TARGET VARIABLE ═══
            wt.next_week_units_sold

        FROM with_target wt
        INNER JOIN product_features pf
            ON wt.product_id = pf.product_id
        LEFT JOIN weekly_platform wp
            ON wt.week_start = wp.week_start

        -- Drop rows where we can't compute the target
        WHERE wt.next_week_units_sold IS NOT NULL
    )

    SELECT * FROM training_data
    ORDER BY product_id, week_start
    """


def create_training_table(client: bigquery.Client) -> int:
    """Execute the feature engineering query and write to BQ."""
    query = build_training_query()

    job_config = bigquery.QueryJobConfig(
        destination=TRAINING_TABLE,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print("  ⏳ Running feature engineering query...")
    job = client.query(query, job_config=job_config)
    job.result()  # Wait for completion

    # Get row count
    table = client.get_table(TRAINING_TABLE)
    row_count = table.num_rows
    print(f"  ✓ Training table created: {TRAINING_TABLE}")
    print(f"  ✓ Rows: {row_count:,}")

    return row_count


def log_run(client: bigquery.Client, run_type: str, status: str,
            started_at: dt.datetime, row_count: int = None,
            error_message: str = None) -> None:
    """Log this run to the metadata table."""
    run_id = hashlib.md5(
        f"{run_type}-{started_at.isoformat()}".encode()
    ).hexdigest()[:16]

    rows = [{
        "run_id": run_id,
        "run_type": run_type,
        "status": status,
        "started_at": started_at.isoformat(),
        "completed_at": dt.datetime.utcnow().isoformat(),
        "details": f"Feature engineering for demand forecast",
        "row_count": row_count,
        "model_name": None,
        "error_message": error_message,
    }]

    errors = client.insert_rows_json(METADATA_TABLE, rows)
    if errors:
        print(f"  ⚠ Metadata logging warning: {errors}")
    else:
        print(f"  ✓ Run logged to metadata (run_id={run_id})")


def print_sample(client: bigquery.Client) -> None:
    """Print a sample of the training data for verification."""
    query = f"""
    SELECT
        product_id,
        product_name,
        week_start,
        category,
        list_price,
        current_week_units_sold,
        next_week_units_sold
    FROM `{TRAINING_TABLE}`
    ORDER BY product_id, week_start
    LIMIT 10
    """
    print("\n  ── Sample Training Data ──────────────────────────────")
    results = client.query(query).result()
    for row in results:
        print(f"    Product {row.product_id} ({row.product_name[:20]:20s}) | "
              f"Week {row.week_start} | "
              f"Price ₹{row.list_price:>8.2f} | "
              f"This wk: {row.current_week_units_sold:>3d} → "
              f"Next wk: {row.next_week_units_sold:>3d}")


def print_stats(client: bigquery.Client) -> None:
    """Print summary statistics of the training dataset."""
    query = f"""
    SELECT
        COUNT(*)                                AS total_rows,
        COUNT(DISTINCT product_id)              AS unique_products,
        COUNT(DISTINCT week_start)              AS unique_weeks,
        COUNT(DISTINCT category)                AS unique_categories,
        ROUND(AVG(next_week_units_sold), 2)     AS avg_target,
        MIN(next_week_units_sold)               AS min_target,
        MAX(next_week_units_sold)               AS max_target,
        ROUND(STDDEV(next_week_units_sold), 2)  AS stddev_target,
        COUNTIF(next_week_units_sold IS NULL)    AS null_targets
    FROM `{TRAINING_TABLE}`
    """
    print("\n  ── Training Dataset Statistics ───────────────────────")
    results = client.query(query).result()
    for row in results:
        print(f"    Total rows:        {row.total_rows:,}")
        print(f"    Unique products:   {row.unique_products}")
        print(f"    Unique weeks:      {row.unique_weeks}")
        print(f"    Unique categories: {row.unique_categories}")
        print(f"    Target (avg):      {row.avg_target}")
        print(f"    Target (min/max):  {row.min_target} / {row.max_target}")
        print(f"    Target (stddev):   {row.stddev_target}")
        print(f"    Null targets:      {row.null_targets}")


def main() -> None:
    """Main entry point."""
    print("=" * 60)
    print("RetailFlow — Prepare Training Data for Vertex AI")
    print("=" * 60)

    started_at = dt.datetime.utcnow()
    client = get_client()

    try:
        # Step 1: Ensure metadata table exists
        print("\n[1/4] Ensuring metadata table exists...")
        ensure_metadata_table(client)

        # Step 2: Build and write the training dataset
        print("\n[2/4] Engineering features and writing training table...")
        row_count = create_training_table(client)

        # Step 3: Print sample and statistics
        print("\n[3/4] Verifying training data...")
        print_sample(client)
        print_stats(client)

        # Step 4: Log the run
        print("\n[4/4] Logging run metadata...")
        log_run(client, "prepare_training_data", "SUCCESS",
                started_at, row_count)

        print("\n" + "=" * 60)
        print("✅ Training data preparation COMPLETE")
        print(f"   Table: {TRAINING_TABLE}")
        print(f"   Rows:  {row_count:,}")
        print("   Ready for: train_automl.py")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        log_run(client, "prepare_training_data", "FAILED",
                started_at, error_message=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
