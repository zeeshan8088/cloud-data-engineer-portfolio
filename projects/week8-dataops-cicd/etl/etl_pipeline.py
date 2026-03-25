"""
Week 8 - DataOps & CI/CD
ETL Pipeline Component - Order Processing
Reads raw order data, transforms it, and outputs summary stats.
Designed to be containerised and deployed via CI/CD.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Logging setup — structured logs work better in Cloud environments
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sample data — simulates what you'd get from an API or Pub/Sub message
# ---------------------------------------------------------------------------
RAW_ORDERS: list[dict[str, Any]] = [
    {"order_id": "ORD-001", "customer_id": "C100", "items": 3, "unit_price": 450.00, "status": "completed"},
    {"order_id": "ORD-002", "customer_id": "C101", "items": 1, "unit_price": 1200.00, "status": "completed"},
    {"order_id": "ORD-003", "customer_id": "C102", "items": 5, "unit_price": 89.99,  "status": "pending"},
    {"order_id": "ORD-004", "customer_id": "C100", "items": 2, "unit_price": 3200.00, "status": "completed"},
    {"order_id": "ORD-005", "customer_id": "C103", "items": 1, "unit_price": 75.00,  "status": "cancelled"},
    {"order_id": "ORD-006", "customer_id": "C104", "items": 4, "unit_price": 560.00, "status": "completed"},
    {"order_id": "ORD-007", "customer_id": "C105", "items": 2, "unit_price": 999.00, "status": "pending"},
    {"order_id": "ORD-008", "customer_id": "C101", "items": 6, "unit_price": 120.00, "status": "completed"},
]


# ---------------------------------------------------------------------------
# Transform layer
# ---------------------------------------------------------------------------
HIGH_VALUE_THRESHOLD = float(os.getenv("HIGH_VALUE_THRESHOLD", "1000.0"))
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")


def transform_order(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Applies business logic to a single raw order record.
    - Calculates total order value
    - Flags high-value orders
    - Adds processing timestamp
    - Normalises status to uppercase
    """
    total_value = raw["items"] * raw["unit_price"]

    return {
        "order_id":        raw["order_id"],
        "customer_id":     raw["customer_id"],
        "items":           raw["items"],
        "unit_price":      raw["unit_price"],
        "total_value":     round(total_value, 2),
        "is_high_value":   total_value >= HIGH_VALUE_THRESHOLD,
        "status":          raw["status"].upper(),
        "processed_at":    datetime.now(timezone.utc).isoformat(),
        "environment":     ENVIRONMENT,
    }


def run_pipeline(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Runs the full ETL:
      Extract  → raw orders list (already in memory here; would be API/GCS in prod)
      Transform → apply business rules to each record
      Load      → in prod this writes to BigQuery; here we log + return results
    """
    logger.info("Pipeline starting | environment=%s | record_count=%d", ENVIRONMENT, len(orders))

    transformed = []
    errors = []

    for raw in orders:
        try:
            record = transform_order(raw)
            transformed.append(record)
            logger.info(
                "Transformed | order_id=%s | total_value=%.2f | high_value=%s",
                record["order_id"],
                record["total_value"],
                record["is_high_value"],
            )
        except (KeyError, TypeError) as exc:
            logger.error("Transform failed | order_id=%s | error=%s", raw.get("order_id", "UNKNOWN"), exc)
            errors.append(raw)

    # Summary stats
    completed = [r for r in transformed if r["status"] == "COMPLETED"]
    high_value = [r for r in transformed if r["is_high_value"]]
    total_revenue = sum(r["total_value"] for r in completed)

    logger.info("--- Pipeline Summary ---")
    logger.info("Total records processed : %d", len(transformed))
    logger.info("Completed orders        : %d", len(completed))
    logger.info("High-value orders       : %d", len(high_value))
    logger.info("Total revenue (completed): ₹%.2f", total_revenue)
    logger.info("Errors                  : %d", len(errors))

    if errors:
        logger.warning("Failed records: %s", json.dumps(errors, indent=2))
        sys.exit(1)

    logger.info("Pipeline completed successfully.")
    return transformed


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_pipeline(RAW_ORDERS)