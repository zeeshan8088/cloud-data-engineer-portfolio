# src/detect_anomalies.py
# ----------------------------------------------------------
# Purpose: Read the e-commerce orders CSV and detect all
#          anomalous rows using rule-based checks.
#
# This is the "inspector" module. It finds bad data and
# describes what's wrong — it does NOT explain why.
# Explaining is Gemini's job (llm_summarizer.py).
#
# Returns a list of anomaly dicts, each containing:
#   - order_id
#   - anomaly_type
#   - description  (plain English, ready to send to Gemini)
#   - raw_row      (the original data for reference)
# ----------------------------------------------------------

import csv
import os
from datetime import datetime
from collections import Counter

# ── Config ────────────────────────────────────────────────
DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'sample_orders.csv')
NOW = datetime.now()


# ── Individual anomaly checks ─────────────────────────────
# Each function takes one row (a dict) and returns either:
#   - A description string if anomaly found
#   - None if the row is clean for this check
#
# Think of each function as one item on the inspector's
# checklist.

def check_negative_amount(row: dict) -> str | None:
    """Flag orders where order_amount is negative."""
    try:
        amount = float(row["order_amount"])
        if amount < 0:
            return (
                f"Order ID {row['order_id']} has a negative order amount "
                f"of {amount:.2f} INR with quantity {row['quantity']}. "
                f"Customer ID {row['customer_id']}."
            )
    except ValueError:
        pass
    return None


def check_zero_amount_high_qty(row: dict) -> str | None:
    """Flag orders with zero amount but suspiciously high quantity."""
    try:
        amount = float(row["order_amount"])
        qty = int(row["quantity"])
        if amount == 0.0 and qty > 100:
            return (
                f"Order ID {row['order_id']} has a quantity of {qty} "
                f"and an order amount of 0.00 INR. "
                f"Product ID {row['product_id']}."
            )
    except ValueError:
        pass
    return None


def check_future_timestamp(row: dict) -> str | None:
    """Flag orders with timestamps set in the future."""
    try:
        order_time = datetime.strptime(row["order_timestamp"], "%Y-%m-%d %H:%M:%S")
        if order_time > NOW:
            return (
                f"Order ID {row['order_id']} has an order timestamp of "
                f"{row['order_timestamp']}, which is in the future. "
                f"Customer ID {row['customer_id']}."
            )
    except ValueError:
        pass
    return None


def check_missing_customer(row: dict) -> str | None:
    """Flag orders where customer_id is null or empty."""
    if not row["customer_id"] or row["customer_id"].strip() == "":
        return (
            f"Order ID {row['order_id']} has a missing or empty customer ID. "
            f"Order amount: {row['order_amount']} INR, "
            f"Product ID: {row['product_id']}."
        )
    return None


# ── Duplicate detection (needs full dataset) ──────────────
# Unlike the other checks which work row-by-row, duplicate
# detection requires seeing ALL rows first, then flagging.
# Think of it like the inspector counting serial numbers
# at the END of the shift, not during.

def find_duplicate_order_ids(rows: list[dict]) -> list[dict]:
    """
    Scan all rows and return anomaly dicts for any
    order_id that appears more than once.
    """
    id_counts = Counter(row["order_id"] for row in rows)
    duplicates = {oid for oid, count in id_counts.items() if count > 1}

    anomalies = []
    seen = set()

    for row in rows:
        if row["order_id"] in duplicates and row["order_id"] not in seen:
            seen.add(row["order_id"])
            count = id_counts[row["order_id"]]
            anomalies.append({
                "order_id":     row["order_id"],
                "anomaly_type": "DUPLICATE_ORDER_ID",
                "description":  (
                    f"Order ID {row['order_id']} appears {count} times "
                    f"in the dataset. This suggests a pipeline retry or "
                    f"double-ingestion issue."
                ),
                "raw_row": row
            })

    return anomalies


# ── Main detection engine ─────────────────────────────────
def detect_anomalies(filepath: str = DATA_PATH) -> list[dict]:
    """
    Read the CSV and run all anomaly checks.
    Returns a list of anomaly report dicts.
    """

    # Map each check function to an anomaly type label
    row_checks = [
        ("NEGATIVE_AMOUNT",       check_negative_amount),
        ("ZERO_AMOUNT_HIGH_QTY",  check_zero_amount_high_qty),
        ("FUTURE_TIMESTAMP",      check_future_timestamp),
        ("MISSING_CUSTOMER_ID",   check_missing_customer),
    ]

    anomalies = []
    all_rows = []

    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            all_rows.append(row)

            # Run each row-level check
            for anomaly_type, check_fn in row_checks:
                description = check_fn(row)
                if description:
                    anomalies.append({
                        "order_id":     row["order_id"],
                        "anomaly_type": anomaly_type,
                        "description":  description,
                        "raw_row":      row
                    })

    # Run the dataset-level duplicate check
    duplicate_anomalies = find_duplicate_order_ids(all_rows)
    anomalies.extend(duplicate_anomalies)

    return anomalies


# ── Test block ────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("ANOMALY DETECTOR — E-commerce Orders")
    print("=" * 60)

    anomalies = detect_anomalies()

    if not anomalies:
        print("✅ No anomalies found.")
    else:
        print(f"\n🚨 Found {len(anomalies)} anomalies:\n")
        for a in anomalies:
            print(f"  [{a['anomaly_type']}] {a['order_id']}")
            print(f"  → {a['description']}")
            print()

    print(f"Total anomalies detected: {len(anomalies)}")