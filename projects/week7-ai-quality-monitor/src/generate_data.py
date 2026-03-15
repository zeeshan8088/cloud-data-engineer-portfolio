# src/generate_data.py
# ----------------------------------------------------------
# Purpose: Generate a realistic e-commerce orders dataset
#          with deliberately injected anomalies for testing
#          our AI quality monitor pipeline.
#
# Output:  data/sample_orders.csv
#
# Think of this file as the "data source simulator" —
# it mimics what a real orders table might look like after
# a bad day of data ingestion.
# ----------------------------------------------------------

import csv
import random
from datetime import datetime, timedelta
import os

# ── Seed for reproducibility ──────────────────────────────
# Setting a seed means every time you run this script,
# you get the same "random" data. Like shuffling a deck
# the same way every time — useful for testing.
random.seed(42)

# ── Config ────────────────────────────────────────────────
NUM_CLEAN_ORDERS = 185
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'sample_orders.csv')

# ── Helper: generate one clean order ─────────────────────
def make_clean_order(order_num: int) -> dict:
    """Generate a single valid e-commerce order."""
    order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
    return {
        "order_id":        f"ORD-{order_num:04d}",
        "customer_id":     f"C-{random.randint(1000, 9999)}",
        "product_id":      f"PROD-{random.randint(1, 50):03d}",
        "quantity":        random.randint(1, 10),
        "order_amount":    round(random.uniform(50.0, 5000.0), 2),
        "order_timestamp": order_date.strftime("%Y-%m-%d %H:%M:%S"),
        "status":          random.choice(["completed", "shipped", "processing"])
    }

# ── Anomaly injectors ─────────────────────────────────────
# Each function takes a base order and corrupts it in one
# specific way. Think of these as "fault injection" —
# deliberately breaking things to test your detector.

def inject_negative_amount(order: dict) -> dict:
    """Negative order amount — looks like a bad refund record."""
    order["order_amount"] = round(random.uniform(-1000.0, -10.0), 2)
    return order

def inject_zero_amount_high_qty(order: dict) -> dict:
    """Zero amount + huge quantity — classic ghost/test order."""
    order["order_amount"] = 0.00
    order["quantity"] = random.choice([999, 9999, 1000])
    return order

def inject_future_timestamp(order: dict) -> dict:
    """Timestamp far in the future — date parsing bug."""
    future_date = datetime(2085, 1, 1) + timedelta(days=random.randint(0, 365))
    order["order_timestamp"] = future_date.strftime("%Y-%m-%d %H:%M:%S")
    return order

def inject_missing_customer(order: dict) -> dict:
    """Null customer ID — attribution failure."""
    order["customer_id"] = ""
    return order

# ── Build the full dataset ────────────────────────────────
def generate_dataset() -> list[dict]:
    """
    Creates 200 orders: 185 clean + 15 anomalous.
    Anomalies are shuffled in randomly so the detector
    can't just check the last 15 rows.
    """
    orders = []

    # Generate clean orders (order IDs 1–185)
    for i in range(1, NUM_CLEAN_ORDERS + 1):
        orders.append(make_clean_order(i))

    # Generate anomalous orders (order IDs 186–200)
    # We use a fixed list so each anomaly type appears
    # a realistic number of times
    anomaly_injectors = [
        inject_negative_amount,       # 4 times
        inject_negative_amount,
        inject_negative_amount,
        inject_negative_amount,
        inject_zero_amount_high_qty,  # 3 times
        inject_zero_amount_high_qty,
        inject_zero_amount_high_qty,
        inject_future_timestamp,      # 3 times
        inject_future_timestamp,
        inject_future_timestamp,
        inject_missing_customer,      # 3 times
        inject_missing_customer,
        inject_missing_customer,
    ]

    for i, injector in enumerate(anomaly_injectors, start=NUM_CLEAN_ORDERS + 1):
        base = make_clean_order(i)
        anomalous = injector(base)
        orders.append(anomalous)

    # Inject 2 duplicate order IDs (reuse existing IDs)
    # This simulates a pipeline retrying and double-inserting
    dup1 = make_clean_order(999)
    dup1["order_id"] = "ORD-0042"   # duplicate of a likely existing ID
    dup2 = make_clean_order(998)
    dup2["order_id"] = "ORD-0042"   # same ID — deliberate duplicate
    orders.append(dup1)
    orders.append(dup2)

    # Shuffle everything so anomalies are mixed in randomly
    random.shuffle(orders)

    return orders

# ── Write to CSV ──────────────────────────────────────────
def save_to_csv(orders: list[dict], path: str):
    """Write the orders list to a CSV file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)

    fieldnames = [
        "order_id", "customer_id", "product_id",
        "quantity", "order_amount", "order_timestamp", "status"
    ]

    with open(path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(orders)

    print(f"✅ Dataset saved to: {path}")
    print(f"   Total orders: {len(orders)}")
    print(f"   Clean orders: {NUM_CLEAN_ORDERS}")
    print(f"   Anomalous orders: {len(orders) - NUM_CLEAN_ORDERS} (hidden in random positions)")

# ── Run ───────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating e-commerce orders dataset...")
    orders = generate_dataset()
    save_to_csv(orders, OUTPUT_PATH)