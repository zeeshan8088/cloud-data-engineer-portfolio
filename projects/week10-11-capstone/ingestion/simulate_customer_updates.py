"""
RetailFlow — Day 4 SCD Type 2 Test Script
==========================================
Simulates customer record UPDATES to prove that dbt snapshot
correctly creates new SCD2 version rows with _valid_to set on
the old version and _is_current = true on the new version.

WHAT THIS SCRIPT DOES:
  1. Reads 5 existing customers from BigQuery Bronze
  2. Modifies their email / city / is_active (tracked columns)
  3. Appends the updated rows back to raw_customers with a new batch_id
  4. When you then run `dbt snapshot`, dbt detects the changes
     and creates version 2 rows for these 5 customers

HOW TO USE:
  python simulate_customer_updates.py
  cd dbt && dbt snapshot
  cd dbt && dbt run --select dim_customers_scd2
  python simulate_customer_updates.py --verify

EXPECTED RESULT AFTER `--verify` FLAG:
  • 5 customers now have 2 rows in dim_customers_scd2
  • Old rows: _is_current = false, _valid_to = <change timestamp>
  • New rows: _is_current = true,  _valid_to = NULL
"""

import argparse
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery

# ── Config ────────────────────────────────────────────────────────────────────
GCP_PROJECT   = "intricate-ward-459513-e1"
BRONZE_TABLE  = f"{GCP_PROJECT}.retailflow_bronze.raw_customers"
SCD2_TABLE    = f"{GCP_PROJECT}.retailflow_silver.dim_customers_scd2"
LOCATION      = "asia-south1"
NUM_CUSTOMERS = 5  # number of customers to modify

# ── Modifications applied to the chosen customers ─────────────────────────────
# Each customer gets a different change to simulate real-world scenarios.
CHANGES = [
    {"field": "email",     "label": "Email address changed (moved provider)"},
    {"field": "city",      "label": "City changed (customer relocated)"},
    {"field": "is_active", "label": "Account deactivated"},
    {"field": "phone",     "label": "Phone number updated"},
    {"field": "city",      "label": "City changed again (second relocation)"},
]


def inject_updates(client: bigquery.Client) -> None:
    """Fetch 5 customers from Bronze and append modified versions."""

    now         = datetime.now(timezone.utc)
    new_batch   = str(uuid.uuid4())
    ingested_ts = now.strftime("%Y-%m-%d %H:%M:%S UTC")
    ingested_dt = now.strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print("  RetailFlow — SCD Type 2 Change Injection")
    print(f"{'='*60}")
    print(f"  New batch_id : {new_batch}")
    print(f"  Timestamp    : {ingested_ts}")
    print(f"  Customers    : {NUM_CUSTOMERS}")
    print(f"{'='*60}\n")

    # ── Step 1: Fetch 5 customers ─────────────────────────────────────────────
    fetch_sql = f"""
        SELECT
            customer_id, first_name, last_name, email, phone,
            address, city, state, country, age, gender,
            registration_date, is_active
        FROM `{BRONZE_TABLE}`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY customer_id ORDER BY _ingested_at DESC
        ) = 1
        ORDER BY customer_id
        LIMIT {NUM_CUSTOMERS}
    """
    print("► Fetching customers from Bronze…")
    rows = list(client.query(fetch_sql, location=LOCATION).result())

    if not rows:
        print("✗  No customers found in Bronze. Run Day 1 ingestion first.")
        return

    print(f"  Found {len(rows)} customers: {[r['customer_id'] for r in rows]}\n")

    # ── Step 2: Build modified rows ───────────────────────────────────────────
    updated_rows = []

    for i, row in enumerate(rows):
        change     = CHANGES[i]
        customer   = dict(row)
        old_val    = customer.get(change["field"])

        # Apply the modification
        if change["field"] == "email":
            customer["email"] = f"updated_{customer['customer_id']}@newprovider.com"
        elif change["field"] == "city":
            new_cities = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai"]
            customer["city"] = new_cities[i % len(new_cities)]
        elif change["field"] == "is_active":
            customer["is_active"] = not bool(customer["is_active"])
        elif change["field"] == "phone":
            customer["phone"] = f"+91-9000-{customer['customer_id']:04d}"

        new_val = customer.get(change["field"])

        print(f"  Customer {customer['customer_id']:>4} | {change['label']}")
        print(f"             {change['field']}: '{old_val}' → '{new_val}'")

        # Add audit columns with new batch metadata
        customer["_ingested_at"]   = ingested_ts
        customer["_source"]        = "simulate_customer_updates"
        customer["_batch_id"]      = new_batch
        customer["_ingested_date"] = ingested_dt

        # Convert types BigQuery expects
        if customer.get("registration_date"):
            customer["registration_date"] = str(customer["registration_date"])

        updated_rows.append(customer)

    print(f"\n► Appending {len(updated_rows)} updated rows to Bronze…")

    # ── Step 3: Insert modified rows back into Bronze ─────────────────────────
    errors = client.insert_rows_json(BRONZE_TABLE, updated_rows)

    if errors:
        print(f"\n✗  BigQuery insert errors:\n{errors}")
        raise RuntimeError("Failed to inject updated customer rows.")

    print("✓  Updated rows successfully written to raw_customers")
    print("\n" + "="*60)
    print("  NEXT STEPS — run these commands in order:")
    print("="*60)
    print(f"  cd projects\\week10-11-capstone\\dbt")
    print(f"  dbt snapshot")
    print(f"  dbt run --select dim_customers_scd2")
    print(f"  python ..\\ingestion\\simulate_customer_updates.py --verify")
    print("="*60 + "\n")


def verify_scd2(client: bigquery.Client) -> None:
    """Query dim_customers_scd2 and show version history for the 5 customers."""

    print(f"\n{'='*60}")
    print("  RetailFlow — SCD Type 2 Verification")
    print(f"{'='*60}\n")

    verify_sql = f"""
        SELECT
            customer_id,
            full_name,
            email,
            city,
            is_active,
            _version_num,
            _is_current,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', _valid_from) AS valid_from,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', _valid_to)   AS valid_to
        FROM `{SCD2_TABLE}`
        WHERE customer_id IN (
            SELECT DISTINCT customer_id
            FROM `{SCD2_TABLE}`
            WHERE _version_num > 1
        )
        ORDER BY customer_id, _version_num
    """

    print("► Querying dim_customers_scd2 for multi-version customers…\n")

    try:
        rows = list(client.query(verify_sql, location=LOCATION).result())
    except Exception as e:
        print(f"✗  Query failed: {e}")
        print("   Have you run `dbt snapshot` and `dbt run --select dim_customers_scd2` yet?")
        return

    if not rows:
        print("✗  No version history found yet.")
        print("   Run `dbt snapshot` then `dbt run --select dim_customers_scd2` first.")
        return

    # ── Pretty-print results ──────────────────────────────────────────────────
    current_customer = None
    passed = 0
    v1_with_valid_to = 0
    v2_current       = 0

    for row in rows:
        if row["customer_id"] != current_customer:
            current_customer = row["customer_id"]
            print(f"  Customer {current_customer} — {row['full_name']}")

        icon = "✓" if row["_is_current"] else "○"
        status = "CURRENT" if row["_is_current"] else "HISTORY"
        print(
            f"    {icon} V{row['_version_num']} [{status}]"
            f"  email={row['email']!r}"
            f"  city={row['city']!r}"
            f"  active={row['is_active']}"
        )
        print(
            f"         valid_from = {row['valid_from']}"
            f"  |  valid_to = {row['valid_to'] or 'NULL (current)'}"
        )

        if row["_version_num"] == 1 and not row["_is_current"]:
            v1_with_valid_to += 1
        if row["_version_num"] >= 2 and row["_is_current"]:
            v2_current += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    multi_version_customers = len(set(r["customer_id"] for r in rows))
    print(f"\n{'='*60}")
    print("  VERIFICATION SUMMARY")
    print(f"{'='*60}")
    print(f"  Customers with history : {multi_version_customers}")
    print(f"  V1 rows closed (_valid_to set) : {v1_with_valid_to}")
    print(f"  V2 rows marked current         : {v2_current}")

    if v1_with_valid_to >= 1 and v2_current >= 1:
        print("\n  ✓  SCD TYPE 2 IS WORKING CORRECTLY!")
        print("     Old versions have _valid_to set.")
        print("     New versions have _is_current = true.")
    else:
        print("\n  ✗  Something looks wrong. Check dbt snapshot ran successfully.")
    print(f"{'='*60}\n")


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Day 4 SCD Type 2 test: inject customer updates or verify results."
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Query dim_customers_scd2 and show version history (run AFTER dbt snapshot).",
    )
    args = parser.parse_args()

    client = bigquery.Client(project=GCP_PROJECT)

    if args.verify:
        verify_scd2(client)
    else:
        inject_updates(client)


if __name__ == "__main__":
    main()
