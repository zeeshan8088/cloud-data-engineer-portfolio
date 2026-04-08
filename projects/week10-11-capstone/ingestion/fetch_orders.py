"""
RetailFlow — Order Ingestion from FakeStore API
=================================================
Cloud Function (Gen2) + standalone script.

Fetches product catalog from FakeStore API (20 products), generates 220
realistic synthetic orders, uploads NDJSON to GCS Bronze zone, and loads
to BigQuery retailflow_bronze.raw_orders.

Table: partitioned by _ingested_date, clustered by status + product_category
"""

import json
import logging
import os
import random
import uuid
from datetime import datetime

import requests
from faker import Faker

from config import (
    GCP_PROJECT,
    BQ_DATASET_BRONZE,
    GCS_BUCKET_BRONZE,
    FAKESTORE_API_URL,
    DEFAULT_NUM_ORDERS,
    DRY_RUN,
)

fake = Faker()
logger = logging.getLogger("retailflow.orders")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)

ORDER_STATUSES = ["pending", "shipped", "delivered", "cancelled"]
STATUS_WEIGHTS = [0.20, 0.25, 0.45, 0.10]
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "net_banking", "cod", "wallet"]

# ── BigQuery schema ──────────────────────────────────────────────
BQ_SCHEMA = [
    ("order_id", "INTEGER", "REQUIRED"),
    ("user_id", "INTEGER", "NULLABLE"),
    ("product_id", "INTEGER", "NULLABLE"),
    ("product_title", "STRING", "NULLABLE"),
    ("product_category", "STRING", "NULLABLE"),
    ("product_price", "FLOAT64", "NULLABLE"),
    ("quantity", "INTEGER", "NULLABLE"),
    ("total_amount", "FLOAT64", "NULLABLE"),
    ("status", "STRING", "NULLABLE"),
    ("order_date", "TIMESTAMP", "NULLABLE"),
    ("shipping_address", "STRING", "NULLABLE"),
    ("payment_method", "STRING", "NULLABLE"),
    ("_ingested_at", "TIMESTAMP", "NULLABLE"),
    ("_source", "STRING", "NULLABLE"),
    ("_batch_id", "STRING", "NULLABLE"),
    ("_ingested_date", "DATE", "NULLABLE"),
]


def fetch_fakestore_products():
    """Fetch product catalog from FakeStore API."""
    url = f"{FAKESTORE_API_URL}/products"
    logger.info(f"Fetching products from {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    products = resp.json()
    logger.info(f"Fetched {len(products)} products")
    return products


def generate_orders(products, num_orders=DEFAULT_NUM_ORDERS):
    """Generate synthetic orders using real FakeStore product data."""
    batch_id = str(uuid.uuid4())
    now = datetime.utcnow()
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S")
    ingested_date = now.strftime("%Y-%m-%d")

    orders = []
    for i in range(num_orders):
        product = random.choice(products)
        qty = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3], k=1)[0]
        order_date = fake.date_time_between(start_date="-90d", end_date="now")

        orders.append({
            "order_id": 10001 + i,
            "user_id": random.randint(1, 500),
            "product_id": product["id"],
            "product_title": product["title"],
            "product_category": product.get("category", "uncategorized"),
            "product_price": round(float(product["price"]), 2),
            "quantity": qty,
            "total_amount": round(float(product["price"]) * qty, 2),
            "status": random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
            "order_date": order_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "shipping_address": fake.address().replace("\n", ", "),
            "payment_method": random.choice(PAYMENT_METHODS),
            "_ingested_at": ingested_at,
            "_source": "fakestore_api",
            "_batch_id": batch_id,
            "_ingested_date": ingested_date,
        })

    logger.info(f"Generated {len(orders)} orders | batch_id={batch_id}")
    return orders, batch_id


def upload_to_gcs(orders, batch_id):
    """Upload raw orders as NDJSON to GCS Bronze zone."""
    from google.cloud import storage as gcs

    client = gcs.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCS_BUCKET_BRONZE)
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    blob_name = f"raw_orders/{date_path}/{batch_id}.json"
    blob = bucket.blob(blob_name)

    ndjson = "\n".join(json.dumps(row) for row in orders)
    blob.upload_from_string(ndjson, content_type="application/json")

    uri = f"gs://{GCS_BUCKET_BRONZE}/{blob_name}"
    logger.info(f"Uploaded to {uri}")
    return uri


def load_to_bigquery(orders):
    """Load orders to BigQuery Bronze with partitioning & clustering."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)
    table_id = f"{GCP_PROJECT}.{BQ_DATASET_BRONZE}.raw_orders"

    schema = [bigquery.SchemaField(n, t, mode=m) for n, t, m in BQ_SCHEMA]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="_ingested_date",
        ),
        clustering_fields=["status", "product_category"],
    )

    job = client.load_table_from_json(orders, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    logger.info(f"Loaded to {table_id} | total rows: {table.num_rows}")
    return table.num_rows


def save_local(orders, batch_id):
    """Save orders locally for dry-run mode."""
    os.makedirs("output", exist_ok=True)
    path = f"output/raw_orders_{batch_id[:8]}.json"
    with open(path, "w") as f:
        for row in orders:
            f.write(json.dumps(row) + "\n")
    logger.info(f"Saved {len(orders)} orders to {path}")
    return path


def main(request=None):
    """Main entry point — standalone script or Cloud Function."""
    logger.info("=" * 60)
    logger.info("RetailFlow | Order Ingestion — START")
    logger.info("=" * 60)
    try:
        products = fetch_fakestore_products()
        orders, batch_id = generate_orders(products)

        if DRY_RUN:
            filepath = save_local(orders, batch_id)
            result = {"status": "success", "mode": "dry_run",
                      "file": filepath, "count": len(orders)}
        else:
            gcs_uri = upload_to_gcs(orders, batch_id)

            total = load_to_bigquery(orders)
            result = {"status": "success", "batch_id": batch_id,
                      "orders": len(orders), "gcs_uri": gcs_uri,
                      "bq_total_rows": total}

        logger.info(f"Complete: {json.dumps(result, indent=2)}")
        if request:
            return json.dumps(result), 200
        return result
    except Exception as e:
        logger.error(f"FAILED: {e}", exc_info=True)
        if request:
            return json.dumps({"status": "error", "message": str(e)}), 500
        raise


# Cloud Function entry point
def ingest_orders(request):
    return main(request)


if __name__ == "__main__":
    main()
