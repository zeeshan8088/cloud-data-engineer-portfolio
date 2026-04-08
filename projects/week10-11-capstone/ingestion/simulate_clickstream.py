"""
RetailFlow — Clickstream Event Simulator
==========================================
Cloud Function (Gen2) + standalone script.

Simulates 1000 user browsing events across a realistic e-commerce funnel:
  page_view → product_view → add_to_cart → checkout_started → purchase

Optionally publishes to Pub/Sub. Always loads batch to BigQuery Bronze.

Table: partitioned by _ingested_date, clustered by event_type + device_type
"""

import json
import logging
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

from config import (
    GCP_PROJECT,
    BQ_DATASET_BRONZE,
    GCS_BUCKET_BRONZE,
    PUBSUB_TOPIC_CLICKSTREAM,
    DEFAULT_NUM_CLICKSTREAM,
    DRY_RUN,
)

fake = Faker()
logger = logging.getLogger("retailflow.clickstream")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)

# ── Funnel event types with relative frequency weights ───────────
EVENT_TYPES = ["page_view", "product_view", "add_to_cart",
               "checkout_started", "purchase"]
EVENT_WEIGHTS = [0.40, 0.30, 0.15, 0.10, 0.05]

DEVICES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.35, 0.55, 0.10]

BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
BROWSER_WEIGHTS = [0.55, 0.20, 0.10, 0.10, 0.05]

PAGE_URLS = [
    "/", "/products", "/products/{pid}", "/cart", "/checkout",
    "/account", "/deals", "/categories/{cat}", "/search?q={q}",
]

CATEGORIES = [
    "electronics", "clothing", "home_kitchen", "books",
    "sports_fitness", "beauty_personal_care", "toys_games",
]

SEARCH_TERMS = [
    "headphones", "laptop", "shoes", "watch", "phone case",
    "book", "yoga mat", "perfume", "charger", "backpack",
]

BQ_SCHEMA = [
    ("event_id", "STRING", "REQUIRED"),
    ("session_id", "STRING", "NULLABLE"),
    ("user_id", "INTEGER", "NULLABLE"),
    ("event_type", "STRING", "NULLABLE"),
    ("page_url", "STRING", "NULLABLE"),
    ("product_id", "INTEGER", "NULLABLE"),
    ("event_timestamp", "TIMESTAMP", "NULLABLE"),
    ("device_type", "STRING", "NULLABLE"),
    ("browser", "STRING", "NULLABLE"),
    ("referrer", "STRING", "NULLABLE"),
    ("_ingested_at", "TIMESTAMP", "NULLABLE"),
    ("_source", "STRING", "NULLABLE"),
    ("_batch_id", "STRING", "NULLABLE"),
    ("_ingested_date", "DATE", "NULLABLE"),
]


def _build_page_url(event_type, product_id):
    """Build a realistic page URL based on event type."""
    if event_type == "page_view":
        return random.choice(["/", "/deals", "/categories/" + random.choice(CATEGORIES),
                              "/search?q=" + random.choice(SEARCH_TERMS)])
    if event_type == "product_view":
        return f"/products/{product_id}"
    if event_type == "add_to_cart":
        return f"/products/{product_id}"
    if event_type == "checkout_started":
        return "/checkout"
    if event_type == "purchase":
        return "/checkout/confirm"
    return "/"


def generate_clickstream(num_events=DEFAULT_NUM_CLICKSTREAM):
    """Generate synthetic clickstream events."""
    batch_id = str(uuid.uuid4())
    now = datetime.utcnow()
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S")
    ingested_date = now.strftime("%Y-%m-%d")

    # Create ~100 sessions with multiple events each
    num_sessions = max(50, num_events // 10)
    sessions = []
    for _ in range(num_sessions):
        sessions.append({
            "session_id": str(uuid.uuid4())[:12],
            "user_id": random.randint(1, 500),
            "device": random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0],
            "browser": random.choices(BROWSERS, weights=BROWSER_WEIGHTS, k=1)[0],
            "start_time": fake.date_time_between(start_date="-30d", end_date="now"),
        })

    referrers = [
        "https://www.google.com", "https://www.facebook.com",
        "direct", "https://www.instagram.com",
        "https://www.youtube.com", "email_campaign",
    ]

    events = []
    for i in range(num_events):
        session = random.choice(sessions)
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        product_id = random.randint(1, 100) if event_type != "page_view" else None

        # Spread events across the session duration (up to 30 min per session)
        offset_sec = random.randint(0, 1800)
        event_time = session["start_time"] + timedelta(seconds=offset_sec)

        events.append({
            "event_id": str(uuid.uuid4()),
            "session_id": session["session_id"],
            "user_id": session["user_id"],
            "event_type": event_type,
            "page_url": _build_page_url(event_type, product_id),
            "product_id": product_id,
            "event_timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "device_type": session["device"],
            "browser": session["browser"],
            "referrer": random.choice(referrers),
            "_ingested_at": ingested_at,
            "_source": "clickstream_simulator",
            "_batch_id": batch_id,
            "_ingested_date": ingested_date,
        })

    logger.info(f"Generated {len(events)} clickstream events "
                f"across {num_sessions} sessions | batch_id={batch_id}")
    return events, batch_id


def publish_to_pubsub(events):
    """Publish events to Pub/Sub topic."""
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT, PUBSUB_TOPIC_CLICKSTREAM)
    futures = []
    for event in events:
        data = json.dumps(event).encode("utf-8")
        futures.append(publisher.publish(topic_path, data))

    # Wait for all publishes
    for fut in futures:
        fut.result()

    logger.info(f"Published {len(events)} events to {topic_path}")


def upload_to_gcs(events, batch_id):
    """Upload clickstream NDJSON to GCS Bronze zone."""
    from google.cloud import storage as gcs

    client = gcs.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCS_BUCKET_BRONZE)
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    blob_name = f"raw_clickstream/{date_path}/{batch_id}.json"
    blob = bucket.blob(blob_name)

    ndjson = "\n".join(json.dumps(row) for row in events)
    blob.upload_from_string(ndjson, content_type="application/json")

    uri = f"gs://{GCS_BUCKET_BRONZE}/{blob_name}"
    logger.info(f"Uploaded to {uri}")
    return uri


def load_to_bigquery(events):
    """Load clickstream to BigQuery Bronze."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)
    table_id = f"{GCP_PROJECT}.{BQ_DATASET_BRONZE}.raw_clickstream"

    schema = [bigquery.SchemaField(n, t, mode=m) for n, t, m in BQ_SCHEMA]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="_ingested_date",
        ),
        clustering_fields=["event_type", "device_type"],
    )

    job = client.load_table_from_json(events, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    logger.info(f"Loaded to {table_id} | total rows: {table.num_rows}")
    return table.num_rows


def save_local(events, batch_id):
    """Save locally for dry-run mode."""
    os.makedirs("output", exist_ok=True)
    path = f"output/raw_clickstream_{batch_id[:8]}.json"
    with open(path, "w") as f:
        for row in events:
            f.write(json.dumps(row) + "\n")
    logger.info(f"Saved {len(events)} events to {path}")
    return path


def main(request=None):
    """Main pipeline."""
    logger.info("=" * 60)
    logger.info("RetailFlow | Clickstream Simulation — START")
    logger.info("=" * 60)
    try:
        events, batch_id = generate_clickstream()

        if DRY_RUN:
            fp = save_local(events, batch_id)
            result = {"status": "success", "mode": "dry_run",
                      "file": fp, "count": len(events)}
        else:
            publish_to_pubsub(events)
            gcs_uri = upload_to_gcs(events, batch_id)

            total = load_to_bigquery(events)
            result = {"status": "success", "batch_id": batch_id,
                      "events": len(events), "gcs_uri": gcs_uri,
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


def simulate_clickstream(request):
    return main(request)


if __name__ == "__main__":
    main()
