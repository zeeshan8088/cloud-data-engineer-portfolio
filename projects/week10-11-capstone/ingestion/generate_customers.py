"""
RetailFlow — Synthetic Customer Data Generator
================================================
Cloud Function (Gen2) + standalone script.

Generates 500 realistic customer profiles (70 % Indian, 30 % international)
using the Faker library. Uploads CSV to GCS and loads to BigQuery Bronze.

Table: partitioned by _ingested_date, clustered by city + gender
"""

import csv
import io
import json
import logging
import os
import random
import uuid
from datetime import datetime

from faker import Faker

from config import (
    GCP_PROJECT,
    BQ_DATASET_BRONZE,
    GCS_BUCKET_BRONZE,
    DEFAULT_NUM_CUSTOMERS,
    DRY_RUN,
)

fake = Faker()
fake_in = Faker("en_IN")
logger = logging.getLogger("retailflow.customers")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)

INDIAN_CITIES = [
    "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai",
    "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow",
    "Chandigarh", "Kochi", "Indore", "Nagpur", "Coimbatore",
]
INDIAN_STATES = [
    "Maharashtra", "Karnataka", "Telangana", "Tamil Nadu", "Delhi",
    "West Bengal", "Gujarat", "Rajasthan", "Uttar Pradesh", "Kerala",
]
GENDERS = ["Male", "Female", "Non-Binary"]
GENDER_WEIGHTS = [0.48, 0.48, 0.04]
EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]

BQ_SCHEMA = [
    ("customer_id", "INTEGER", "REQUIRED"),
    ("first_name", "STRING", "NULLABLE"),
    ("last_name", "STRING", "NULLABLE"),
    ("email", "STRING", "NULLABLE"),
    ("phone", "STRING", "NULLABLE"),
    ("address", "STRING", "NULLABLE"),
    ("city", "STRING", "NULLABLE"),
    ("state", "STRING", "NULLABLE"),
    ("country", "STRING", "NULLABLE"),
    ("age", "INTEGER", "NULLABLE"),
    ("gender", "STRING", "NULLABLE"),
    ("registration_date", "DATE", "NULLABLE"),
    ("is_active", "BOOLEAN", "NULLABLE"),
    ("_ingested_at", "TIMESTAMP", "NULLABLE"),
    ("_source", "STRING", "NULLABLE"),
    ("_batch_id", "STRING", "NULLABLE"),
    ("_ingested_date", "DATE", "NULLABLE"),
]


def generate_customers(num=DEFAULT_NUM_CUSTOMERS):
    """Generate synthetic customer records."""
    batch_id = str(uuid.uuid4())
    now = datetime.utcnow()
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S")
    ingested_date = now.strftime("%Y-%m-%d")

    customers = []
    for i in range(num):
        is_indian = random.random() < 0.70
        f = fake_in if is_indian else fake
        gender = random.choices(GENDERS, weights=GENDER_WEIGHTS, k=1)[0]

        first_name = (f.first_name_male() if gender == "Male"
                      else f.first_name_female() if gender == "Female"
                      else f.first_name())
        last_name = f.last_name()

        if is_indian:
            city, state, country = random.choice(INDIAN_CITIES), random.choice(INDIAN_STATES), "India"
            phone = f"+91{random.randint(7000000000, 9999999999)}"
        else:
            city, state, country = fake.city(), fake.state(), fake.country()
            phone = fake.phone_number()

        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1,999)}@{random.choice(EMAIL_DOMAINS)}"

        customers.append({
            "customer_id": i + 1,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "address": f.street_address(),
            "city": city,
            "state": state,
            "country": country,
            "age": random.randint(18, 65),
            "gender": gender,
            "registration_date": fake.date_between(
                start_date="-2y", end_date="today"
            ).strftime("%Y-%m-%d"),
            "is_active": random.choices([True, False], weights=[0.85, 0.15], k=1)[0],
            "_ingested_at": ingested_at,
            "_source": "synthetic_faker",
            "_batch_id": batch_id,
            "_ingested_date": ingested_date,
        })

    logger.info(f"Generated {len(customers)} customers | batch_id={batch_id}")
    return customers, batch_id


def upload_to_gcs(customers, batch_id):
    """Upload customer CSV to GCS Bronze zone."""
    from google.cloud import storage as gcs

    client = gcs.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCS_BUCKET_BRONZE)
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    blob_name = f"raw_customers/{date_path}/{batch_id}.csv"
    blob = bucket.blob(blob_name)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=customers[0].keys())
    writer.writeheader()
    writer.writerows(customers)
    blob.upload_from_string(buf.getvalue(), content_type="text/csv")

    uri = f"gs://{GCS_BUCKET_BRONZE}/{blob_name}"
    logger.info(f"Uploaded to {uri}")
    return uri


def load_to_bigquery(customers):
    """Load customers to BigQuery Bronze."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)
    table_id = f"{GCP_PROJECT}.{BQ_DATASET_BRONZE}.raw_customers"

    schema = [bigquery.SchemaField(n, t, mode=m) for n, t, m in BQ_SCHEMA]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="_ingested_date",
        ),
        clustering_fields=["city", "gender"],
    )

    job = client.load_table_from_json(customers, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    logger.info(f"Loaded to {table_id} | total rows: {table.num_rows}")
    return table.num_rows


def save_local(customers, batch_id):
    """Save locally for dry-run mode."""
    os.makedirs("output", exist_ok=True)
    path = f"output/raw_customers_{batch_id[:8]}.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=customers[0].keys())
        w.writeheader()
        w.writerows(customers)
    logger.info(f"Saved {len(customers)} customers to {path}")
    return path


def main(request=None):
    """Main pipeline."""
    logger.info("=" * 60)
    logger.info("RetailFlow | Customer Generation — START")
    logger.info("=" * 60)
    try:
        customers, batch_id = generate_customers()

        if DRY_RUN:
            fp = save_local(customers, batch_id)
            result = {"status": "success", "mode": "dry_run",
                      "file": fp, "count": len(customers)}
        else:
            gcs_uri = upload_to_gcs(customers, batch_id)

            total = load_to_bigquery(customers)
            result = {"status": "success", "batch_id": batch_id,
                      "customers": len(customers), "gcs_uri": gcs_uri,
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


def ingest_customers(request):
    return main(request)


if __name__ == "__main__":
    main()
