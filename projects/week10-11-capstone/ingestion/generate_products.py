"""
RetailFlow — Synthetic Product Catalog Generator
==================================================
Cloud Function (Gen2) + standalone script.

Generates 100 e-commerce products across 7 categories with realistic
pricing, cost, stock, ratings, and supplier info.

Table: partitioned by _ingested_date, clustered by category
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
    DEFAULT_NUM_PRODUCTS,
    DRY_RUN,
)

fake = Faker()
logger = logging.getLogger("retailflow.products")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)

PRODUCT_CATALOG = {
    "electronics": {
        "range": (999, 89999),
        "names": [
            "Wireless Bluetooth Headphones", "USB-C Fast Charger",
            "Portable Power Bank 20000mAh", "Smart Watch Fitness Tracker",
            "Noise Cancelling Earbuds", "Laptop Stand Adjustable",
            "Mechanical Keyboard RGB", "Wireless Mouse Ergonomic",
            "Webcam HD 1080p", "Portable SSD 1TB",
            "HDMI Cable 4K", "USB Hub 7-Port",
            "Phone Gimbal Stabilizer", "Ring Light 10 inch",
            "Monitor Arm Mount",
        ],
    },
    "clothing": {
        "range": (299, 4999),
        "names": [
            "Cotton Crew Neck T-Shirt", "Slim Fit Jeans",
            "Hooded Sweatshirt", "Formal Dress Shirt",
            "Running Shorts", "Winter Jacket Waterproof",
            "Polo Shirt Classic", "Linen Trousers",
            "Athletic Track Pants", "Denim Jacket",
            "Flannel Shirt Checkered", "V-Neck Sweater Wool",
            "Cargo Shorts", "Blazer Slim Fit", "Kurta Set Cotton",
        ],
    },
    "home_kitchen": {
        "range": (199, 12999),
        "names": [
            "Non-Stick Cookware Set", "Electric Kettle 1.5L",
            "Air Fryer 4L", "Stainless Steel Water Bottle",
            "Silicone Baking Mat", "Cast Iron Skillet",
            "Bamboo Cutting Board Set", "French Press Coffee Maker",
            "Kitchen Scale Digital", "Spice Rack Organizer",
            "Glass Food Containers Set", "Hand Blender 800W",
            "Pressure Cooker 5L", "Toaster 2-Slice",
            "Vacuum Insulated Thermos",
        ],
    },
    "books": {
        "range": (149, 1999),
        "names": [
            "Data Engineering with Python", "Designing Data-Intensive Apps",
            "The Pragmatic Programmer", "Clean Code",
            "System Design Interview", "Atomic Habits",
            "Sapiens: A Brief History", "The Psychology of Money",
            "Deep Work", "Thinking Fast and Slow",
            "The Lean Startup", "Zero to One",
            "Ikigai", "Rich Dad Poor Dad", "The Alchemist",
        ],
    },
    "sports_fitness": {
        "range": (299, 14999),
        "names": [
            "Yoga Mat Premium 6mm", "Resistance Bands Set",
            "Adjustable Dumbbells", "Jump Rope Speed",
            "Foam Roller Muscle", "Pull Up Bar Doorway",
            "Ab Roller Wheel", "Kettlebell 16kg",
            "Gym Gloves Leather", "Sports Armband Phone",
            "Running Belt Waist Pack", "Compression Socks",
            "Boxing Hand Wraps", "Badminton Racket Pro",
            "Cricket Bat English Willow",
        ],
    },
    "beauty_personal_care": {
        "range": (99, 3999),
        "names": [
            "Face Wash Gel Cleanser", "Sunscreen SPF 50+",
            "Hair Oil Argan", "Body Lotion Moisturizing",
            "Beard Trimmer Cordless", "Lip Balm SPF",
            "Face Serum Vitamin C", "Shampoo Anti-Dandruff",
            "Deodorant Roll-On", "Hand Cream Shea Butter",
            "Hair Dryer Ionic", "Face Mask Sheet Pack",
            "Perfume Eau de Toilette", "Nail Polish Set",
            "Makeup Brush Set",
        ],
    },
    "toys_games": {
        "range": (199, 4999),
        "names": [
            "Building Blocks 500pc", "Remote Control Car",
            "Board Game Strategy", "Puzzle 1000 Pieces",
            "Action Figure Collectible", "Card Game Deck",
            "Drone Mini Quadcopter", "Rubiks Cube Speed",
            "Nerf Blaster Elite", "LEGO Architecture Set",
            "Chess Set Wooden", "Telescope Kids",
            "Science Kit Chemistry", "Art Supplies Kit",
            "Magnetic Tiles 60pc",
        ],
    },
}

SUPPLIERS = [
    "TechWorld Supplies", "CloudNine Imports", "Stellar Distributors",
    "Prime Wholesale Co", "Global Trade Hub", "Eastern Commerce Ltd",
    "Pacific Goods Inc", "Mountain View Trading", "Delta Supplies Corp",
    "Horizon Retail Partners",
]

BQ_SCHEMA = [
    ("product_id", "INTEGER", "REQUIRED"),
    ("product_name", "STRING", "NULLABLE"),
    ("category", "STRING", "NULLABLE"),
    ("price", "FLOAT64", "NULLABLE"),
    ("cost_price", "FLOAT64", "NULLABLE"),
    ("stock_quantity", "INTEGER", "NULLABLE"),
    ("supplier", "STRING", "NULLABLE"),
    ("rating", "FLOAT64", "NULLABLE"),
    ("review_count", "INTEGER", "NULLABLE"),
    ("is_available", "BOOLEAN", "NULLABLE"),
    ("_ingested_at", "TIMESTAMP", "NULLABLE"),
    ("_source", "STRING", "NULLABLE"),
    ("_batch_id", "STRING", "NULLABLE"),
    ("_ingested_date", "DATE", "NULLABLE"),
]


def generate_products(num=DEFAULT_NUM_PRODUCTS):
    """Generate synthetic product catalog."""
    batch_id = str(uuid.uuid4())
    now = datetime.utcnow()
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S")
    ingested_date = now.strftime("%Y-%m-%d")

    categories = list(PRODUCT_CATALOG.keys())
    products = []

    for i in range(num):
        cat = categories[i % len(categories)]
        cat_info = PRODUCT_CATALOG[cat]
        name_idx = (i // len(categories)) % len(cat_info["names"])
        name = cat_info["names"][name_idx]

        lo, hi = cat_info["range"]
        price = round(random.uniform(lo, hi), 2)
        cost_price = round(price * random.uniform(0.40, 0.75), 2)
        stock = random.choices(
            [0, random.randint(1, 20), random.randint(21, 200), random.randint(201, 1000)],
            weights=[0.05, 0.25, 0.50, 0.20], k=1
        )[0]

        products.append({
            "product_id": i + 1,
            "product_name": name,
            "category": cat,
            "price": price,
            "cost_price": cost_price,
            "stock_quantity": stock,
            "supplier": random.choice(SUPPLIERS),
            "rating": round(random.uniform(2.5, 5.0), 1),
            "review_count": random.randint(0, 5000),
            "is_available": stock > 0,
            "_ingested_at": ingested_at,
            "_source": "synthetic_faker",
            "_batch_id": batch_id,
            "_ingested_date": ingested_date,
        })

    logger.info(f"Generated {len(products)} products | batch_id={batch_id}")
    return products, batch_id


def upload_to_gcs(products, batch_id):
    """Upload product CSV to GCS Bronze zone."""
    from google.cloud import storage as gcs

    client = gcs.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCS_BUCKET_BRONZE)
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    blob_name = f"raw_products/{date_path}/{batch_id}.csv"
    blob = bucket.blob(blob_name)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=products[0].keys())
    writer.writeheader()
    writer.writerows(products)
    blob.upload_from_string(buf.getvalue(), content_type="text/csv")

    uri = f"gs://{GCS_BUCKET_BRONZE}/{blob_name}"
    logger.info(f"Uploaded to {uri}")
    return uri


def load_to_bigquery(products):
    """Load products to BigQuery Bronze."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)
    table_id = f"{GCP_PROJECT}.{BQ_DATASET_BRONZE}.raw_products"

    schema = [bigquery.SchemaField(n, t, mode=m) for n, t, m in BQ_SCHEMA]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="_ingested_date",
        ),
        clustering_fields=["category"],
    )

    job = client.load_table_from_json(products, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    logger.info(f"Loaded to {table_id} | total rows: {table.num_rows}")
    return table.num_rows


def save_local(products, batch_id):
    """Save locally for dry-run mode."""
    os.makedirs("output", exist_ok=True)
    path = f"output/raw_products_{batch_id[:8]}.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=products[0].keys())
        w.writeheader()
        w.writerows(products)
    logger.info(f"Saved {len(products)} products to {path}")
    return path


def main(request=None):
    """Main pipeline."""
    logger.info("=" * 60)
    logger.info("RetailFlow | Product Generation — START")
    logger.info("=" * 60)
    try:
        products, batch_id = generate_products()

        if DRY_RUN:
            fp = save_local(products, batch_id)
            result = {"status": "success", "mode": "dry_run",
                      "file": fp, "count": len(products)}
        else:
            gcs_uri = upload_to_gcs(products, batch_id)

            total = load_to_bigquery(products)
            result = {"status": "success", "batch_id": batch_id,
                      "products": len(products), "gcs_uri": gcs_uri,
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


def ingest_products(request):
    return main(request)


if __name__ == "__main__":
    main()
