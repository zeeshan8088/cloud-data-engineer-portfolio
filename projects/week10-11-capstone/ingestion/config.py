"""
RetailFlow — Shared Configuration
===================================
Central config for all ingestion scripts.
Values read from environment variables with sensible defaults.
"""

import os

# ── GCP ──────────────────────────────────────────────────────────
GCP_PROJECT = os.environ.get("GCP_PROJECT", "intricate-ward-459513-e1")
GCP_REGION = os.environ.get("GCP_REGION", "asia-south1")

# ── BigQuery ─────────────────────────────────────────────────────
BQ_DATASET_BRONZE = os.environ.get("BQ_DATASET_BRONZE", "retailflow_bronze")

# ── Cloud Storage ────────────────────────────────────────────────
GCS_BUCKET_BRONZE = os.environ.get(
    "GCS_BUCKET_BRONZE",
    f"retailflow-bronze-{GCP_PROJECT}",
)

# ── Pub/Sub ──────────────────────────────────────────────────────
PUBSUB_TOPIC_CLICKSTREAM = os.environ.get(
    "PUBSUB_TOPIC_CLICKSTREAM",
    "retailflow-clickstream",
)

# ── FakeStore API ────────────────────────────────────────────────
FAKESTORE_API_URL = "https://fakestoreapi.com"

# ── Defaults ─────────────────────────────────────────────────────
DEFAULT_NUM_ORDERS = 220
DEFAULT_NUM_CUSTOMERS = 500
DEFAULT_NUM_PRODUCTS = 100
DEFAULT_NUM_CLICKSTREAM = 1000

# ── Dry-run mode (skip GCS/BQ, save locally) ────────────────────
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
