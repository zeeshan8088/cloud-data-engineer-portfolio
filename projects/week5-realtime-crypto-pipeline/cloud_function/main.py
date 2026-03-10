# ============================================================
# Week 5 - Cloud Function v2: Real-Time Crypto Price Publisher
# Now writes directly to BigQuery AND publishes to Pub/Sub
#
# Architecture:
# CoinGecko API → validate → BigQuery streaming insert
#                          → Pub/Sub publish (for downstream)
# ============================================================

import json
import requests
import logging
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from google.cloud import bigquery

# ── Configuration ──────────────────────────────────────────
PROJECT_ID   = "intricate-ward-459513-e1"
TOPIC_NAME   = "crypto-prices-topic"
DATASET_ID   = "crypto_streaming"
TABLE_ID     = "raw_crypto_prices"

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINS         = ["bitcoin", "ethereum", "solana"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_crypto_prices() -> dict:
    """Fetch BTC, ETH, SOL prices from CoinGecko API."""
    params = {
        "ids": ",".join(COINS),
        "vs_currencies": "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }
    logger.info("Fetching prices from CoinGecko...")
    response = requests.get(COINGECKO_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    logger.info(f"Fetched: {list(data.keys())}")
    return data


def validate_price_data(coin: str, data: dict) -> bool:
    """Data quality check — reject bad data before it enters warehouse."""
    required = ["usd", "usd_24h_change", "usd_24h_vol"]
    for field in required:
        if field not in data:
            logger.error(f"VALIDATION FAILED: {coin} missing '{field}'")
            return False
    if not isinstance(data["usd"], (int, float)) or data["usd"] <= 0:
        logger.error(f"VALIDATION FAILED: {coin} invalid price {data['usd']}")
        return False
    return True


def insert_to_bigquery(bq_client, rows: list) -> None:
    """
    Stream rows directly into BigQuery.
    This is called streaming insert — data appears in seconds.
    """
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    errors = bq_client.insert_rows_json(table_ref, rows)

    if errors:
        logger.error(f"BigQuery insert errors: {errors}")
        raise RuntimeError(f"BigQuery insert failed: {errors}")
    
    logger.info(f"Successfully inserted {len(rows)} rows into BigQuery")


def publish_to_pubsub(publisher, topic_path: str, message: dict) -> str:
    """Publish message to Pub/Sub for any downstream consumers."""
    message_bytes = json.dumps(message).encode("utf-8")
    future = publisher.publish(topic_path, message_bytes)
    return future.result()


def fetch_and_publish_prices(request):
    """
    MAIN ENTRY POINT
    1. Fetch prices from CoinGecko
    2. Validate each coin's data
    3. Insert clean rows into BigQuery (streaming)
    4. Also publish to Pub/Sub (for downstream pipelines)
    """
    logger.info("=" * 50)
    logger.info(f"Triggered at {datetime.now(timezone.utc).isoformat()}")

    # ── Fetch ──────────────────────────────────────────────
    try:
        raw_data = fetch_crypto_prices()
    except requests.exceptions.RequestException as e:
        logger.error(f"CoinGecko API failed: {e}")
        return {"status": "error", "message": str(e)}, 500

    # ── Setup clients ──────────────────────────────────────
    bq_client  = bigquery.Client(project=PROJECT_ID)
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    rows_to_insert = []
    results        = []
    failed_count   = 0

    # ── Process each coin ──────────────────────────────────
    for coin in COINS:
        if coin not in raw_data:
            logger.warning(f"'{coin}' missing from API response")
            failed_count += 1
            continue

        coin_data = raw_data[coin]

        if not validate_price_data(coin, coin_data):
            failed_count += 1
            continue

        # Build clean row
        row = {
            "coin":             coin,
            "price_usd":        round(coin_data["usd"], 2),
            "volume_24h":       round(coin_data["usd_24h_vol"], 2),
            "change_24h_pct":   round(coin_data["usd_24h_change"], 4),
            "ingested_at":      datetime.now(timezone.utc).isoformat(),
            "source":           "coingecko_api",
            "pipeline_version": "week5_v2"
        }
        rows_to_insert.append(row)
        results.append({"coin": coin, "price": row["price_usd"]})

    # ── Insert all rows to BigQuery in one batch ───────────
    if rows_to_insert:
        try:
            insert_to_bigquery(bq_client, rows_to_insert)
        except Exception as e:
            logger.error(f"BigQuery insert failed: {e}")
            return {"status": "error", "message": str(e)}, 500

    # ── Publish to Pub/Sub ─────────────────────────────────
    for row in rows_to_insert:
        try:
            msg_id = publish_to_pubsub(publisher, topic_path, row)
            logger.info(f"Published {row['coin']} to Pub/Sub: {msg_id}")
        except Exception as e:
            logger.warning(f"Pub/Sub publish failed for {row['coin']}: {e}")

    # ── Summary ────────────────────────────────────────────
    summary = {
        "status":    "success",
        "published": len(rows_to_insert),
        "failed":    failed_count,
        "coins":     results,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    logger.info(f"Done. Inserted: {len(rows_to_insert)}, Failed: {failed_count}")
    logger.info("=" * 50)
    return summary, 200