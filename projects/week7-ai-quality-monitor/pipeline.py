# pipeline.py
# ----------------------------------------------------------
# Purpose: Main pipeline coordinator for the AI Quality
#          Monitor. Wires together data loading, anomaly
#          detection, LLM explanation, and BigQuery storage.
#
# Run this file to execute the full pipeline:
#   python pipeline.py
#
# Flow:
#   1. Load e-commerce orders from CSV
#   2. Detect all anomalous rows
#   3. Send each anomaly to Gemini for explanation
#   4. Write all results to BigQuery
#   5. Print + save full quality report
# ----------------------------------------------------------

import time
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import our specialist modules
from src.detect_anomalies import detect_anomalies
from src.llm_summarizer import get_anomaly_explanation
from src.bq_writer import write_to_bigquery

# ── Config ────────────────────────────────────────────────
DATA_PATH   = os.path.join("data", "sample_orders.csv")
REPORT_PATH = os.path.join("data", "quality_report.txt")

# Pause between Gemini API calls to avoid rate limits
API_CALL_DELAY = 3


# ── Report builder ────────────────────────────────────────
def build_report_header(anomaly_count: int) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"{'=' * 65}\n"
        f"  AI DATA QUALITY REPORT — E-commerce Orders Pipeline\n"
        f"  Generated: {now}\n"
        f"  Total anomalies detected: {anomaly_count}\n"
        f"{'=' * 65}\n"
    )


def build_anomaly_block(index: int, anomaly: dict, explanation: str) -> str:
    return (
        f"\n[{index}] {anomaly['anomaly_type']} — {anomaly['order_id']}\n"
        f"{'-' * 65}\n"
        f"📌 DETECTED ISSUE:\n"
        f"   {anomaly['description']}\n\n"
        f"🤖 GEMINI ANALYSIS:\n"
        f"   {explanation.strip()}\n"
    )


# ── Main pipeline ─────────────────────────────────────────
def run_pipeline():

    print("\n" + "=" * 65)
    print("  AI QUALITY MONITOR PIPELINE — Starting")
    print("=" * 65)

    # ── Step 1: Detect anomalies ──────────────────────────
    print(f"\n📂 Loading orders from: {DATA_PATH}")
    anomalies = detect_anomalies(DATA_PATH)
    print(f"🚨 Detected {len(anomalies)} anomalies — sending to Gemini...\n")

    if not anomalies:
        print("✅ No anomalies found. Dataset is clean.")
        return

    # ── Step 2: Get Gemini explanations ───────────────────
    # We now collect results into a list as we go.
    # Each item: {"anomaly": ..., "explanation": ...}
    # This list is what we'll hand to bq_writer at the end.
    results = []
    report_blocks = []

    for i, anomaly in enumerate(anomalies, start=1):
        print(f"  [{i}/{len(anomalies)}] Analysing {anomaly['order_id']} "
              f"({anomaly['anomaly_type']})...")

        try:
            explanation = get_anomaly_explanation(anomaly["description"])
            status = "✅ Done"

        except Exception as e:
            explanation = f"Gemini call failed: {str(e)}"
            status = f"⚠️  Failed"

        # Collect for BigQuery — always append even if failed
        results.append({
            "anomaly":     anomaly,
            "explanation": explanation
        })

        # Collect for text report
        block = build_anomaly_block(i, anomaly, explanation)
        report_blocks.append(block)

        print(f"         {status}")

        # Rate limiting pause
        if i < len(anomalies):
            time.sleep(API_CALL_DELAY)

    # ── Step 3: Write to BigQuery ─────────────────────────
    # Hand the complete results list to bq_writer in one shot.
    # One batch write is more efficient than writing row by row.
    run_id = write_to_bigquery(results)

    # ── Step 4: Assemble and save text report ─────────────
    full_report = build_report_header(len(anomalies)) + "".join(report_blocks)
    full_report += f"\n{'=' * 65}\n"
    full_report += f"  BigQuery Run ID: {run_id}\n"
    full_report += f"  Table: intricate-ward-459513-e1.ecommerce_quality_monitor.anomaly_reports\n"
    full_report += f"{'=' * 65}\n"

    print("\n" + full_report)

    os.makedirs("data", exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(full_report)

    print(f"✅ Pipeline complete.")
    print(f"   Report saved to : {REPORT_PATH}")
    print(f"   BigQuery run ID : {run_id}\n")


# ── Entry point ───────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()