# pipeline.py
# ----------------------------------------------------------
# Purpose: Main pipeline coordinator for the AI Quality
#          Monitor. Wires together data loading, anomaly
#          detection, and LLM explanation into one flow.
#
# Run this file to execute the full pipeline:
#   python pipeline.py
#
# Flow:
#   1. Load e-commerce orders from CSV
#   2. Detect all anomalous rows
#   3. Send each anomaly to Gemini for explanation
#   4. Print a full quality report
#   5. Save report to data/quality_report.txt
# ----------------------------------------------------------

import time
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
# This picks up your GEMINI_API_KEY automatically
load_dotenv()

# Import our three specialist modules
from src.detect_anomalies import detect_anomalies
from src.llm_summarizer import get_anomaly_explanation

# ── Config ────────────────────────────────────────────────
DATA_PATH    = os.path.join("data", "sample_orders.csv")
REPORT_PATH  = os.path.join("data", "quality_report.txt")

# Pause between Gemini API calls (seconds)
# This prevents hitting rate limits — like waiting between
# phone calls to a busy call centre
API_CALL_DELAY = 3


# ── Report builder ────────────────────────────────────────
def build_report_header(anomaly_count: int) -> str:
    """Build the top section of the quality report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"{'=' * 65}\n"
        f"  AI DATA QUALITY REPORT — E-commerce Orders Pipeline\n"
        f"  Generated: {now}\n"
        f"  Total anomalies detected: {anomaly_count}\n"
        f"{'=' * 65}\n"
    )


def build_anomaly_block(index: int, anomaly: dict, explanation: str) -> str:
    """Format one anomaly + its Gemini explanation as a report block."""
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
    """
    Execute the full AI quality monitor pipeline.
    Detects anomalies, gets Gemini explanations,
    prints report, and saves to file.
    """

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
    # Process one at a time with a delay to avoid rate limits
    report_blocks = []
    report_header = build_report_header(len(anomalies))

    for i, anomaly in enumerate(anomalies, start=1):
        print(f"  [{i}/{len(anomalies)}] Analysing {anomaly['order_id']} "
              f"({anomaly['anomaly_type']})...")

        try:
            explanation = get_anomaly_explanation(anomaly["description"])
            block = build_anomaly_block(i, anomaly, explanation)
            report_blocks.append(block)
            print(f"         ✅ Done")

        except Exception as e:
            # If one Gemini call fails, log it and continue
            # Don't let one failure stop the entire pipeline
            # This is called "fault tolerance" — a key production pattern
            error_msg = f"   ⚠️  Gemini call failed: {str(e)}"
            block = build_anomaly_block(i, anomaly, error_msg)
            report_blocks.append(block)
            print(f"         ⚠️  Failed: {e}")

        # Rate limiting pause — skip after the last item
        if i < len(anomalies):
            time.sleep(API_CALL_DELAY)

    # ── Step 3: Assemble and print the report ─────────────
    full_report = report_header + "".join(report_blocks)

    print("\n" + full_report)

    # ── Step 4: Save report to file ───────────────────────
    os.makedirs("data", exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(full_report)

    print(f"\n{'=' * 65}")
    print(f"✅ Pipeline complete. Report saved to: {REPORT_PATH}")
    print(f"{'=' * 65}\n")


# ── Entry point ───────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()