#!/bin/bash
# ============================================================
# Week 5 - Cloud Scheduler Setup
# Triggers the Cloud Function every 60 seconds automatically
# ============================================================

PROJECT_ID="intricate-ward-459513-e1"
REGION="us-central1"
FUNCTION_URL="https://us-central1-intricate-ward-459513-e1.cloudfunctions.net/fetch-and-publish-prices"

# Create the scheduler job
gcloud scheduler jobs create http crypto-price-fetcher \
  --location=$REGION \
  --schedule="* * * * *" \
  --uri=$FUNCTION_URL \
  --http-method=GET \
  --attempt-deadline=60s \
  --description="Fetches BTC/ETH/SOL prices every 60 seconds and streams to BigQuery"

echo "Scheduler created. Pipeline is now fully automated."