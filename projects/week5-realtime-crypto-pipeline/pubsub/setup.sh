#!/bin/bash
# ============================================================
# Week 5 - Pub/Sub Setup Script
# This script creates:
# 1. Main topic (where crypto prices get published)
# 2. Dead letter topic (catches failed messages)
# 3. Subscription (BigQuery listens here)
# ============================================================

PROJECT_ID="intricate-ward-459513-e1"
TOPIC_NAME="crypto-prices-topic"
DEAD_LETTER_TOPIC="crypto-prices-dead-letter"
SUBSCRIPTION_NAME="crypto-prices-sub"

echo "Setting project..."
gcloud config set project $PROJECT_ID

echo ""
echo "Step 1: Creating main Pub/Sub topic..."
gcloud pubsub topics create $TOPIC_NAME
echo "✓ Topic created: $TOPIC_NAME"

echo ""
echo "Step 2: Creating dead letter topic..."
gcloud pubsub topics create $DEAD_LETTER_TOPIC
echo "✓ Dead letter topic created: $DEAD_LETTER_TOPIC"

echo ""
echo "Step 3: Creating subscription with dead letter policy..."
gcloud pubsub subscriptions create $SUBSCRIPTION_NAME \
  --topic=$TOPIC_NAME \
  --dead-letter-topic=$DEAD_LETTER_TOPIC \
  --max-delivery-attempts=5 \
  --ack-deadline=60
echo "✓ Subscription created: $SUBSCRIPTION_NAME"

echo ""
echo "============================================================"
echo "ALL DONE! Your Pub/Sub infrastructure is ready."
echo "Topic: $TOPIC_NAME"
echo "Dead Letter: $DEAD_LETTER_TOPIC"
echo "Subscription: $SUBSCRIPTION_NAME"
echo "============================================================"