 # Day 6 – End-to-End Data Pipeline

Pipeline overview:

Data Source
- CSV files (batch)
- Order events (real-time)

Ingestion
- Batch upload
- Event-driven functions
- Pub/Sub messaging

Raw Storage
- BigQuery raw tables (raw_data.orders)

Analytics
- Aggregation queries
- Cost-aware SQL
- Business metrics per user

Key idea:
Ingestion method can change, but analytics queries remain stable.
