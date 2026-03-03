# Week 3 – BigQuery Two-Layer Data Warehouse

## Objective
Design a simple, cost-efficient BigQuery data warehouse using
best practices for ingestion, partitioning, and analytics readiness.

Dataset used: **NYC Taxi Trips (BigQuery Public Dataset)**

---

## Warehouse Design

This project follows a **two-layer architecture**:

### 1. RAW Layer
- Purpose: Store data as received from source systems
- Append-only, immutable
- Used as a source of truth and for reprocessing

Table:
- `raw.raw_taxi_trips`

Characteristics:
- Minimal transformation
- Includes ingestion timestamp
- Never queried directly by dashboards

---

### 2. CURATED Layer
- Purpose: Make data analytics-ready
- Built entirely from RAW
- Can be dropped and rebuilt safely

Table:
- `curated.fact_taxi_trips`

Characteristics:
- Cleaned timestamps
- Derived columns (trip_date, trip_duration_minutes)
- Partitioned by trip date for cost-efficient querying

---

## Key Design Decisions

- RAW data is never updated or deleted
- CURATED data is reproducible from RAW
- Partitioning is applied only in CURATED
- Negative transaction amounts are preserved (refunds/adjustments)
- Sandbox-safe BigQuery approach (no billing required)

---

## Outcome

- ~3.6 million taxi trip records ingested
- Clear separation between ingestion and analytics
- Interview-ready warehouse design