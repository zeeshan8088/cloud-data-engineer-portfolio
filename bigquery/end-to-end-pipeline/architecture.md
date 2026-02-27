# End-to-End Pipeline Architecture

Source Systems
      ↓
RAW Tables
      ↓
CURATED Tables
      ↓
FACT Tables + DIM Tables
      ↓
Materialized Views
      ↓
Dashboards / Analytics

---

## Key Decisions
- Append-only ingestion for safety
- Batch processing for analytics
- Event ingestion for low latency
- Materialized views for cost reduction