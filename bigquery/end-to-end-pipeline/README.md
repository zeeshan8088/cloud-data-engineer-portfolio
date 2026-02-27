# Week 3 Day 6 — End-to-End Data Pipeline Design

## Objective
Design a reliable, scalable, and cost-efficient data pipeline
from ingestion to analytics consumption.

---

## Pipeline Layers

1. Source Systems
2. RAW Tables (append-only ingestion)
3. CURATED Tables (cleaned data)
4. FACT & DIMENSION Tables (analytics modeling)
5. Materialized Views (performance optimization)
6. Dashboards & Consumers

---

## Design Principles
- RAW data is immutable
- CURATED data is reproducible
- Analytics modeled using star schema
- History preserved using SCD Type 2
- Cost optimized using materialized views