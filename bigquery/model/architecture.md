# BigQuery Warehouse Architecture

## High-Level Flow

Source (Public Dataset)
        ↓
RAW Layer
- raw.raw_taxi_trips
        ↓
CURATED Layer
- curated.fact_taxi_trips
        ↓
Analytics / BI / Aggregations

---

## Layer Responsibilities

### RAW
- Safety layer
- Immutable
- Stores original data
- Enables reprocessing and debugging

### CURATED
- Analytics-friendly
- Partitioned for performance
- Contains derived metrics
- Used by analysts and dashboards

---

## Key Principle

RAW exists so CURATED can always be rebuilt safely.