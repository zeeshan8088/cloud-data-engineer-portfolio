# Week 3 Day 2 — Data Ingestion & Staging (RAW → CURATED)

## Core Idea
Incoming data is often messy, duplicated, late, or inconsistent.
To build reliable analytics systems, we separate ingestion into two layers:

RAW tables and CURATED tables.

---

## RAW Tables (Source of Truth)
RAW tables store data exactly as it arrives from the source.

Characteristics:
- Append-only (INSERT only)
- No updates or deletes
- No business logic
- Schema can be flexible
- Used for debugging, backfills, and reprocessing

RAW tables act as immutable history.

---

## CURATED Tables (Analytics Ready)
CURATED tables are derived from RAW tables using transformation logic.

Characteristics:
- Cleaned and structured
- Stable schema
- Business logic applied
- Safe for dashboards and analytics
- Can be deleted and rebuilt anytime

---

## Key Design Rule
RAW data is immutable.
CURATED data is reproducible.

If transformation logic changes, CURATED tables are rebuilt from RAW.

---

## Data Flow
Source → RAW table → Transformation logic → CURATED table → Analytics