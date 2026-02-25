# Week 3 Day 3 — Analytics Modeling (Star Schema & SCD Type 2)

## Purpose
Curated data is clean but not optimized for analytics.
Analytics modeling organizes data so queries are:
- Simple
- Fast
- Consistent
- Business-friendly

This is done using:
- Fact tables
- Dimension tables
- Star schema (preferred)
- Snowflake schema (when needed)
- Slowly Changing Dimensions (SCD Type 2)

---

## Core Concepts

### Fact Tables
Fact tables store measurable events.
They are large and contain numeric values used for aggregation.

Examples:
- orders
- payments
- clicks
- events

Fact tables answer:
- How much?
- How many?
- How often?

---

### Dimension Tables
Dimension tables store descriptive context.
They are smaller and used for filtering and grouping.

Examples:
- customers
- products
- locations
- time

Dimension tables answer:
- Who?
- What?
- Where?

---

## Star Schema
- One central fact table
- Multiple dimension tables
- Simple joins
- Fast queries
- Preferred for analytics

---

## Snowflake Schema
- Dimensions normalized into sub-dimensions
- More joins
- Less duplication
- Used only when dimensions become very large

---

## Slowly Changing Dimensions (SCD Type 2)
Dimension data changes over time.
SCD Type 2 preserves history by:
- Closing old records
- Inserting new records
- Never overwriting history

This allows correct historical analysis.