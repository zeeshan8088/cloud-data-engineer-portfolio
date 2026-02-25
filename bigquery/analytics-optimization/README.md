# Week 3 Day 4 — Materialized Views & Cost Optimization

## Problem
Repeated analytical queries over large fact tables are expensive and slow.

Dashboards often execute the same aggregations multiple times a day.

---

## Solution: Materialized Views
Materialized views store precomputed query results.

They:
- Reduce data scanned
- Improve query performance
- Lower BigQuery costs
- Are ideal for dashboards

---

## Views vs Materialized Views

| Feature | View | Materialized View |
|------|-----|------------------|
| Stores SQL | Yes | Yes |
| Stores Results | No | Yes |
| Recomputes Every Query | Yes | No |
| Cost Efficient | ❌ | ✅ |

---

## When to Use
- Repeated aggregations
- Business metrics
- Star-schema analytics

---

## Key Rule
Materialized views cache results, not logic.