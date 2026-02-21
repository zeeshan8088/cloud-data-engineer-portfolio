# BigQuery Data Modeling – Partitioning & Clustering

## Problem
BigQuery charges based on how much data a query scans.
Poor table design leads to unnecessary full-table scans and high costs.

This module demonstrates how to design a BigQuery table that is:
- Cost-efficient
- Query-friendly
- Production-ready

## Partitioning
The table is partitioned by `event_date`.

Why:
- Most analytics queries are time-bounded (daily, weekly, monthly).
- Partitioning allows BigQuery to scan only relevant date partitions.
- Without a date filter, partitioning provides no benefit.

## Clustering
The table is clustered by:
- `user_id`
- `event_type`

Why:
- These columns are frequently used in WHERE clauses.
- They have sufficient cardinality.
- Clustering helps BigQuery skip irrelevant data blocks inside partitions.

## Cost-Aware Design
- Queries must filter on the partition column to reduce scan cost.
- Clustering optimizes scans only after partition pruning.
- Avoid functions on partition columns.
- Avoid SELECT * in analytical queries.

This design reflects real-world BigQuery best practices.