# RetailFlow Cost Optimisation Audit (Day 11)

## Overview
This audit documents the partitioning, clustering, and materialized view optimizations implemented in the RetailFlow data platform to minimize query bytes processed and reduce BigQuery costs.

## 1. Partitioning & Clustering Audit
Partitioning acts to prune data before reading, directly reducing query cost. Clustering sorts the data within partitions, accelerating queries that filter or aggregate on specific clustered columns. 

**Gold Layer Optimisations:**
- `mart_sales_daily`: 
  - **Partitioned by:** `order_date` (day granularity)
  - **Clustered by:** `top_category`
  - *Benefit:* Dashboards typically query recent date ranges and allow category drill-downs. By returning only the filtered days/categories, BigQuery completely avoids full table scans.
- `mart_funnel`: 
  - **Partitioned by:** `event_date` (day granularity)
  - *Benefit:* Daily conversion funnel metrics can now be queried incrementally or time-filtered without scanning the entire clickstream history.

## 2. Materialized Views
- **Created:** `retailflow_gold.mv_daily_category_revenue`
- **Definition:** Aggregates order volume and revenue by date and category at the Silver layer.
- **Refresh Setting:** Automatic refresh enabled (`refresh_interval_minutes = 60`).
- **Why it saves cost:** Dashboard widgets that chart category revenue trends heavily aggregate millions of raw rows daily. By materializing the daily results, Looker Studio visualizations execute instantly via zero-byte pre-computed lookups.
*(Note: Materialized views on dbt models built with window-functions for deduplication require workarounds or base table targets depending on the dialect, so optimal strategy mandates ensuring the base `stg_` models are materialized properly prior).*

## 3. Top Expensive Queries Findings
An analysis of the top expensive queries over the past 7 days (`INFORMATION_SCHEMA.JOBS_BY_PROJECT`) revealed the following patterns:
1. **Dbt Full-Refreshes:** Large table regenerations that don't utilize incremental materialization result in massive bytes processed.
2. **Unpartitioned Window Functions:** Calculations like Customer LTV models that require sweeping entire transaction histories.
3. **Cross Joins:** Sub-optimal dbt test queries (such as exact reconciliation checks mapping aggregated data to staging data without partition-binding).

## 4. Current Estimated Monthly Cost (Synthetic Data Scale)
- At current volume (synthetic data generated for portfolio), the daily ingestion and dbt transformations process less than 1-2 GB total.
- **Estimated Daily Cost:** ~$0.01
- **Estimated Monthly Cost:** ~$0.30 
- **BigQuery Free-Tier:** 1 TB of query processing is free each month. Because we partition appropriately, we would remain comfortably within the free tier forever at this dataset size.

## 5. Cost Estimation Tool
- A developer tool has been added at `scripts/estimate_query_cost.py` to intercept runaway query costs.
- **Usage:** Engineers can pass a raw SQL string to see precisely the Data Processed (MB) and the USD Cost estimate *before* execution via the `dry-run` BigQuery API flag.

