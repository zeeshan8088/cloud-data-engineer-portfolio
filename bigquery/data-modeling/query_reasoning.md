# Query Reasoning – Partitioning & Clustering

## Query 1: Optimal Query
```sql
SELECT COUNT(*)
FROM analytics.events_curated
WHERE event_date = '2024-02-01'
  AND user_id = 'U123';