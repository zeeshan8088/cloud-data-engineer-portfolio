-- BigQuery Data Modeling: Partitioning & Clustering
-- This table is designed for large-scale event analytics.

CREATE TABLE analytics.events_curated (
  event_id STRING,
  user_id STRING,
  event_type STRING,
  event_timestamp TIMESTAMP,
  event_date DATE,        -- Derived from event_timestamp
  country STRING,
  device_type STRING
)
PARTITION BY event_date      -- Partition by date to reduce scanned data
CLUSTER BY user_id, event_type;  -- Cluster for efficient filtering