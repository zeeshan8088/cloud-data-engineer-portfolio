-- CURATED table: cleaned and analytics-ready data
-- Built from RAW tables using transformation logic

CREATE TABLE curated.events (
  event_id STRING,
  user_id STRING,
  event_type STRING,
  event_timestamp TIMESTAMP,
  event_date DATE,              -- derived field
  country STRING,
  device STRING
)
PARTITION BY event_date
CLUSTER BY user_id, event_type;