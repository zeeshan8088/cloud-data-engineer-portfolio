-- RAW table: stores incoming data exactly as received
-- No transformations, no assumptions, append-only

CREATE TABLE raw.events (
  event_id STRING,
  user_id STRING,
  event_type STRING,
  event_timestamp TIMESTAMP,
  metadata STRING,              -- JSON stored as-is
  ingestion_time TIMESTAMP      -- when data was received
)
PARTITION BY DATE(ingestion_time);