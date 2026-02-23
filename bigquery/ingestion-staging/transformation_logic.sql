-- Transformation logic: RAW → CURATED
-- RAW remains unchanged; CURATED can be rebuilt anytime

INSERT INTO curated.events
SELECT
  event_id,
  user_id,
  event_type,
  event_timestamp,
  DATE(event_timestamp) AS event_date,
  JSON_VALUE(metadata, '$.country') AS country,
  JSON_VALUE(metadata, '$.device') AS device
FROM raw.events;