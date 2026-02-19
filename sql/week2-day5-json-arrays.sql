-- Week 2 Day 5: JSON & Arrays in BigQuery

-- Extract scalar values from JSON
SELECT
  event_id,
  user_id,
  JSON_VALUE(event_payload, '$.event') AS event_type,
  CAST(JSON_VALUE(event_payload, '$.amount') AS INT64) AS amount
FROM events;

-- Extract array from JSON
SELECT
  event_id,
  JSON_QUERY_ARRAY(event_payload, '$.items') AS items_array
FROM events;

-- Unnest array into rows
SELECT
  event_id,
  item
FROM events,
UNNEST(JSON_QUERY_ARRAY(event_payload, '$.items')) AS item;

-- Count items across events
SELECT
  item,
  COUNT(*) AS item_count
FROM events,
UNNEST(JSON_QUERY_ARRAY(event_payload, '$.items')) AS item
GROUP BY item;
