-- Fact Tables
-- Fact tables store measurable business events

-- Example: Orders fact table
CREATE TABLE analytics.fact_orders (
  order_id STRING,
  order_date DATE,
  customer_id STRING,
  product_id STRING,
  amount NUMERIC
);

-- Notes:
-- - Fact tables are large
-- - Contain numeric metrics
-- - Reference dimensions using IDs
-- - Used for aggregations (SUM, COUNT, AVG)