-- Materialized Views for Analytics Optimization

-- Example: Sales by country
CREATE MATERIALIZED VIEW analytics.sales_by_country_mv AS
SELECT
  country,
  SUM(amount) AS total_sales
FROM analytics.fact_orders
GROUP BY country;

-- Notes:
-- - Used by dashboards
-- - Reduces repeated aggregation cost
-- - Automatically updated by BigQuery