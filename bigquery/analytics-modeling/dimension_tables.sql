-- Dimension Tables
-- Dimension tables store descriptive attributes

-- Example: Customer dimension
CREATE TABLE analytics.dim_customer (
  customer_id STRING,
  customer_name STRING,
  city STRING,
  country STRING
);

-- Example: Product dimension
CREATE TABLE analytics.dim_product (
  product_id STRING,
  product_name STRING,
  category STRING
);

-- Notes:
-- - Dimension tables are smaller
-- - Used for filtering and grouping
-- - Joined with fact tables