-- Week 2 Day 6: BigQuery Optimization & Cost Awareness

-- Bad: scans all columns
SELECT *
FROM orders
WHERE order_date = '2024-01-10';

-- Good: column pruning
SELECT
  order_id,
  user_id,
  amount
FROM orders
WHERE order_date = '2024-01-10';

-- Bad: function on partition column
SELECT *
FROM orders
WHERE EXTRACT(YEAR FROM order_date) = 2024;

-- Good: partition-friendly filter
SELECT
  order_id,
  user_id,
  amount
FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31';
