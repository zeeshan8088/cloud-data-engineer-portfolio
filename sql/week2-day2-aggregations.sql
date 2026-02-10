-- Week 2 Day 2: Aggregations, HAVING, ROLLUP

-- Total spend per user
SELECT
  user_id,
  SUM(amount) AS total_spent
FROM orders
GROUP BY user_id;

-- Orders count and total spend
SELECT
  user_id,
  COUNT(order_id) AS total_orders,
  SUM(amount) AS total_spent
FROM orders
GROUP BY user_id;

-- HAVING to filter aggregated results
SELECT
  user_id,
  SUM(amount) AS total_spent
FROM orders
GROUP BY user_id
HAVING SUM(amount) > 300;

-- WHERE + HAVING together
SELECT
  user_id,
  SUM(amount) AS total_spent
FROM orders
WHERE amount >= 200
GROUP BY user_id
HAVING SUM(amount) > 300;

-- ROLLUP for grand total
SELECT
  user_id,
  SUM(amount) AS total_spent
FROM orders
GROUP BY ROLLUP(user_id);
