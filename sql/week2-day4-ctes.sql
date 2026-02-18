-- Week 2 Day 4: CTEs and Modular SQL

-- Basic CTE
WITH user_totals AS (
  SELECT
    user_id,
    SUM(amount) AS total_spent
  FROM orders
  GROUP BY user_id
)
SELECT *
FROM user_totals
WHERE total_spent > 300;

-- CTE with window function
WITH orders_with_totals AS (
  SELECT
    order_id,
    user_id,
    amount,
    SUM(amount) OVER (PARTITION BY user_id) AS total_spent_per_user
  FROM orders
)
SELECT *
FROM orders_with_totals;

-- Multiple CTEs
WITH user_totals AS (
  SELECT
    user_id,
    SUM(amount) AS total_spent
  FROM orders
  GROUP BY user_id
),
ranked_users AS (
  SELECT
    user_id,
    total_spent,
    RANK() OVER (ORDER BY total_spent DESC) AS rank_val
  FROM user_totals
)
SELECT *
FROM ranked_users
WHERE rank_val = 1;
