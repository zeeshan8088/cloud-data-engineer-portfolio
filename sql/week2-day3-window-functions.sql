-- Week 2 Day 3: Window Functions

-- Total spend per user (without collapsing rows)
SELECT
  order_id,
  user_id,
  amount,
  SUM(amount) OVER (PARTITION BY user_id) AS total_spent_per_user
FROM orders;

-- Row number per user by highest amount
SELECT
  order_id,
  user_id,
  amount,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn
FROM orders;

-- Rank orders per user
SELECT
  order_id,
  user_id,
  amount,
  RANK() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rank_val
FROM orders;
