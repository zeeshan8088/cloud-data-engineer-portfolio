-- Week 2 Day 1: SQL Joins

-- INNER JOIN: matched rows only
SELECT
  u.user_id,
  u.user_name,
  o.order_id,
  o.amount
FROM users u
INNER JOIN orders o
ON u.user_id = o.user_id;

-- LEFT JOIN: all users, orders if present
SELECT
  u.user_id,
  u.user_name,
  o.order_id,
  o.amount
FROM users u
LEFT JOIN orders o
ON u.user_id = o.user_id;

-- RIGHT JOIN: all orders, users if present
SELECT
  u.user_id,
  u.user_name,
  o.order_id,
  o.amount
FROM users u
RIGHT JOIN orders o
ON u.user_id = o.user_id;

-- SEMI JOIN: users with at least one order
SELECT *
FROM users u
WHERE EXISTS (
  SELECT 1
  FROM orders o
  WHERE u.user_id = o.user_id
);

-- ANTI JOIN: users with no orders
SELECT *
FROM users u
WHERE NOT EXISTS (
  SELECT 1
  FROM orders o
  WHERE u.user_id = o.user_id
);
