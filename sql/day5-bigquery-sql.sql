SELECT * 
FROM raw_data.orders;

SELECT
  order_id,
  user_id,
  amount
FROM raw_data.orders
WHERE amount > 200;


SELECT
  user_id,
  COUNT(order_id) AS total_orders,
  SUM(amount) AS total_spent
FROM raw_data.orders
GROUP BY user_id
ORDER BY total_spent DESC;


SELECT
  user_id,
  COUNT(order_id) AS total_orders,
  SUM(amount) AS total_spent
FROM raw_data.orders
GROUP BY user_id
ORDER BY total_spent DESC;


SELECT
  user_id,
  SUM(amount) AS total_spent
FROM raw_data.orders
GROUP BY user_id
HAVING total_spent > 300;
