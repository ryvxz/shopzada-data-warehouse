-- Customer Retention / Repeat Purchase Funnel
-- Counts: customers with 1+ orders, 2+ orders, 3+ orders

WITH base_line_items AS (
  SELECT o.order_id, o.user_id, o.transaction_date::date AS order_date
  FROM operations_order_data o
  WHERE o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
),
customer_orders AS (
  SELECT user_id, COUNT(DISTINCT order_id) AS orders
  FROM base_line_items
  GROUP BY user_id
)
SELECT
  SUM(CASE WHEN orders >= 1 THEN 1 ELSE 0 END) AS customers_1_plus,
  SUM(CASE WHEN orders >= 2 THEN 1 ELSE 0 END) AS customers_2_plus,
  SUM(CASE WHEN orders >= 3 THEN 1 ELSE 0 END) AS customers_3_plus,
  COUNT(*) AS total_customers
FROM customer_orders;
