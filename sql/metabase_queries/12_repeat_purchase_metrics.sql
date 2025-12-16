-- Repeat Purchase Rate & Repeat Revenue share

WITH base_line_items AS (
  SELECT p.order_id, pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1) AS gross_line_total,
         o.user_id, o.transaction_date::date AS order_date
  FROM operations_line_item_data_products p
  JOIN operations_line_item_data_prices pr ON p.order_id = pr.order_id
  JOIN operations_order_data o ON p.order_id = o.order_id
  WHERE o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
),
customer_orders AS (
  SELECT user_id, COUNT(DISTINCT order_id) AS total_orders, SUM(gross_line_total) AS total_spend
  FROM base_line_items
  GROUP BY user_id
),
repeat_metrics AS (
  SELECT
    COUNT(*) FILTER (WHERE total_orders > 1) AS repeat_customers,
    COUNT(*) FILTER (WHERE total_orders = 1) AS one_time_customers,
    COUNT(*) AS total_customers,
    SUM(total_spend) FILTER (WHERE total_orders > 1) AS repeat_spend,
    SUM(total_spend) AS total_spend
  FROM customer_orders
)
SELECT
  repeat_customers,
  one_time_customers,
  total_customers,
  (repeat_customers::numeric / total_customers::numeric) * 100 AS repeat_rate_pct,
  repeat_spend,
  total_spend,
  (repeat_spend::numeric / NULLIF(total_spend,0)) * 100 AS repeat_spend_pct,
  (total_spend::numeric / total_customers::numeric) AS avg_customer_value
FROM repeat_metrics;
