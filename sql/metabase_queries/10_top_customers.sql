-- Top Customers by Total Spend
-- KPI/Bar chart

WITH base_line_items AS (
  SELECT p.order_id, pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1) AS gross_line_total,
         o.user_id, o.transaction_date::date AS order_date
  FROM operations_line_item_data_products p
  JOIN operations_line_item_data_prices pr ON p.order_id = pr.order_id
  JOIN operations_order_data o ON p.order_id = o.order_id
  WHERE o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
),
customer_revenue AS (
  SELECT user_id, SUM(gross_line_total) AS total_spend, COUNT(DISTINCT order_id) AS total_orders
  FROM base_line_items
  GROUP BY user_id
)
SELECT cr.user_id, cu.name AS customer_name, cr.total_spend, cr.total_orders
FROM customer_revenue cr
LEFT JOIN customer_user_data cu ON cr.user_id = cu.user_id
ORDER BY cr.total_spend DESC
LIMIT 100;
