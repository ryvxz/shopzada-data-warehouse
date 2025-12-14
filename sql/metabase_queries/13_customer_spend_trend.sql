-- Customer Spend Trend Over Time (by month) - split by New vs Returning

WITH first_order AS (
  SELECT user_id, MIN(transaction_date::date) AS first_order_date
  FROM operations_order_data
  GROUP BY user_id
),
base AS (
  SELECT p.order_id, pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1) AS gross_line_total,
         o.user_id, o.transaction_date::date AS order_date
  FROM operations_line_item_data_products p
  JOIN operations_line_item_data_prices pr ON p.order_id = pr.order_id
  JOIN operations_order_data o ON p.order_id = o.order_id
  WHERE o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
),
classified AS (
  SELECT b.*, fo.first_order_date,
         CASE WHEN fo.first_order_date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date) THEN 'New' ELSE 'Returning' END AS customer_type
  FROM base b
  LEFT JOIN first_order fo ON b.user_id = fo.user_id
)
SELECT date_trunc('month', order_date)::date AS month_start, customer_type, SUM(gross_line_total) AS revenue
FROM classified
GROUP BY month_start, customer_type
ORDER BY month_start, customer_type;
