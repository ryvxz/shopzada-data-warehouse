-- Revenue by Merchant

WITH base_line_items AS (
  SELECT p.order_id, pr.price::numeric AS unit_price, COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1) AS qty,
         (pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1)) AS gross_line_total,
         o.transaction_date::date AS order_date,
         mm.merchant_id, mm.name AS merchant_name
  FROM operations_line_item_data_products p
  JOIN operations_line_item_data_prices pr ON p.order_id = pr.order_id
  JOIN operations_order_data o ON p.order_id = o.order_id
  LEFT JOIN enterprise_order_with_merchant_data m ON p.order_id = m.order_id
  LEFT JOIN enterprise_merchant_data mm ON m.merchant_id = mm.merchant_id
  WHERE o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
)
SELECT
  merchant_id,
  COALESCE(merchant_name,'Unknown') AS merchant_name,
  SUM(gross_line_total) AS total_revenue,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(gross_line_total) / NULLIF(COUNT(DISTINCT order_id),0) AS aov
FROM base_line_items
GROUP BY merchant_id, merchant_name
ORDER BY total_revenue DESC
LIMIT 200;
