-- Revenue by Product Category
-- Show product_type or product_name breakdown

WITH base_line_items AS (
  -- include the base CTE from 00_base_line_item_cte.sql
  SELECT * FROM (
    SELECT p.order_id, p.product_id, p.product_name, b.product_type, pr.price::numeric AS unit_price,
           COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1) AS qty,
           (pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1)) AS gross_line_total,
           o.transaction_date::date AS order_date
    FROM operations_line_item_data_products p
    JOIN operations_line_item_data_prices pr ON p.order_id = pr.order_id
    JOIN operations_order_data o ON p.order_id = o.order_id
    LEFT JOIN business_product_list b ON p.product_id = b.product_id
    WHERE o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
  ) t
)
SELECT
  COALESCE(product_type, 'Unknown') AS product_type,
  SUM(gross_line_total) AS total_revenue,
  SUM(qty) AS total_quantity_sold,
  COUNT(DISTINCT order_id) AS distinct_orders,
  SUM(gross_line_total) / NULLIF(COUNT(DISTINCT order_id),0) AS aov
FROM base_line_items
GROUP BY product_type
ORDER BY total_revenue DESC
LIMIT 100; -- set limit for Metabase visual
