-- Campaign performance: compare availed vs non-availed orders; campaign revenue and average discount

WITH base_line_items AS (
  SELECT p.order_id, pr.price::numeric AS unit_price, COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1) AS qty,
         (pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1)) AS gross_line_total,
         o.transaction_date::date AS order_date,
         t.campaign_id, c.campaign_name, t.availed,
         CASE WHEN t.availed = 1 AND c.discount IS NOT NULL THEN (regexp_replace(c.discount, '[^0-9.]', '', 'g')::numeric / 100) ELSE 0 END AS discount_pct
  FROM operations_line_item_data_products p
  JOIN operations_line_item_data_prices pr ON p.order_id = pr.order_id
  JOIN operations_order_data o ON p.order_id = o.order_id
  LEFT JOIN marketing_transactional_campaign_data t ON p.order_id = t.order_id
  LEFT JOIN marketing_campaign_data c ON t.campaign_id = c.campaign_id
  WHERE o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
),
-- calculate net revenue per line considering campaign discount if availed
line_revenue AS (
  SELECT *,
         gross_line_total - (gross_line_total * discount_pct) AS net_revenue
  FROM base_line_items
)
SELECT
  COALESCE(campaign_id, 'no_campaign') AS campaign_id,
  COALESCE(campaign_name, 'No Campaign') AS campaign_name,
  SUM(net_revenue) AS total_revenue,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(CASE WHEN availed = 1 THEN 1 ELSE 0 END) AS availed_count,
  AVG(discount_pct) FILTER (WHERE discount_pct > 0) AS avg_discount_pct
FROM line_revenue
GROUP BY campaign_id, campaign_name
ORDER BY total_revenue DESC
LIMIT 200;
