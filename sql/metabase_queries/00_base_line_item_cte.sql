-- Base CTE for line items used by Metabase queries
-- Variables you can use in Metabase: {{start_date}} (Date), {{end_date}} (Date)
-- Replace staging table names below with your final fact/dim names after ETL completes.

WITH base_line_items AS (
  SELECT
    p.order_id,
    p.product_id,
    p.product_name,
    b.product_type,
    pr.price::numeric AS unit_price,
    -- normalize quantity strings like '8pc' or '5piece' to integer
    COALESCE(NULLIF(regexp_replace(pr.quantity, '[^0-9]', '', 'g'), ''), '1')::int AS qty,
    (pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity, '[^0-9]', '', 'g'), ''), '1')::int) AS gross_line_total,
    o.user_id,
    o.transaction_date::date AS order_date,
    m.merchant_id,
    mm.name AS merchant_name,
    t.campaign_id,
    c.campaign_name,
    -- parse discount_pct; fall back to 0 if not present or not availed
    CASE WHEN t.availed = 1 AND c.discount IS NOT NULL
         THEN (regexp_replace(c.discount, '[^0-9.]', '', 'g')::numeric / 100)
         ELSE 0 END AS discount_pct,
    CASE WHEN t.availed = 1 AND c.discount IS NOT NULL
         THEN (pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity, '[^0-9]', '', 'g'), ''), '1')::int)
              * (regexp_replace(c.discount, '[^0-9.]', '', 'g')::numeric / 100)
         ELSE 0 END AS discount_amount,
    -- net revenue after discount applied evenly to line
    (pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity, '[^0-9]', '', 'g'), ''), '1')::int) -
    CASE WHEN t.availed = 1 AND c.discount IS NOT NULL
         THEN (pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity, '[^0-9]', '', 'g'), ''), '1')::int)
              * (regexp_replace(c.discount, '[^0-9.]', '', 'g')::numeric / 100)
         ELSE 0 END AS net_revenue
  FROM operations_line_item_data_products p
  JOIN operations_line_item_data_prices pr ON p.order_id = pr.order_id
  JOIN operations_order_data o ON p.order_id = o.order_id
  LEFT JOIN enterprise_order_with_merchant_data m ON p.order_id = m.order_id
  LEFT JOIN enterprise_merchant_data mm ON m.merchant_id = mm.merchant_id
  LEFT JOIN marketing_transactional_campaign_data t ON p.order_id = t.order_id
  LEFT JOIN marketing_campaign_data c ON t.campaign_id = c.campaign_id
  LEFT JOIN business_product_list b ON p.product_id = b.product_id
  WHERE (o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date))
)
SELECT * FROM base_line_items -- keep for debugging; usually you'll wrap it in other queries.
