-- Revenue KPIs for Metabase
-- KPI to show: Total Revenue, Total Orders, AOV, Revenue Growth % (period over period)
-- Uses base_line_items CTE at top (or paste 00_base_line_item_cte.sql content before this query in Metabase)

WITH base_line_items AS (
  -- paste content of 00_base_line_item_cte.sql here or call view if created in DB
  -- trimmed for brevity in the saved file
  SELECT * FROM (
    SELECT p.order_id, p.product_id, pr.price::numeric AS unit_price, COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1) AS qty,
           (pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1)) AS gross_line_total,
           o.user_id, o.transaction_date::date AS order_date
    FROM operations_line_item_data_products p
    JOIN operations_line_item_data_prices pr ON p.order_id = pr.order_id
    JOIN operations_order_data o ON p.order_id = o.order_id
    WHERE (o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date))
  ) t
),
order_agg AS (
  SELECT order_id,
         SUM(gross_line_total) AS gross_order_total
  FROM base_line_items
  GROUP BY order_id
),
period_values AS (
  SELECT
    SUM(gross_order_total)::numeric AS total_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    CASE WHEN COUNT(DISTINCT order_id) = 0 THEN 0
         ELSE SUM(gross_order_total)::numeric / COUNT(DISTINCT order_id) END AS aov
  FROM order_agg
)
SELECT
  pv.total_revenue,
  pv.total_orders,
  pv.aov,
  -- simple period-over-period revenue growth: compare current period to previous period
  CASE WHEN prev.total_revenue IS NULL OR prev.total_revenue = 0 THEN NULL
       ELSE (pv.total_revenue - prev.total_revenue) / prev.total_revenue * 100 END AS revenue_growth_pct
FROM period_values pv
LEFT JOIN (
  -- previous period same length immediately preceding current period
  WITH period_range AS (
    SELECT COALESCE({{start_date}}, current_date) AS start_date, COALESCE({{end_date}}, current_date) AS end_date
  )
  SELECT SUM(gross_order_total) AS total_revenue
  FROM base_line_items bli
  JOIN period_range pr ON 1=1
  JOIN (
    SELECT order_id, SUM(gross_line_total) as gross_order_total
    FROM base_line_items
    WHERE order_date BETWEEN (pr.start_date - (pr.end_date - pr.start_date) - 1)
                        AND (pr.start_date - 1)
    GROUP BY order_id
  ) sub ON 1=1
) prev ON 1=1;
