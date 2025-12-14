-- Revenue Over Time (daily/weekly/monthly) - parameterize granularity
-- Metabase variable: {{granularity}} with values: day|week|month
-- Defaults to month

WITH base_line_items AS (
  SELECT p.order_id, pr.price::numeric AS unit_price, COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1) AS qty,
         (pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int,1)) AS gross_line_total,
         o.transaction_date::date AS order_date
  FROM operations_line_item_data_products p
  JOIN operations_line_item_data_prices pr ON p.order_id = pr.order_id
  JOIN operations_order_data o ON p.order_id = o.order_id
  WHERE o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
),
series AS (
  SELECT
    CASE WHEN '{{granularity}}' = 'day' THEN order_date
         WHEN '{{granularity}}' = 'week' THEN date_trunc('week', order_date)::date
         ELSE date_trunc('month', order_date)::date END AS period_start,
    gross_line_total
  FROM base_line_items
)
SELECT period_start AS "date",
       SUM(gross_line_total) AS total_revenue,
       COUNT(DISTINCT order_id) AS total_orders,
       SUM(gross_line_total) / NULLIF(COUNT(DISTINCT order_id),0) AS aov
FROM (
  SELECT period_start, gross_line_total, order_id
  FROM (
    SELECT order_id, period_start, gross_line_total FROM (
      SELECT p.order_id, p.gross_line_total,
             CASE WHEN '{{granularity}}' = 'day' THEN p.order_date
                  WHEN '{{granularity}}' = 'week' THEN date_trunc('week', p.order_date)::date
                  ELSE date_trunc('month', p.order_date)::date END AS period_start
      FROM (
        SELECT p.order_id, p.gross_line_total, p.order_date
        FROM (
          SELECT p.order_id, pr.price::numeric * COALESCE(NULLIF(regexp_replace(pr.quantity,'[^0-9]','', 'g'),'')::int, 1) AS gross_line_total, o.transaction_date::date AS order_date
          FROM operations_line_item_data_products p
          JOIN operations_line_item_data_prices pr ON p.order_id = pr.order_id
          JOIN operations_order_data o ON p.order_id = o.order_id
          WHERE o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
        ) pr
      ) p
    ) s
  ) f
) src
GROUP BY period_start
ORDER BY period_start;
