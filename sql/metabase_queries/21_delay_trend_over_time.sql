-- Delay trend over time (month) aggregated

WITH delay_data AS (
  SELECT order_id, "delay in days"::numeric AS delay_in_days
  FROM operations_order_delays
),
order_dates AS (
  SELECT o.order_id, o.transaction_date::date AS order_date
  FROM operations_order_data o
)
SELECT date_trunc('month', od.order_date)::date AS month_start,
       AVG(dd.delay_in_days) AS avg_delay_days,
       COUNT(dd.order_id)::int AS orders_with_delay,
       SUM(CASE WHEN dd.delay_in_days > 0 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(dd.order_id),0) * 100 AS pct_delayed
FROM delay_data dd
JOIN order_dates od ON dd.order_id = od.order_id
WHERE od.order_date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
GROUP BY month_start
ORDER BY month_start;
