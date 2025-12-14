-- Merchant operational metrics with repeat rate for correlation analysis
-- Fields: merchant_id, merchant_name, avg_delay_days, pct_delayed, total_orders, repeat_rate (by merchant customers)

WITH orders AS (
  SELECT o.order_id, o.user_id, o.transaction_date::date AS order_date
  FROM operations_order_data o
  WHERE o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
),
merchant_map AS (
  SELECT ow.order_id, mm.merchant_id, mm.name AS merchant_name
  FROM enterprise_order_with_merchant_data ow
  LEFT JOIN enterprise_merchant_data mm ON ow.merchant_id = mm.merchant_id
),
delays AS (
  SELECT dd.order_id, dd."delay in days"::numeric AS delay_in_days
  FROM operations_order_delays dd
),
order_merchant AS (
  SELECT o.order_id, o.user_id, om.merchant_id, om.merchant_name, o.order_date
  FROM orders o
  JOIN merchant_map om ON o.order_id = om.order_id
),
merchant_delays AS (
  SELECT om.merchant_id, om.merchant_name, od.delay_in_days
  FROM order_merchant om
  LEFT JOIN delays od ON om.order_id = od.order_id
),
merchant_orders AS (
  SELECT om.merchant_id, om.merchant_name, om.user_id, COUNT(DISTINCT om.order_id) AS orders_by_user
  FROM order_merchant om
  GROUP BY om.merchant_id, om.merchant_name, om.user_id
),
merchant_summary AS (
  SELECT md.merchant_id, md.merchant_name,
         AVG(md.delay_in_days) AS avg_delay_days,
         SUM(CASE WHEN md.delay_in_days > 0 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(md.delay_in_days),0) * 100 AS pct_delayed_orders,
         COUNT(DISTINCT om.order_id) AS total_orders
  FROM merchant_delays md
  JOIN order_merchant om ON md.order_id = om.order_id
  GROUP BY md.merchant_id, md.merchant_name
),
merchant_repeat AS (
  SELECT merchant_id,
         COUNT(*) FILTER (WHERE orders_by_user > 1) AS repeat_customers,
         COUNT(*) AS unique_customers,
         COUNT(*) FILTER (WHERE orders_by_user > 1)::numeric / NULLIF(COUNT(*)::numeric,0) * 100 AS repeat_rate_pct
  FROM merchant_orders
  GROUP BY merchant_id
)
SELECT ms.merchant_id, ms.merchant_name, ms.avg_delay_days, ms.pct_delayed_orders, ms.total_orders,
       mr.unique_customers, mr.repeat_customers, mr.repeat_rate_pct
FROM merchant_summary ms
LEFT JOIN merchant_repeat mr ON ms.merchant_id = mr.merchant_id
ORDER BY ms.avg_delay_days DESC
LIMIT 200;
