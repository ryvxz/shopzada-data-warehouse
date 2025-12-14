-- Average Delivery Delay per Merchant
-- Uses operations_order_delays and enterprise_order_with_merchant_data

WITH delay_data AS (
  SELECT od.order_id, od."delay in days"::numeric AS delay_in_days
  FROM operations_order_delays od
),
order_merchant AS (
  SELECT ow.order_id, mm.merchant_id, mm.name AS merchant_name
  FROM enterprise_order_with_merchant_data ow
  LEFT JOIN enterprise_merchant_data mm ON ow.merchant_id = mm.merchant_id
),
joined AS (
  SELECT dm.order_id, dm.delay_in_days, om.merchant_id, om.merchant_name
  FROM delay_data dm
  JOIN order_merchant om ON dm.order_id = om.order_id
  WHERE dm.order_id IS NOT NULL
)
SELECT merchant_id, COALESCE(merchant_name,'Unknown') AS merchant_name,
       AVG(delay_in_days) AS avg_delay_days,
       SUM(CASE WHEN delay_in_days > 0 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) * 100 AS pct_delayed_orders,
       COUNT(*)::int AS total_orders_with_delay
FROM joined
GROUP BY merchant_id, merchant_name
ORDER BY avg_delay_days DESC
LIMIT 200;
