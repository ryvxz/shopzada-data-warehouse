-- NOTE: Cancellation and Return Rates
-- The current dataset does not include explicit cancellation or return flags in the staging reports.
-- If you have an orders/status column (e.g., 'cancelled' or 'returned') add the table/field into the query below.

-- Example template if you had "order_status" in operations_order_data:
-- SELECT mm.merchant_id, mm.name AS merchant_name,
--        SUM(CASE WHEN o.order_status = 'cancelled' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(o.order_id),0) * 100 AS cancellation_rate_pct
-- FROM operations_order_data o
-- JOIN enterprise_order_with_merchant_data om ON o.order_id = om.order_id
-- JOIN enterprise_merchant_data mm ON om.merchant_id = mm.merchant_id
-- WHERE o.transaction_date::date BETWEEN COALESCE({{start_date}}, '1900-01-01') AND COALESCE({{end_date}}, current_date)
-- GROUP BY mm.merchant_id, mm.name
-- ORDER BY cancellation_rate_pct DESC;

-- If returns are tracked via a separate table (e.g., operations_order_returns), join and compute similarly:
-- SELECT p.product_id, b.product_type, SUM(CASE WHEN r.reason IS NOT NULL THEN 1 ELSE 0 END) / COUNT(p.order_id) AS return_rate
-- FROM operations_line_item_data_products p
-- LEFT JOIN operations_order_returns r ON p.order_id = r.order_id
-- LEFT JOIN business_product_list b ON p.product_id = b.product_id
-- GROUP BY p.product_id, b.product_type

-- Add these tables to the ETL or create an ingestion mapping so we can compute cancellation/return KPIs.
