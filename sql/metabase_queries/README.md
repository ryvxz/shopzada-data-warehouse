Metabase SQL Queries for ShopZada BI

Folder: sql/metabase_queries/

Usage:
- Each .sql file contains a query you can paste into Metabase's SQL editor.
- Replace table names with your final fact/dim (e.g., fact_order_line_item) after ETL finishes.
- The standard Metabase date variables used: {{start_date}}, {{end_date}}. Ensure their type is set to Date.
- For granularity parameter in time series, set a text variable {{granularity}} with values 'day', 'week', 'month'.

How to use the base CTE:
- The file 00_base_line_item_cte.sql contains a common CTE you can paste at top of other queries, or you can turn it into a DB view named vw_order_line_item_flat.

Queries:
- 01_revenue_kpis.sql: KPIs (Total Revenue, Total Orders, AOV, Revenue Growth %)
- 02_revenue_by_product_category.sql: Bar chart - revenue by product type
- 03_revenue_by_merchant.sql: Bar chart - revenue by merchant
- 04_revenue_time_series.sql: Line chart - revenue over time; uses {{granularity}}
- 05_revenue_campaign_performance.sql: Campaign influence and availed vs non
- 06_revenue_by_customer_type.sql: Revenue by New vs Returning

- 10_top_customers.sql: Top customers by total spend
- 11_top_customer_segments.sql: Top segments by job_title (or modify for issuing bank, region)
- 12_repeat_purchase_metrics.sql: Repeat purchase rate and repeat spend
- 13_customer_spend_trend.sql: Customer spend trend by month split by New/Returning
- 14_customer_retention_funnel.sql: Funnel counts for 1+, 2+, 3+ orders

- 20_avg_delay_by_merchant.sql: Avg delivery delay per merchant
- 21_delay_trend_over_time.sql: Delay trend over time
- 22_return_and_cancellation_notes.sql: Template + note about missing return/cancellation fields
- 23_merchant_ops_customer_correlation.sql: Merchant metrics with repeat rate correlation

Notes & Caveats:
- Cancellations and returns are not present in the current staging. You need to add fields or tables capturing order_status (canceled) or returns data to compute rates.
- Discount fields in marketing_campaign_data are noisy (strings like '1pct', '1%', '1percent', '10%%'). SQL uses a regex-based parser; confirm discount semantics and whether discounts apply at order or line level.
- The 'availed' flag indicates whether a campaign was used on an order; we apply discount proportionally to all lines on that order as a simple approach. For precise allocation, ETL should provide discount allocation fields or coupon-level directives.
- New vs Returning classification uses first order date; you can adjust the rule if you want first-order-month vs new within analysis period.
- After ETL completes, consider converting heavy computed fields (regex parsing, repeated calculations) into preprocessed columns in the fact tables for performance.

Next steps for production readiness:
1. Convert base CTE into a DB view `vw_order_line_item_flat` with parsed columns (qty_int, gross_line_total, net_revenue, discount_pct).
2. Replace staging table names with final `fact_*` and `dim_*` tables; use surrogate keys in joins.
3. Add or map cancellation/returns data into facts for Merchant performance KPIs.
4. Validate metrics with stakeholders (revenue, AOV, discount amounts) and adjust discount allocation rules.
5. Use Tableau/Metabase native relationships as needed and schedule refreshes/ extracts.

If you'd like me to create DB views from the CTEs or add more specialized queries, tell me which queries to prioritize and if you want the queries saved to specific DB views in the repository.
