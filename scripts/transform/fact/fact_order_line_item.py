import pandas as pd
from sqlalchemy import create_engine, text
import os

# Database Connection details
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'shopzada123')
DB_STAGING_HOST = os.getenv('DB_STAGING_HOST', 'db_staging')
DB_DWH_HOST = os.getenv('DB_DWH_HOST', 'db_dwh')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_STAGING_NAME = os.getenv('DB_STAGING_NAME', 'shopzada_staging')
DB_DWH_NAME = os.getenv('DB_DWH_NAME', 'shopzada_dwh')

def main():
    staging_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}")
    dwh_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}")

    print("Step 1: Extracting raw tables...")
    df_orders = pd.read_sql("SELECT order_id, user_id, transaction_date FROM operations_order_data", staging_engine)
    df_merchants = pd.read_sql("SELECT order_id, merchant_id, staff_id FROM enterprise_order_with_merchant_data", staging_engine)
    df_products = pd.read_sql("SELECT order_id, product_id FROM operations_line_item_data_products", staging_engine)
    df_prices = pd.read_sql("SELECT order_id, quantity, price FROM operations_line_item_data_prices", staging_engine)

    print("Step 2: Loading to Landing Area...")
    with dwh_engine.begin() as conn:
        df_orders.to_sql("stg_raw_orders", conn, if_exists="replace", index=False)
        df_merchants.to_sql("stg_raw_merchants_ref", conn, if_exists="replace", index=False)
        df_products.to_sql("stg_raw_products_ref", conn, if_exists="replace", index=False)
        df_prices.to_sql("stg_raw_prices_ref", conn, if_exists="replace", index=False)
        
        print("Step 3: Executing DISTINCT ON Upsert...")
        # DISTINCT ON (order_id) ensures that we only ever propose ONE row per order_id to the INSERT
        upsert_sql = """
        INSERT INTO fact_order_line_item (
            order_id, sk_customer, sk_merchant, sk_staff, 
            sk_product, sk_date, quantitysold, lineitemprice, lineitemtotalamount
        )
        SELECT DISTINCT ON (sub.order_id)
            sub.order_id,
            sub.sk_customer,
            sub.sk_merchant,
            sub.sk_staff,
            sub.sk_product,
            sub.sk_date,
            sub.clean_qty,
            sub.price,
            sub.line_total
        FROM (
            SELECT 
                o.order_id,
                c.sk_customer,
                m.sk_merchant,
                s.sk_staff,
                p.sk_product,
                d.sk_date,
                CAST(NULLIF(regexp_replace(pr.quantity, '[^0-9.]', '', 'g'), '') AS NUMERIC) as clean_qty,
                pr.price::DECIMAL as price,
                (CAST(NULLIF(regexp_replace(pr.quantity, '[^0-9.]', '', 'g'), '') AS NUMERIC) * pr.price::DECIMAL) as line_total
            FROM stg_raw_orders o
            JOIN stg_raw_merchants_ref m_ref ON o.order_id = m_ref.order_id
            JOIN stg_raw_products_ref p_ref ON o.order_id = p_ref.order_id
            JOIN stg_raw_prices_ref pr ON o.order_id = pr.order_id
            LEFT JOIN dim_customer c ON o.user_id = c.customer_id
            LEFT JOIN dim_merchant m ON m_ref.merchant_id = m.merchant_id
            LEFT JOIN dim_staff s ON m_ref.staff_id = s.staff_id
            LEFT JOIN dim_product p ON p_ref.product_id = p.product_id
            LEFT JOIN dim_date d ON o.transaction_date::date = d.fulldate
        ) sub
        ORDER BY sub.order_id -- Required for DISTINCT ON
        ON CONFLICT (order_id) DO UPDATE SET
            quantitysold = EXCLUDED.quantitysold,
            lineitemtotalamount = EXCLUDED.lineitemtotalamount,
            last_updated = CURRENT_TIMESTAMP;
        """
        conn.execute(text(upsert_sql))
        print("FACT_ORDER_LINE_ITEM updated successfully with DISTINCT ON.")

if __name__ == "__main__":
    main()