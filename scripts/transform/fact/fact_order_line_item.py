import pandas as pd
from sqlalchemy import create_engine, text
import os

# 1. Define your connection details
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_STAGING_HOST = os.getenv('DB_STAGING_HOST') 
DB_DWH_HOST = os.getenv('DB_DWH_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_STAGING_NAME = os.getenv('DB_STAGING_NAME')
DB_DWH_NAME = os.getenv('DB_DWH_NAME')

def load_fact_order_line_item(staging_engine, dwh_engine):
    print("Starting FACT_ORDER_LINE_ITEM load...")

    # STEP 1: Transform / Extract from Staging
    # We use ROW_NUMBER to join Products and Prices cleanly (avoiding duplicates)
    transformation_query = """
    WITH prod_cte AS (
        SELECT order_id, product_id, 
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY product_id) as rn 
        FROM operations_line_item_data_products
    ),
    price_cte AS (
        SELECT order_id, quantity, price, 
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY price) as rn 
        FROM operations_line_item_data_prices
    )
    SELECT 
        o.order_id,
        o.user_id,
        prod.product_id,
        map.merchant_id,
        map.staff_id,
        o.transaction_date,
        price.quantity::INTEGER as quantity,
        price.price::DECIMAL(10,2) as price
    FROM operations_order_data o
    JOIN prod_cte prod ON o.order_id = prod.order_id
    JOIN price_cte price ON o.order_id = price.order_id AND prod.rn = price.rn
    LEFT JOIN enterprise_order_with_merchant_data map ON o.order_id = map.order_id;
    """

    try:
        with staging_engine.connect() as connection:
            df_fact = pd.read_sql(text(transformation_query), connection)
            print(f"Extracted {len(df_fact)} rows from Staging.")
    except Exception as e:
        print(f"Error during extraction: {e}")
        return

    # STEP 2: Load to Temp & Insert to DWH
    try:
        with dwh_engine.begin() as connection:
            # Load into temporary table
            df_fact.to_sql("temp_fact_lines", connection, if_exists="replace", index=False)

            # Insert into Fact Table (Looking up SKs from Dimensions)
            # We use NOT EXISTS to prevent duplicates if run multiple times
            insert_query = """
            INSERT INTO fact_order_line_item (
                SK_Date, SK_Customer, SK_Product, SK_Merchant, SK_Staff, 
                Order_ID, QuantitySold, LineItemPrice, LineItemTotalAmount
            )
            SELECT 
                d.SK_Date,
                c.SK_Customer,
                p.SK_Product,
                m.SK_Merchant,
                s.SK_Staff,
                t.order_id,
                t.quantity,
                t.price,
                (t.quantity * t.price) as LineItemTotalAmount
            FROM temp_fact_lines t
            LEFT JOIN dim_date d ON CAST(TO_CHAR(t.transaction_date::DATE, 'YYYYMMDD') AS INTEGER) = d.SK_Date
            LEFT JOIN dim_customer c ON t.user_id = c.customer_id
            LEFT JOIN dim_product p ON t.product_id = p.product_id
            LEFT JOIN dim_merchant m ON t.merchant_id = m.merchant_id
            LEFT JOIN dim_staff s ON t.staff_id = s.staff_id
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_order_line_item f 
                WHERE f.Order_ID = t.order_id 
                AND f.SK_Product = p.SK_Product
            );
            """
            connection.execute(text(insert_query))
            print("FACT_ORDER_LINE_ITEM successfully loaded.")

    except Exception as e:
        print(f"Error during DWH load: {e}")

def main():
    conn_staging = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}"
    conn_dwh = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    staging_engine = create_engine(conn_staging)
    dwh_engine = create_engine(conn_dwh)

    load_fact_order_line_item(staging_engine, dwh_engine)

if __name__ == "__main__":
    main()
