import os
from sqlalchemy import create_engine, text

# Database Connection details
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'shopzada123')
DB_DWH_HOST = os.getenv('DB_DWH_HOST', 'db_dwh')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_DWH_NAME = os.getenv('DB_DWH_NAME', 'shopzada_dwh')

def main():
    # 1. Connect to the Data Warehouse
    dwh_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}")

    # 2. Define the list of tables to drop based on your image
    # These are the intermediate tables that should not be permanent
    tables_to_clean = [
        "stg_raw_campaign",
        "stg_raw_merchants_ref",
        "stg_raw_order_delay",
        "stg_raw_orders",
        "stg_raw_prices_ref",
        "stg_raw_products_ref",
        "temp_campaign_sync",
        "temp_customer_sync",
        "temp_date_sync",
        "temp_merchant_sync",
        "temp_product_sync",
        "temp_staff_sync"
    ]

    print(f"Starting cleanup of {len(tables_to_clean)} temporary tables...")

    with dwh_engine.begin() as conn:
        for table in tables_to_clean:
            try:
                # 'CASCADE' ensures that if any views depend on these, they are handled
                conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
                print(f"Successfully dropped: {table}")
            except Exception as e:
                print(f"Error dropping {table}: {e}")

    print("Warehouse cleanup complete. Only Dim and Fact tables remain.")

if __name__ == "__main__":
    main()