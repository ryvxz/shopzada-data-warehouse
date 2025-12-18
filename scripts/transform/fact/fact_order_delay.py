import pandas as pd
from sqlalchemy import create_engine, text
import os

# Database Connection details from Environment Variables
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'shopzada123')
DB_STAGING_HOST = os.getenv('DB_STAGING_HOST', 'db_staging')
DB_DWH_HOST = os.getenv('DB_DWH_HOST', 'db_dwh')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_STAGING_NAME = os.getenv('DB_STAGING_NAME', 'shopzada_staging')
DB_DWH_NAME = os.getenv('DB_DWH_NAME', 'shopzada_dwh')

def main():
    # 1. Create Engines for both databases
    staging_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}")
    dwh_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}")

    print("Step 1: Extracting order delay data from Staging...")
    # We join with the main order table in staging to get the transaction_date for our dim_date mapping
    query = """
    SELECT 
        od.order_id, 
        od."delay in days" as delay_days, 
        o.transaction_date
    FROM operations_order_delays od
    JOIN operations_order_data o ON od.order_id = o.order_id
    """
    df = pd.read_sql(query, staging_engine)

    # 2. Load to a Landing Table in DWH
    print("Step 2: Loading to Landing Table in DWH...")
    with dwh_engine.begin() as conn:
        # We use 'replace' to ensure the staging table is fresh every time
        df.to_sql("stg_raw_order_delay", conn, if_exists="replace", index=False)
        
        # 3. Use SQL to perform the Upsert into the Fact table
        print("Step 3: Transforming and Upserting to Fact Table...")
        upsert_sql = """
        INSERT INTO fact_order_delay (
            order_id, 
            daysdelayed, 
            sk_date
        )
        SELECT 
            s.order_id,
            s.delay_days::INT,
            d.sk_date
        FROM stg_raw_order_delay s
        LEFT JOIN dim_date d ON s.transaction_date::date = d.fulldate
        ON CONFLICT (order_id) DO UPDATE SET
            daysdelayed = EXCLUDED.daysdelayed,
            sk_date = EXCLUDED.sk_date,
            last_updated = CURRENT_TIMESTAMP;
        """
        conn.execute(text(upsert_sql))
        print("FACT_ORDER_DELAY updated successfully.")

if __name__ == "__main__":
    main()