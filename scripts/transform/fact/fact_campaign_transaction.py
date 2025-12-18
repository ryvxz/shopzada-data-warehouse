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
    # 1. Create Engines
    staging_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}")
    dwh_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}")

    print("Extracting raw data from Staging...")
    query = "SELECT order_id, campaign_id, transaction_date, availed FROM marketing_transactional_campaign_data"
    df = pd.read_sql(query, staging_engine)

    # 2. Load to a Landing Table in DWH
    print("Loading to Landing Table...")
    with dwh_engine.begin() as conn:
        # We use 'replace' to ensure a clean slate in the landing table
        df.to_sql("stg_raw_campaign", conn, if_exists="replace", index=False)
        
        # 3. Use SQL to do the Transformation and Upsert
        print("Transforming and Upserting to Fact Table...")
        # FIX: Changed s.availed::boolean to (s.availed <> 0)
        upsert_sql = """
        INSERT INTO fact_campaign_transaction (
            order_id, sk_campaign, sk_date, availedflag, estimatedarrivaldays
        )
        SELECT 
            s.order_id,
            c.sk_campaign,
            d.sk_date,
            (s.availed <> 0),
            (CURRENT_DATE - s.transaction_date::date)
        FROM stg_raw_campaign s
        LEFT JOIN dim_campaign c ON s.campaign_id = c.campaign_id
        LEFT JOIN dim_date d ON s.transaction_date::date = d.fulldate
        ON CONFLICT (order_id) DO UPDATE SET
            sk_campaign = EXCLUDED.sk_campaign,
            sk_date = EXCLUDED.sk_date,
            availedflag = EXCLUDED.availedflag,
            estimatedarrivaldays = EXCLUDED.estimatedarrivaldays,
            last_updated = CURRENT_TIMESTAMP;
        """
        conn.execute(text(upsert_sql))
        print("FACT_CAMPAIGN_TRANSACTION updated successfully.")

if __name__ == "__main__":
    main()