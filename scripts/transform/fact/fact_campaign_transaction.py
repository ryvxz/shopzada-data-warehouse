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

def load_fact_campaign(staging_engine, dwh_engine):
    print("Starting FACT_CAMPAIGN_TRANSACTION load...")

    # STEP 1: Transform / Extract from Staging
    transformation_query = """
    SELECT 
        transaction_date,
        campaign_id,
        order_id,
        estimated_arrival,
        availed
    FROM marketing_transactional_campaign_data;
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
        with dwh_engine.begin() as conn:
            df_fact.to_sql("temp_fact_campaign", conn, if_exists="replace", index=False)

            insert_query = """
            INSERT INTO fact_campaign_transaction (
                SK_Date, SK_Campaign, Order_ID, AvailedFlag, EstimatedArrivalDays
            )
            SELECT 
                d.SK_Date,
                c.SK_Campaign,
                t.order_id,
                CASE WHEN t.availed = 'True' THEN 1 ELSE 0 END, 
                CAST(NULLIF(regexp_replace(t.estimated_arrival, '\D', '', 'g'), '') AS INTEGER)
            FROM temp_fact_campaign t
            LEFT JOIN dim_date d ON CAST(TO_CHAR(t.transaction_date::DATE, 'YYYYMMDD') AS INTEGER) = d.SK_Date
            LEFT JOIN dim_campaign c ON t.campaign_id = c.campaign_id
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_campaign_transaction f 
                WHERE f.Order_ID = t.order_id 
                AND f.SK_Campaign = c.SK_Campaign
            );
            """
            conn.execute(text(insert_query))
            print("FACT_CAMPAIGN_TRANSACTION successfully loaded.")

    except Exception as e:
        print(f"Error during DWH load: {e}")

def main():
    conn_staging = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}"
    conn_dwh = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    staging_engine = create_engine(conn_staging)
    dwh_engine = create_engine(conn_dwh)

    load_fact_campaign(staging_engine, dwh_engine)

if __name__ == "__main__":
    main()
