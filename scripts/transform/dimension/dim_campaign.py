import pandas as pd
from sqlalchemy import create_engine, text
import os

# 1. Define your connection details for staging layer
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_STAGING_HOST = os.getenv('DB_STAGING_HOST') 
DB_DWH_HOST = os.getenv('DB_DWH_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_STAGING_NAME = os.getenv('DB_STAGING_NAME')
DB_DWH_NAME = os.getenv('DB_DWH_NAME')


def load_dim_campaign(staging_engine,dwh_engine):
    transformation_query = """
    SELECT ROW_NUMBER() OVER (ORDER BY campaign_id) AS SK_Campaign,
           campaign_id, 
           campaign_name AS CampaignName, 
           campaign_description AS CampaignDescription, 
           discount AS CampaignDiscountRate
    FROM marketing_campaign_data;
    """
    
    try:
        with staging_engine.connect() as connection:
            # Execute the query and return the result as a DataFrame
            df_dim = pd.read_sql(text(transformation_query), connection)
            df_dim = df_dim.drop_duplicates(subset=['campaign_id'], keep='first')
            print(f"DIM_CAMPAIGN transformation success!")
            print(df_dim.head())
        
    except Exception as e:
        print(f"Error during DIM_CAMPAIGN transformation: {e}")
        return None

    try:
        with dwh_engine.begin() as conn:
            # Load the transformed data into the temp table
            df_dim.to_sql("temp_campaign_sync", conn, if_exists="replace", index=False)

            # PostgreSQL-friendly upsert query
            sql_upsert = """
            WITH upsert AS (
                -- Insert new records from staging table into the dim_campaign table
                INSERT INTO dim_campaign (campaign_id, CampaignName, CampaignDescription, CampaignDiscountRate)
                SELECT s.campaign_id, s.CampaignName, s.CampaignDescription, s.CampaignDiscountRate
                FROM temp_campaign_sync s
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_campaign d WHERE d.campaign_id = s.campaign_id
                )
                RETURNING campaign_id  -- Return inserted campaign_id to avoid updating those rows
            )
            -- Update existing records that are not in the upserted rows (prevent re-updating newly inserted records)
            UPDATE dim_campaign AS d
            SET CampaignName = s.CampaignName,
                CampaignDiscountRate = s.CampaignDiscountRate,
                last_updated = CURRENT_TIMESTAMP
            FROM temp_campaign_sync s
            WHERE d.campaign_id = s.campaign_id
            AND NOT EXISTS (SELECT 1 FROM upsert u WHERE u.campaign_id = d.campaign_id);
            """
            
            # Execute the upsert SQL
            conn.execute(text(sql_upsert))
        
        print("DIM_CAMPAIGN successfully synchronized.")
        
    except Exception as e:
        print(f"Error during DIM_CAMPAIGN load: {e}")


def main():
    connection_staging_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}"
    staging_engine = create_engine(connection_staging_string)
    connection_dwh_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    dwh_engine = create_engine(connection_dwh_string)

    load_dim_campaign(staging_engine,dwh_engine)

if __name__ == "__main__":
    main()