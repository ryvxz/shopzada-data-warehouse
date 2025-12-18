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

def load_dim_merchant(staging_engine,dwh_engine):
    # STEP 1: Transform - Get clean data without generating an SK in Python
    transformation_query = """
    SELECT 
        merchant_id, 
        name AS MerchantName, 
        creation_date AS CreationDate,
        street AS StreetAddress, 
        city AS City, 
        state AS State, 
        country AS Country, 
        contact_number AS ContactNumber
    FROM enterprise_merchant_data;
    """

    try:
        with staging_engine.connect() as connection:
            # Execute the query and return the result as a DataFrame
            df_dim = pd.read_sql(text(transformation_query), connection)
            df_dim = df_dim.drop_duplicates(subset=['merchant_id'], keep='first')
            print(f"DIM_MERCHANT transformation success!")
            print(df_dim.head())
        
    except Exception as e:
        print(f"Error during DIM_CAMPAIGN transformation: {e}")
        return None
    
    try:
        with dwh_engine.begin() as conn:
            # 1. Upload the transformed data into a temporary staging table
            df_dim.to_sql("temp_merchant_sync", conn, if_exists="replace", index=False)

            # 2. PostgreSQL-friendly upsert: Insert new rows and update existing ones
            upsert_query = """
            WITH upsert AS (
                -- Insert new records that don't exist in dim_merchant
                INSERT INTO dim_merchant (merchant_id, MerchantName, CreationDate, StreetAddress, City, State, Country, ContactNumber)
                SELECT t.merchant_id, t.MerchantName, t.CreationDate, t.StreetAddress, t.City, t.State, t.Country, t.ContactNumber
                FROM temp_merchant_sync t
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_merchant d WHERE d.merchant_id = t.merchant_id
                )
                RETURNING merchant_id  -- Return inserted merchant_id to avoid updating them later
            )
            -- Update existing records if they already exist
            UPDATE dim_merchant AS d
            SET 
                MerchantName = t.MerchantName,
                StreetAddress = t.StreetAddress,
                City = t.City,
                State = t.State,
                ContactNumber = t.ContactNumber,
                last_updated = CURRENT_TIMESTAMP
            FROM temp_merchant_sync t
            WHERE d.merchant_id = t.merchant_id
            AND NOT EXISTS (SELECT 1 FROM upsert u WHERE u.merchant_id = d.merchant_id);
            """

            # Execute the upsert SQL query
            conn.execute(text(upsert_query))
            print("DIM_MERCHANT successfully synchronized.")

    except Exception as e:
        print(f"Error during DIM_MERCHANT load: {e}")


def main():
    connection_staging_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}"
    staging_engine = create_engine(connection_staging_string)
    connection_dwh_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    dwh_engine = create_engine(connection_dwh_string)

    load_dim_merchant(staging_engine,dwh_engine)


if __name__ == "__main__":
    main()