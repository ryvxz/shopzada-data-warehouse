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

def load_dim_product(staging_engine,dwh_engine):
    # 1. Transform: Only get the data and rename columns. No SK generation here!
    transformation_query = """
    SELECT 
        product_id,
        product_name AS ProductName,
        product_type AS ProductType,
        price AS ProductPrice
    FROM business_product_list;
    """
    try:
        with staging_engine.connect() as connection:
            # Execute the query and return the result as a DataFrame
            df_dim = pd.read_sql(text(transformation_query), connection)
            print(f"DIM_PRODUCT transformation success!")
            df_dim = df_dim.drop_duplicates(subset=['product_id'], keep='first')
            print(df_dim.head())
        
    except Exception as e:
        print(f"Error during DIM_CAMPAIGN transformation: {e}")
        return None
    
    try:
        with dwh_engine.begin() as conn:
            # 1. Upload the transformed data into a temporary staging table
            df_dim.to_sql("temp_product_sync", conn, if_exists="replace", index=False)

            # 2. PostgreSQL-friendly upsert: Insert new rows and update existing ones
            upsert_query = """
            WITH upsert AS (
                -- Insert new records that don't exist in dim_product
                INSERT INTO dim_product (product_id, ProductName, ProductType, ProductPrice)
                SELECT t.product_id, t.ProductName, t.ProductType, t.ProductPrice
                FROM temp_product_sync t
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_product d WHERE d.product_id = t.product_id
                )
                RETURNING product_id  -- Return inserted product_id to avoid updating them later
            )
            -- Update existing records if they already exist
            UPDATE dim_product AS d
            SET 
                ProductName = t.ProductName,
                ProductPrice = t.ProductPrice,
                last_updated = CURRENT_TIMESTAMP
            FROM temp_product_sync t
            WHERE d.product_id = t.product_id
            AND NOT EXISTS (SELECT 1 FROM upsert u WHERE u.product_id = d.product_id);
            """

            # Execute the upsert SQL query
            conn.execute(text(upsert_query))
            print("DIM_PRODUCT successfully synchronized.")

    except Exception as e:
        print(f"Error during DIM_PRODUCT load: {e}")


def main():
    connection_staging_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}"
    staging_engine = create_engine(connection_staging_string)
    connection_dwh_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    dwh_engine = create_engine(connection_dwh_string)

    load_dim_product(staging_engine,dwh_engine)


if __name__ == "__main__":
    main()