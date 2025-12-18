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

def load_dim_customer(staging_engine,dwh_engine):
    
    transformation_query = """
    SELECT 
        d.user_id AS customer_id,
        d.name AS CustomerName,
        j.job_title AS JobTitle,
        j.job_level AS JobLevel,
        c.issuing_bank AS CreditCardType
    FROM customer_user_data d
    LEFT JOIN customer_user_job j ON d.user_id = j.user_id
    LEFT JOIN customer_user_credit_card c ON d.user_id = c.user_id;
    """
    try:
        with staging_engine.connect() as connection:
            # Execute the query and return the result as a DataFrame
            df_dim = pd.read_sql(text(transformation_query), connection)
            print(f"DIM_CUSTOMER transformation success!")
            df_dim = df_dim.drop_duplicates(subset=['customer_id'], keep='first')
            print(df_dim.head())
    except Exception as e:
        print(f"Error during DIM_CAMPAIGN transformation: {e}")
        return None
    
    try:
        with dwh_engine.begin() as conn:
            # 1. Load the joined data into a temporary table
            df_dim.to_sql("temp_customer_load", conn, if_exists="replace", index=False)

            # 2. PostgreSQL-friendly upsert: Insert new rows and update existing ones
            upsert_query = """
            WITH upsert AS (
                -- Insert new records that don't exist in dim_customer
                INSERT INTO dim_customer (customer_id, CustomerName, JobTitle, JobLevel, CreditCardType)
                SELECT t.customer_id, t.CustomerName, t.JobTitle, t.JobLevel, t.CreditCardType
                FROM temp_customer_load t
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_customer d WHERE d.customer_id = t.customer_id
                )
                RETURNING customer_id  -- Return inserted customer_id to avoid updating them later
            )
            -- Update existing records if they already exist
            UPDATE dim_customer AS d
            SET 
                CustomerName = t.CustomerName,
                JobTitle = t.JobTitle,
                JobLevel = t.JobLevel,
                CreditCardType = t.CreditCardType,
                last_updated = CURRENT_TIMESTAMP
            FROM temp_customer_load t
            WHERE d.customer_id = t.customer_id
            AND NOT EXISTS (SELECT 1 FROM upsert u WHERE u.customer_id = d.customer_id);
            """

            # Execute the upsert SQL query
            conn.execute(text(upsert_query))
            print("DIM_CUSTOMER successfully synchronized.")

    except Exception as e:
        print(f"Error: {e}")


def main():
    connection_staging_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}"
    staging_engine = create_engine(connection_staging_string)
    connection_dwh_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    dwh_engine = create_engine(connection_dwh_string)

    load_dim_customer(staging_engine,dwh_engine)


if __name__ == "__main__":
    main()