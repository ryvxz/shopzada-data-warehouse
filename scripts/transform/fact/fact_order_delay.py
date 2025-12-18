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

def load_fact_delay(staging_engine, dwh_engine):
    print("Starting FACT_ORDER_DELAY load...")

    # STEP 1: Transform / Extract from Staging
    # Join with order_data to get the date of the order
    transformation_query = """
    SELECT 
        d.order_id,
        d.delay_in_days,
        o.transaction_date
    FROM operations_order_delays d
    JOIN operations_order_data o ON d.order_id = o.order_id;
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
            df_fact.to_sql("temp_fact_delay", connection, if_exists="replace", index=False)

            insert_query = """
            INSERT INTO fact_order_delay (
                SK_Date, Order_ID, DaysDelayed
            )
            SELECT 
                dt.SK_Date,
                t.order_id,
                t.delay_in_days::INTEGER
            FROM temp_fact_delay t
            LEFT JOIN dim_date dt ON CAST(TO_CHAR(t.transaction_date::DATE, 'YYYYMMDD') AS INTEGER) = dt.SK_Date
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_order_delay f 
                WHERE f.Order_ID = t.order_id
            );
            """
            connection.execute(text(insert_query))
            print("FACT_ORDER_DELAY successfully loaded.")

    except Exception as e:
        print(f"Error during DWH load: {e}")

def main():
    conn_staging = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}"
    conn_dwh = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    staging_engine = create_engine(conn_staging)
    dwh_engine = create_engine(conn_dwh)

    load_fact_delay(staging_engine, dwh_engine)

if __name__ == "__main__":
    main()
