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



def load_dim_date(staging_engine,dwh_engine):
    # We define a range (e.g., 2020 to 2030) or use your dynamic range logic.
    # It is safer to generate a wide fixed range (e.g., 5 years past, 5 years future).
    transformation_query = """
    SELECT 
        CAST(TO_CHAR(datum, 'YYYYMMDD') AS INTEGER) AS SK_Date,
        datum AS FullDate,
        TO_CHAR(datum, 'Month') AS MonthName,
        EXTRACT(MONTH FROM datum) AS MonthNumber,
        EXTRACT(QUARTER FROM datum) AS Quarter,
        EXTRACT(YEAR FROM datum) AS Year,
        TO_CHAR(datum, 'Day') AS DayName,
        CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS DayType,
        EXTRACT(WEEK FROM datum) AS WeekOfYear
    FROM generate_series(
        '2020-01-01'::DATE, 
        '2030-12-31'::DATE, 
        '1 day'::interval
    ) datum;
    """

    try:
        with staging_engine.connect() as connection:
            # Execute the query and return the result as a DataFrame
            df_dim = pd.read_sql(text(transformation_query), connection)
            print(f"DIM_DATE transformation success!")
            print(df_dim.head())
        
    except Exception as e:
        print(f"Error during DIM_CAMPAIGN transformation: {e}")
        return None
    
    try:
        with dwh_engine.begin() as conn:
            # 1. Load the transformed data into a temporary table
            df_dim.to_sql("temp_date_sync", conn, if_exists="replace", index=False)

            # 2. PostgreSQL-friendly upsert: Insert new rows and update existing ones
            upsert_query = """
            WITH upsert AS (
                -- Insert new records that don't exist in dim_date
                INSERT INTO dim_date (SK_Date, FullDate, MonthName, MonthNumber, Quarter, Year, DayName, DayType, WeekOfYear)
                SELECT t.SK_Date, t.FullDate, t.MonthName, t.MonthNumber, t.Quarter, t.Year, t.DayName, t.DayType, t.WeekOfYear
                FROM temp_date_sync t
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_date d WHERE d.SK_Date = t.SK_Date
                )
                RETURNING SK_Date  -- Return inserted SK_Date to avoid updating them later
            )
            -- Update existing records if they already exist
            UPDATE dim_date AS d
            SET 
                FullDate = t.FullDate,
                MonthName = t.MonthName,
                MonthNumber = t.MonthNumber,
                Quarter = t.Quarter,
                Year = t.Year,
                DayName = t.DayName,
                DayType = t.DayType,
                WeekOfYear = t.WeekOfYear,
                last_updated = CURRENT_TIMESTAMP
            FROM temp_date_sync t
            WHERE d.SK_Date = t.SK_Date
            AND NOT EXISTS (SELECT 1 FROM upsert u WHERE u.SK_Date = d.SK_Date);
            """

            # Execute the upsert SQL query
            conn.execute(text(upsert_query))
            print("DIM_DATE successfully synchronized.")

    except Exception as e:
        print(f"Error during DIM_DATE load: {e}")

    
def main():
    connection_staging_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}"
    staging_engine = create_engine(connection_staging_string)
    connection_dwh_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    dwh_engine = create_engine(connection_dwh_string)

    load_dim_date(staging_engine,dwh_engine)


if __name__ == "__main__":
    main()