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

def load_dim_staff(staging_engine,dwh_engine):
    # STEP 1: Transform (Clean and Format)
    # We pull from staging, but we DON'T generate an SK here.
    transformation_query = """
    SELECT 
        staff_id, 
        name AS StaffName, 
        job_level AS JobLevel, 
        creation_date::DATE AS HireDate,
        state AS State,
        city AS City,
        street AS Street
    FROM enterprise_staff_data;
    """
    try:
        with staging_engine.connect() as connection:
            # Execute the query and return the result as a DataFrame
            df_dim = pd.read_sql(text(transformation_query), connection)
            print(f"DIM_STAFF transformation success!")
            df_dim = df_dim.drop_duplicates(subset=['staff_id'], keep='first')
            print(df_dim.head())
        
    except Exception as e:
        print(f"Error during DIM_CAMPAIGN transformation: {e}")
        return None
    
    try:
        with dwh_engine.begin() as conn:
            # 1. Load transformation into a temporary work table
            df_dim.to_sql("temp_staff_load", conn, if_exists="replace", index=False)

            # 2. PostgreSQL-friendly upsert: Insert new rows and update existing ones
            upsert_query = """
            WITH upsert AS (
                -- Insert new records that don't exist in dim_staff
                INSERT INTO dim_staff (staff_id, StaffName, JobLevel, HireDate, State, City, Street)
                SELECT t.staff_id, t.StaffName, t.JobLevel, t.HireDate, t.State, t.City, t.Street
                FROM temp_staff_load t
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_staff d WHERE d.staff_id = t.staff_id
                )
                RETURNING staff_id  -- Return inserted staff_id to avoid updating them later
            )
            -- Update existing records if they already exist
            UPDATE dim_staff AS d
            SET 
                StaffName = t.StaffName,
                JobLevel = t.JobLevel,
                State = t.State,
                City = t.City,
                Street = t.Street,
                last_updated = CURRENT_TIMESTAMP
            FROM temp_staff_load t
            WHERE d.staff_id = t.staff_id
            AND NOT EXISTS (SELECT 1 FROM upsert u WHERE u.staff_id = d.staff_id);
            """

            # Execute the upsert SQL query
            conn.execute(text(upsert_query))
            print("DIM_STAFF successfully synchronized.")

    except Exception as e:
        print(f"Error during DIM_STAFF load: {e}")


def main():
    connection_staging_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_STAGING_HOST}:{DB_PORT}/{DB_STAGING_NAME}"
    staging_engine = create_engine(connection_staging_string)
    connection_dwh_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    dwh_engine = create_engine(connection_dwh_string)

    load_dim_staff(staging_engine,dwh_engine)


if __name__ == "__main__":
    main()

