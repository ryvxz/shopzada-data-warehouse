import pandas as pd
from sqlalchemy import text

def transform_date(engine):
    """
    Generates a Date Dimension based on the range of dates found 
    across transaction, merchant, and staff data.
    """
    transformation_query = """
    WITH date_range AS (
        -- Find the absolute min and max dates across all relevant tables
        SELECT 
            MIN(min_date)::DATE as start_date, 
            MAX(max_date)::DATE as end_date
        FROM (
            SELECT MIN(transaction_data) as min_date, MAX(transaction_data) as max_date FROM operations_order_data
            UNION ALL
            SELECT MIN(creation_date), MAX(creation_date) FROM enterprise_merchant_data
            UNION ALL
            SELECT MIN(creation_date), MAX(creation_date) FROM enterprise_staff_data
        ) sub
    ),
    all_dates AS (
        -- Generate one row for every day between the min and max
        SELECT 
            generate_series(start_date, end_date, '1 day'::interval)::DATE AS FullDate
        FROM date_range
    )
    SELECT 
        -- SK_Date: Integer Format (YYYYMMDD) is standard for Date Dimensions
        CAST(TO_CHAR(FullDate, 'YYYYMMDD') AS INTEGER) AS "SK_Date",
        
        -- FullDate: The actual date object
        FullDate AS "FullDate",
        
        -- Useful extras often included in DIM_DATE
        EXTRACT(YEAR FROM FullDate) AS "Year",
        EXTRACT(MONTH FROM FullDate) AS "Month",
        TO_CHAR(FullDate, 'Month') AS "MonthName",
        EXTRACT(DAY FROM FullDate) AS "Day",
        TO_CHAR(FullDate, 'Day') AS "DayOfWeek"
        
    FROM all_dates;
    """
    
    try:
        with engine.connect() as connection:
            df_dim = pd.read_sql(text(transformation_query), connection)
        
        return df_dim
        
    except Exception as e:
        print(f"Error during DIM_DATE transformation: {e}")
        return None