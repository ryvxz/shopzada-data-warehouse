import pandas as pd
from sqlalchemy import text

def transform_staff(engine):
    transformation_query = """
    SELECT 
        -- SK_Staff (PK): System Generated Surrogate Key
        ROW_NUMBER() OVER (ORDER BY staff_id) AS "SK_Staff",
        
        -- staff_id (Natural Key): Direct Map
        staff_id,
        
        -- StaffName: Direct Map
        name AS "StaffName",
        
        -- JobLevel: Direct Map
        job_level AS "JobLevel",
        
        -- HireDate: Mapped from creation_date
        -- Casting to DATE ensures it matches the target dimension type
        creation_date::DATE AS "HireDate"
        
    FROM enterprise_staff_data;
    """
    
    try:
        with engine.connect() as connection:
            # Execute the query and return the result as a DataFrame
            df_dim = pd.read_sql(text(transformation_query), connection)
        
        return df_dim
        
    except Exception as e:
        print(f"Error during DIM_STAFF transformation: {e}")
        return None