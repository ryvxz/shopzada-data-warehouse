import pandas as pd
from sqlalchemy import text

def transform_merchant(engine):
    transformation_query = """
    SELECT 
        -- Surrogate Key (System Generated)
        ROW_NUMBER() OVER (ORDER BY merchant_id) AS "SK_Merchant",
        
        -- Natural Key & Direct Mapping
        merchant_id,
        name AS "MerchantName",
        
        -- CreationDate: Direct Map
        creation_date AS "CreationDate",
        
        -- Address Components: Direct Map
        street AS "StreetAddress",
        city AS "City",
        state AS "State",
        country AS "Country",
        
        -- Contact Info: Direct Map
        contact_number AS "ContactNumber"
        
    FROM enterprise_merchant_data;
    """
    
    try:
        with engine.connect() as connection:
            # Fetch the data into a pandas DataFrame
            df_dim = pd.read_sql(text(transformation_query), connection)
        
        return df_dim
        
    except Exception as e:
        print(f"Error during DIM_MERCHANT transformation: {e}")
        return None