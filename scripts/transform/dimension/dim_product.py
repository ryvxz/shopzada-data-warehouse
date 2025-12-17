import pandas as pd
from sqlalchemy import text

def transform_product(engine):
    transformation_query = """
    SELECT 
        -- Surrogate Key (System Generated)
        ROW_NUMBER() OVER (ORDER BY product_id) AS SK_Product,
        
        -- Natural Key & Direct Mapping
        product_id,
        product_name AS ProductName,
        product_type AS ProductType,
        
        -- Snapshot of current price
        price AS ProductPrice
    FROM business_product_list;
    """
    
    try:
        with engine.connect() as connection:
            df_dim = pd.read_sql(text(transformation_query), connection)
        
        return df_dim
        
    except Exception as e:
        print(f"Error during transformation: {e}")
        return None
