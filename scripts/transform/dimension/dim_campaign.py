import pandas as pd
from sqlalchemy import text

def transform_campaign(engine):
    """
    Extracts and transforms campaign data from the raw source table 
    into the DIM_CAMPAIGN format using PostgreSQL logic.
    """
    transformation_query = """
    SELECT 
        -- SK_Campaign (PK): System Generated Surrogate Key
        ROW_NUMBER() OVER (ORDER BY campaign_id) AS "SK_Campaign",
        
        -- campaign_id (Natural Key): Direct Map
        campaign_id,
        
        -- CampaignName: Direct Map
        campaign_name AS "CampaignName",
        
        -- CampaignDescription: Direct Map
        campaign_description AS "CampaignDescription",
        
        -- CampaignDiscountRate: Direct Map
        discount AS "CampaignDiscountRate"
        
    FROM marketing_campaign_data;
    """
    
    try:
        with engine.connect() as connection:
            # Execute the query and return the result as a DataFrame
            df_dim = pd.read_sql(text(transformation_query), connection)
        
        return df_dim
        
    except Exception as e:
        print(f"Error during DIM_CAMPAIGN transformation: {e}")
        return None