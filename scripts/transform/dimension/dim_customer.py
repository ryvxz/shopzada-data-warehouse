import pandas as pd
from sqlalchemy import create_engine, text

def transform_customer(engine):
    
    transformation_query = """
    WITH joined_data AS (
        SELECT 
            d.user_id,
            d.name AS CustomerName,
            j.job_title AS JobTitle,
            j.job_level AS JobLevel,
            c.issuing_bank,
            c.number,
            -- Logic to infer CreditCardType
            CASE 
                WHEN c.issuing_bank = 'Chase' THEN 'Visa'
                WHEN c.issuing_bank = 'Amex' THEN 'American Express'
                WHEN c.number LIKE '4%' THEN 'Visa'
                WHEN c.number LIKE '5%' THEN 'MasterCard'
                ELSE 'Unknown'
            END AS CreditCardType
        FROM customer_user_data d
        LEFT JOIN user_job j ON d.user_id = j.user_id
        LEFT JOIN user_credit_card c ON d.user_id = c.user_id
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY user_id) AS SK_Customer, -- Surrogate Key
        user_id,                                           -- Natural Key
        CustomerName,
        JobTitle,
        JobLevel,
        CreditCardType
    FROM joined_data;
    """
    
    try:
        with engine.connect() as connection:
            df_customer = pd.read_sql(text(transformation_query), connection)
        return df_customer
    except Exception as e:
        print(f"Error transforming DIM_CUSTOMER: {e}")
        return None