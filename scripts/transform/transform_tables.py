import pandas as pd
from sqlalchemy import create_engine
import os
from scripts.transform.dimension.dim_product import transform_product
from scripts.transform.dimension.dim_campaign import transform_campaign
from scripts.transform.dimension.dim_customer import transform_customer
from scripts.transform.dimension.dim_date import transform_date
from scripts.transform.dimension.dim_merchant import transform_merchant
from scripts.transform.dimension.dim_staff import transform_staff

from scripts.transform.fact.fact_order_line_item import transform_fact_order_line_item
from scripts.transform.fact.fact_campaign_transaction import transform_campaign_transaction
from scripts.transform.fact.fact_order_delay import transform_order_delay
# 1. Define your connection details for staging layer
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_STAGING_HOST') # or your server IP/hostname
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_STAGING_NAME')

# 2. Create the database engine (connection string)
# Format: postgresql+psycopg2://user:password@host:port/dbname
connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

def transform_dimension_tables(engine):
    try:
        # 1. Transform each dimension using the individual scripts
        #dim_campaign = transform_campaign(engine)
        dim_date     = transform_date(engine)
        dim_merchant = transform_merchant(engine)
        dim_staff    = transform_staff(engine)
        dim_customer = transform_customer(engine)
        dim_product  = transform_product(engine)

        # 2. Print Previews (Using a dictionary for cleaner iteration)
        dims = {
            #"DIM_CAMPAIGN": dim_campaign,
            "DIM_DATE": dim_date,
            "DIM_MERCHANT": dim_merchant,
            "DIM_STAFF": dim_staff,
            "DIM_CUSTOMER":dim_customer,
            "DIM_PRODUCT":dim_product
        }

        for name, df in dims.items():
            if df is not None:
                print(f"--- {name} Head ---")
                print(df.head(), "\n")
            else:
                print(f"--- {name} FAILED to transform ---\n")

        # Return the DataFrames in a dictionary for easy access in the Load phase
        return dims

    except Exception as e:
        print(f"Error in orchestration: {e}")
        return None

def transform_fact_tables(engine):
    try:
        
        fact_order_line_item = transform_fact_order_line_item(engine)
        fact_campaign_transaction = transform_campaign_transaction(engine)
        fact_order_delay = transform_order_delay(engine)

        facts = {
            "FACT_ORDER_LINE_ITEM": fact_order_line_item,
            "FACT_CAMPAIGN_TRANSACTION": fact_campaign_transaction,
            "FACT_ORDER_DELAY": fact_order_delay
        }

        for name, df in facts.items():
            if df is not None:
                print(f"--- {name} Head ---")
                print(df.head(),"\n")
            else:
                print(f"--- {name} FAILED to transform ---\n")

        return facts
    
    except Exception as e:
        print(f"Error in orchestration: {e}")
        return None

def main():
    dimension_tables = transform_dimension_tables(engine)
    fact_tables = transform_fact_tables(engine)

    return dimension_tables, fact_tables

if __name__ == '__main__':
    main()
