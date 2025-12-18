# create_fact_tables.py
from sqlalchemy import create_engine, text
import os

# Define your connection details for the DWH
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DWH_HOST = os.getenv('DB_DWH_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_DWH_NAME = os.getenv('DB_DWH_NAME')

# Function to create FACT_ORDER_LINE_ITEM table
def create_fact_order_line_item_table(dwh_engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fact_order_line_item (
        SK_OrderLineItem SERIAL PRIMARY KEY,
        Order_ID INT NOT NULL,
        SK_Customer INT NOT NULL,
        SK_Merchant INT NOT NULL,
        SK_Staff INT NOT NULL,
        SK_Product INT NOT NULL,
        SK_Date INT NOT NULL,
        QuantitySold INT,
        LineItemPrice DECIMAL,
        LineItemTotalAmount DECIMAL,
        FOREIGN KEY (SK_Customer) REFERENCES dim_customer(SK_Customer),
        FOREIGN KEY (SK_Merchant) REFERENCES dim_merchant(SK_Merchant),
        FOREIGN KEY (SK_Staff) REFERENCES dim_staff(SK_Staff),
        FOREIGN KEY (SK_Product) REFERENCES dim_product(SK_Product),
        FOREIGN KEY (SK_Date) REFERENCES dim_date(SK_Date),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("fact_order_line_item table created successfully (or already exists).")
    except Exception as e:
        print(f"Error creating fact_order_line_item table: {e}")

# Function to create FACT_CAMPAIGN_TRANSACTION table
def create_fact_campaign_transaction_table(dwh_engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fact_campaign_transaction (
        SK_CampaignTrans SERIAL PRIMARY KEY,
        Order_ID INT NOT NULL,
        SK_Campaign INT NOT NULL,
        SK_Date INT NOT NULL,
        AvailedFlag BOOLEAN,
        EstimatedArrivalDays INT,
        FOREIGN KEY (SK_Campaign) REFERENCES dim_campaign(SK_Campaign),
        FOREIGN KEY (SK_Date) REFERENCES dim_date(SK_Date),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("fact_campaign_transaction table created successfully (or already exists).")
    except Exception as e:
        print(f"Error creating fact_campaign_transaction table: {e}")

# Function to create FACT_ORDER_DELAY table
def create_fact_order_delay_table(dwh_engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fact_order_delay (
        SK_OrderDelay SERIAL PRIMARY KEY,
        Order_ID INT NOT NULL,
        DaysDelayed INT,
        SK_Date INT NOT NULL,
        FOREIGN KEY (SK_Date) REFERENCES dim_date(SK_Date),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("fact_order_delay table created successfully (or already exists).")
    except Exception as e:
        print(f"Error creating fact_order_delay table: {e}")

def main():
    # Set up the connection string to your DWH (Data Warehouse)
    connection_dwh_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    dwh_engine = create_engine(connection_dwh_string)

    # Create all the necessary fact tables
    create_fact_order_line_item_table(dwh_engine)
    create_fact_campaign_transaction_table(dwh_engine)
    create_fact_order_delay_table(dwh_engine)

if __name__ == "__main__":
    main()
