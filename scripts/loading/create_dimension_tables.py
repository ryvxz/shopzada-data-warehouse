# create_tables.py
from sqlalchemy import create_engine, text
import os

# Define your connection details for the DWH
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DWH_HOST = os.getenv('DB_DWH_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_DWH_NAME = os.getenv('DB_DWH_NAME')

# Function to create dim_campaign table
def create_dim_campaign_table(dwh_engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dim_campaign (
        SK_Campaign SERIAL PRIMARY KEY,
        campaign_id TEXT UNIQUE NOT NULL,
        CampaignName VARCHAR(255),
        CampaignDescription TEXT,
        CampaignDiscountRate TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("dim_campaign table created successfully (or already exists).")
    except Exception as e:
        print(f"Error creating dim_campaign table: {e}")

# Function to create dim_customer table
def create_dim_customer_table(dwh_engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dim_customer (
        SK_Customer SERIAL PRIMARY KEY,
        customer_id TEXT UNIQUE NOT NULL,
        CustomerName VARCHAR(255),
        JobTitle VARCHAR(100),
        JobLevel VARCHAR(100),
        CreditCardType VARCHAR(100),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("dim_customer table created successfully (or already exists).")
    except Exception as e:
        print(f"Error creating dim_customer table: {e}")

# Function to create dim_date table
def create_dim_date_table(dwh_engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dim_date (
        SK_Date INT PRIMARY KEY,
        FullDate DATE,
        MonthName VARCHAR(50),
        MonthNumber INT,
        Quarter INT,
        Year INT,
        DayName VARCHAR(50),
        DayType VARCHAR(50),
        WeekOfYear INT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("dim_date table created successfully (or already exists).")
    except Exception as e:
        print(f"Error creating dim_date table: {e}")

# Function to create dim_merchant table
def create_dim_merchant_table(dwh_engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dim_merchant (
        SK_Merchant SERIAL PRIMARY KEY,
        merchant_id TEXT UNIQUE NOT NULL,
        MerchantName VARCHAR(255),
        CreationDate TEXT,
        StreetAddress VARCHAR(255),
        City VARCHAR(100),
        State VARCHAR(100),
        Country VARCHAR(100),
        ContactNumber VARCHAR(50),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("dim_merchant table created successfully (or already exists).")
    except Exception as e:
        print(f"Error creating dim_merchant table: {e}")

# Function to create dim_product table
def create_dim_product_table(dwh_engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dim_product (
        SK_Product SERIAL PRIMARY KEY,
        product_id TEXT UNIQUE NOT NULL,
        ProductName VARCHAR(255),
        ProductType VARCHAR(100),
        ProductPrice DECIMAL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("dim_product table created successfully (or already exists).")
    except Exception as e:
        print(f"Error creating dim_product table: {e}")

# Function to create dim_staff table
def create_dim_staff_table(dwh_engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dim_staff (
        SK_Staff SERIAL PRIMARY KEY,
        staff_id TEXT UNIQUE NOT NULL,
        StaffName VARCHAR(255),
        JobLevel VARCHAR(100),
        HireDate DATE,
        State VARCHAR(100),
        City VARCHAR(100),
        Street VARCHAR(255),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("dim_staff table created successfully (or already exists).")
    except Exception as e:
        print(f"Error creating dim_staff table: {e}")

def main():
    # Set up the connection string to your DWH (Data Warehouse)
    connection_dwh_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    dwh_engine = create_engine(connection_dwh_string)

    # Create all the necessary dimension tables
    create_dim_campaign_table(dwh_engine)
    create_dim_customer_table(dwh_engine)
    create_dim_date_table(dwh_engine)
    create_dim_merchant_table(dwh_engine)
    create_dim_product_table(dwh_engine)
    create_dim_staff_table(dwh_engine)

if __name__ == "__main__":
    main()
