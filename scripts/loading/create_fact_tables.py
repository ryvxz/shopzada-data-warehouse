from sqlalchemy import create_engine, text
import os

# Define your connection details for the DWH
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DWH_HOST = os.getenv('DB_DWH_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_DWH_NAME = os.getenv('DB_DWH_NAME')

# Function to create fact_order_line_item table
def create_fact_order_line_item_table(dwh_engine):
    # CHANGED: All column names to lowercase for ETL compatibility
    # ADDED: UNIQUE constraint on order_id to allow ON CONFLICT upserts
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fact_order_line_item (
        sk_orderlineitem SERIAL PRIMARY KEY,
        order_id TEXT NOT NULL UNIQUE,
        sk_customer INT NOT NULL,
        sk_merchant INT NOT NULL,
        sk_staff INT NOT NULL,
        sk_product INT NOT NULL,
        sk_date INT NOT NULL,
        quantitysold INT,
        lineitemprice DECIMAL,
        lineitemtotalamount DECIMAL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sk_customer) REFERENCES dim_customer(sk_customer),
        FOREIGN KEY (sk_merchant) REFERENCES dim_merchant(sk_merchant),
        FOREIGN KEY (sk_staff) REFERENCES dim_staff(sk_staff),
        FOREIGN KEY (sk_product) REFERENCES dim_product(sk_product),
        FOREIGN KEY (sk_date) REFERENCES dim_date(sk_date)
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("fact_order_line_item table created successfully.")
    except Exception as e:
        print(f"Error creating fact_order_line_item table: {e}")

# Function to create fact_campaign_transaction table
def create_fact_campaign_transaction_table(dwh_engine):
    # CHANGED: All column names to lowercase
    # ADDED: UNIQUE constraint on order_id
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fact_campaign_transaction (
        sk_campaigntrans SERIAL PRIMARY KEY,
        order_id TEXT NOT NULL UNIQUE,
        sk_campaign INT NOT NULL,
        sk_date INT NOT NULL,
        availedflag BOOLEAN,
        estimatedarrivaldays INT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sk_campaign) REFERENCES dim_campaign(sk_campaign),
        FOREIGN KEY (sk_date) REFERENCES dim_date(sk_date)
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("fact_campaign_transaction table created successfully.")
    except Exception as e:
        print(f"Error creating fact_campaign_transaction table: {e}")

# Function to create fact_order_delay table
def create_fact_order_delay_table(dwh_engine):
    # CHANGED: All column names to lowercase
    # ADDED: UNIQUE constraint on order_id
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fact_order_delay (
        sk_orderdelay SERIAL PRIMARY KEY,
        order_id TEXT NOT NULL UNIQUE,
        daysdelayed INT,
        sk_date INT NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sk_date) REFERENCES dim_date(sk_date)
    );
    """
    try:
        with dwh_engine.begin() as connection:
            connection.execute(text(create_table_sql))
            print("fact_order_delay table created successfully.")
    except Exception as e:
        print(f"Error creating fact_order_delay table: {e}")

def main():
    connection_dwh_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_DWH_HOST}:{DB_PORT}/{DB_DWH_NAME}"
    dwh_engine = create_engine(connection_dwh_string)

    create_fact_order_line_item_table(dwh_engine)
    create_fact_campaign_transaction_table(dwh_engine)
    create_fact_order_delay_table(dwh_engine)

if __name__ == "__main__":
    main()