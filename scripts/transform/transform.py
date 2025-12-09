from sqlalchemy import create_engine, text
import os
DB_USER='postgres'
DB_PASSWORD='shopzada123'
DB_HOST='db_staging'
DB_PORT='5432'
DB_NAME='shopzada_staging'
# --- Configuration ---
user = DB_USER
password = DB_PASSWORD
host = DB_HOST
port = DB_PORT
db = DB_NAME

# Create engine for the postgresql (moved inside main or kept global if needed across modules)
ENGINE = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

def main():

    # Table name (replace with your actual table name)
    table_name = "customer_user_credit_card"
    
    # Creating the query string
    query = f"SELECT * FROM {table_name}"

    # Connect to the database and execute the query
    with ENGINE.connect() as connection:
        result = connection.execute(text(query))
        
        # Fetch all the rows
        rows = result.fetchall()
        
        # Print the result
        for row in rows:
            print(row)

if __name__ == "__main__":
    main()
