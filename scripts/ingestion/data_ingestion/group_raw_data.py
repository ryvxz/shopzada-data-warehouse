from typing import Dict
import pandas as pd

def check_file_domain(headers: list):
    # Define your categories and their associated headers
    business_files = {"product_list": ['product_id', 'product_name', 'product_type', 'price']}
    customer_files = {
        'user_credit_card': ['user_id', 'name', 'credit_card_number', 'issuing_bank'],
        'user_data': ['user_id', 'creation_date', 'name', 'street', 'state', 'city', 'country', 'birthdate', 'gender', 'device_address', 'user_type'],
        'user_job': ['user_id', 'name', 'job_title', 'job_level'],
    }
    enterprise_files = {
        'merchant_data': ['merchant_id', 'creation_date', 'name', 'street', 'state', 'city', 'country', 'contact_number'],
        'order_with_merchant_data': ['order_id', 'merchant_id', 'staff_id'],
        'staff_data': ['staff_id', 'name', 'job_level', 'street', 'state', 'city', 'country', 'contact_number', 'creation_date'],
    }
    marketing_files = {
        'campaign_data': ['campaign_id', 'campaign_name', 'campaign_description', 'discount'],
        'transactional_campaign_data': ['transaction_date', 'campaign_id', 'order_id', 'estimated arrival', 'availed'],
    }
    operations_files = {
        'line_item_data_prices': ['order_id', 'price', 'quantity'],
        'line_item_data_products': ['order_id', 'product_name', 'product_id'],
        'order_data': ['order_id', 'user_id', 'estimated arrival', 'transaction_date'],
        'order_delays': ['order_id', 'delay in days'],
    }

    # Combine all file categories into one dictionary
    all_files = {
        **business_files,
        **customer_files,
        **enterprise_files,
        **marketing_files,
        **operations_files
    }

    # Check the headers against each file category
    for file_category, columns in all_files.items():
        if set(headers) == set(columns):
            return file_category
    
    # Return None or a message if no match is found
    return "Unknown file type or unrecognized headers"

def merge_files(staging_tables: Dict[str, any], file_name_1: str, file_name_2: str, new_file_name: str):
    try:
        # Check if the file names exist in the dictionary
        if file_name_1 not in staging_tables or file_name_2 not in staging_tables:
            raise KeyError(f"One or both of the keys '{file_name_1}' or '{file_name_2}' do not exist in the dictionary.")

         # Get the DataFrames
        df1, df2 = staging_tables[file_name_1], staging_tables[file_name_2]
        
        # Check for empty DataFrames
        if df1.empty or df2.empty:
            raise ValueError(f"One or both of the DataFrames are empty: '{file_name_1}', '{file_name_2}'")
            
        # Merging the two DataFrames (df + df1)
        staging_tables[new_file_name] = pd.concat([staging_tables[file_name_1], staging_tables[file_name_2]], ignore_index=True)
        
        # Optionally, remove the original keys 'file_name_1' and 'file_name_2' if not needed
        if file_name_1 != new_file_name:
            del staging_tables[file_name_1]

        if file_name_2 != new_file_name:
            del staging_tables[file_name_2]
        
        print(f"Files '{file_name_1}' and '{file_name_2}' successfully merged into '{new_file_name}'.")

    except KeyError as e:
        print(f"KeyError: {e}")
    except ValueError as e:
        print(f"ValueError: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")



    
