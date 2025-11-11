import pandas as pd
import json
import pickle

product_file = "../data/raw/business/product_list.xlsx"
products_df = pd.read_excel(product_file)
print("Products Data:")
print(products_df.head(), "\n")

user_file = "../data/raw/customer/user_data.json"
with open(user_file, "r") as f:
    user_data = json.load(f)
users_df = pd.json_normalize(user_data)
print("Users Data:")
print(users_df.head(), "\n")

pickle_file = "../data/raw/customer/user_credit_card.pickle"
with open(pickle_file, "rb") as f:
    credit_card_data = pickle.load(f)
credit_df = pd.DataFrame(credit_card_data)
print("Credit Card Data:")
print(credit_df.head(), "\n")

order_file = "../data/raw/operations/order_data_20200101-20200701.parquet"
orders_df = pd.read_parquet(order_file)
print("Orders Data:")
print(orders_df.head(), "\n")
