import pandas as pd
import pickle

def read_pickle(file_path):
    with open(file_path, "rb") as f:
        data = pickle.load(f)
    return pd.DataFrame(data)
