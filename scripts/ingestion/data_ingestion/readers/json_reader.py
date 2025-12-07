import pandas as pd
import json

def read_json(file_path):
    with open(file_path) as f:
        data = json.load(f)
    return pd.DataFrame(data)
