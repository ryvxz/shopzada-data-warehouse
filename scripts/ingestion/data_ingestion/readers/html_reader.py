import pandas as pd

def read_html(file_path):
    dfs = pd.read_html(file_path)
    return dfs[0] if dfs else pd.DataFrame()
