import pandas as pd

def read_xlsx(file_path: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(file_path)
        return df
    except Exception as e:
        print(f"Failed to read Excel file {file_path}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error
