import os
import pandas as pd

def stage_to_parquet(df, staging_folder, file_name):
    """
    Stages the DataFrame to Parquet format.
    Creates the folder if it doesn't exist.
    """
    os.makedirs(staging_folder, exist_ok=True)
    if file_name.endswith(('.csv', '.xlsx', '.json', '.pkl', '.parquet', '.html')):
        file_name = os.path.splitext(file_name)[0]
    staged_path = os.path.join(staging_folder, f"{file_name}.parquet")
    df.to_parquet(staged_path, index=False)
    return staged_path
