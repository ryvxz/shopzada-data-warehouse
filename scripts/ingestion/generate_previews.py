import os
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

STAGING_DIR = os.getenv("STAGING_DIR")
PREVIEW_DIR = os.getenv("PREVIEW_DIR")
os.makedirs(PREVIEW_DIR or "/opt/airflow/plugins/data/staging/previews", exist_ok=True)

for file in os.listdir(STAGING_DIR):
    if file.endswith(".parquet"):
        table_name = os.path.splitext(file)[0]
        staged_path = os.path.join(STAGING_DIR or "/opt/airflow/plugins/data/staging", file)
        df = pd.read_parquet(staged_path)
        preview_path = os.path.join(PREVIEW_DIR or "/opt/airflow/plugins/data/staging/previews", f"{table_name}_preview.csv")
        df.head(10).to_csv(preview_path, index=False)
        print(f"Preview saved: {preview_path}")

print("\nAll table previews generated. You can open these CSVs to show 'staging tables populated'.")
