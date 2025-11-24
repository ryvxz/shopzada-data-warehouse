import os
import pandas as pd

STAGING_DIR = r"C:\Users\Aaron\shopzada-data-warehouse\data\staging"
PREVIEW_DIR = os.path.join(STAGING_DIR, "previews")
os.makedirs(PREVIEW_DIR, exist_ok=True)

for file in os.listdir(STAGING_DIR):
    if file.endswith(".parquet"):
        table_name = os.path.splitext(file)[0]
        staged_path = os.path.join(STAGING_DIR, file)
        df = pd.read_parquet(staged_path)
        preview_path = os.path.join(PREVIEW_DIR, f"{table_name}_preview.csv")
        df.head(10).to_csv(preview_path, index=False)
        print(f"Preview saved: {preview_path}")

print("\nAll table previews generated. You can open these CSVs to show 'staging tables populated'.")
