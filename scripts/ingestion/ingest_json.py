import pandas as pd
import glob

def ingest_json():
    json_files = glob.glob("../../data/raw/**/*.json", recursive=True)
    print(f"Found JSON files: {json_files}")

    for file in json_files:
        try:
            df = pd.read_json(file, lines=False)
            print(f"[SUCCESS] Loaded JSON file: {file}, rows={len(df)}")
        except ValueError:
            df = pd.read_json(file, lines=True)
            print(f"[SUCCESS] Loaded JSON file (line mode): {file}, rows={len(df)}")
        except Exception as e:
            print(f"[ERROR] Could not load {file}: {e}")