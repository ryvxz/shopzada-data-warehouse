import pandas as pd
import glob

def ingest_html():
    html_files = glob.glob("../../data/raw/**/*.html", recursive=True)
    print(f"Found HTML files: {html_files}")

    for file in html_files:
        try:
            dfs = pd.read_html(file)
            print(f"[SUCCESS] Loaded HTML file: {file}, tables found={len(dfs)}")
        except Exception as e:
            print(f"[ERROR] Could not load {file}: {e}")