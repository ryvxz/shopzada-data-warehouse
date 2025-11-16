from ingest_excel import ingest_excel
from ingest_json import ingest_json
from ingest_parquet import ingest_parquet
from ingest_pickle import ingest_pickle
from ingest_html import ingest_html

if __name__ == "__main__":
    print("Starting ingestion...")
    ingest_excel()
    ingest_json()
    ingest_parquet()
    ingest_pickle()
    ingest_html()
    print("Ingestion completed.")