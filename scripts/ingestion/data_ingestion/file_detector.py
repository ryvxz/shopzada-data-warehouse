import os

def detect_file_type(file_path):
    _, ext = os.path.splitext(file_path.lower())
    ext = ext.replace(".", "")

    if ext in ["csv"]:
        return "csv"
    if ext in ["xlsx", "xls"]:
        return "xlsx"
    if ext in ["json"]:
        return "json"
    if ext in ["pkl", "pickle"]:
        return "pickle"
    if ext in ["parquet"]:
        return "parquet"
    if ext in ["html", "htm"]:
        return "html"

    raise Exception(f"Unsupported file type: {ext}")
