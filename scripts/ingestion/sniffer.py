import os

def detect_file_type(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        return "csv"
    elif ext in [".json"]:
        return "json"
    elif ext in [".xls", ".xlsx"]:
        return "excel"
    elif ext in [".pkl", ".pickle"]:
        return "pickle"
    elif ext in [".html", ".htm"]:
        return "html"
    elif ext == ".parquet":
        return "parquet"
    else:
        return None
