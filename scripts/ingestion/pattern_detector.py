import re
import pandas as pd

def detect_format(value):
    if isinstance(value, str):
        if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            return "YYYY-MM-DD"
        if re.match(r"^\d{4}_\d{2}$", value):
            return "YEAR_MONTH"
        if re.match(r"^USER\d+$", value):
            return "USER_ID"
        if re.match(r"^\d+days$", value):
            return "NUM_DAYS"
        if re.match(r"^\d+(\.\d+)?$", value):
            return "NUMERIC_STRING"
        if re.match(r"^\d{2}:\d{2}(:\d{2})?$", value):
            return "TIME"
        if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?$", value):
            return "DATETIME"
    return "UNKNOWN"

def extract_column_formats(df, sample_size=10):
    formats = {}
    for col in df.columns:
        col_as_str = df[col].dropna().astype(str)
        sample = col_as_str.head(sample_size).tolist()
        detected = set(detect_format(v) for v in sample)
        formats[col] = list(detected)
    return formats