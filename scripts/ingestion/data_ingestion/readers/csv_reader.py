import pandas as pd
from io import StringIO

def read_csv(file_path):
    # Define common delimiters to check, in order of preference/likelihood
    DELIMITERS = [',', '\t', ';', '|']
    EXPECTED_MIN_COLUMNS = 2  # A good heuristic for actual data files
    
    # 1. Read the file into a string buffer to allow multiple read attempts
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            file_content = f.read()
    except Exception as e:
        raise IOError(f"Error reading file content: {e}")

    # 2. Iterate through delimiters to find the best match
    best_delimiter = None
    
    for delimiter in DELIMITERS:
        try:
            # Use StringIO to treat the content string as a file for reading
            df_test = pd.read_csv(StringIO(file_content), sep=delimiter)
            
            # Check if the number of columns meets the minimum expectation
            if df_test.shape[1] >= EXPECTED_MIN_COLUMNS:
                print(f"--- Detected separator: '{delimiter}' for {file_path}")
                best_delimiter = delimiter
                break # Found a successful delimiter
        except pd.errors.ParserError:
            # Skip if the pandas parser fails with this delimiter
            continue 

    # 3. Read the full file using the determined delimiter
    if best_delimiter:
        # We need to read it again from the file path, 
        # as the StringIO parsing above might have limitations on large files.
        return pd.read_csv(file_path, sep=best_delimiter)
    else:
        # Fallback: Try reading with the default (comma) one last time
        # if the column check failed, but warn the user.
        try:
            print(f"--- Warning: Could not auto-detect separator. Reading {file_path} with default comma.")
            df = pd.read_csv(file_path)
            if df.shape[1] < EXPECTED_MIN_COLUMNS:
                 raise ValueError("File parsed with few columns, likely incorrect delimiter.")
            return df
        except Exception as e:
            raise ValueError(f"Could not successfully parse file '{file_path}' with common delimiters or default settings. Error: {e}")