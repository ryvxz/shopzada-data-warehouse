import pandas as pd
import numpy as np

# --- Helper Functions for Standardization ---

def standardize_string_column(series: pd.Series) -> pd.Series:
    """Cleans up strings: trims whitespace, converts to lowercase."""
    return series.astype(str).str.strip().str.lower()

def standardize_boolean_column(series: pd.Series) -> pd.Series:
    """Converts common truthy/falsy strings (Y/N, T/F) to standard Booleans."""
    # Ensure all values are strings for comparison, handle NaNs
    series_str = series.astype(str).str.strip().str.lower()
    
    # Map common values to 1/0, then convert to boolean
    mapping = {
        'yes': True, 'y': True, 'true': True, 't': True, '1': True,
        'no': False, 'n': False, 'false': False, 'f': False, '0': False,
        'nan': np.nan, 'none': np.nan # Keep NaNs/NoneType as is
    }
    
    # Apply mapping, then attempt to convert to bool (pandas handles NaN/None gracefully)
    return series_str.map(mapping).astype('bool', errors='ignore')


# --- Core Quality Check and Standardization Function ---

def check_and_report_quality(staging_tables: dict) -> dict:
    """
    Performs quality checks and standardizes values for all DataFrames.
    
    Args:
        staging_tables: Dictionary of {table_name: DataFrame}.
        
    Returns:
        The updated dictionary of DataFrames after processing.
    """
    print("\n--- Starting Data Quality Validation and Standardization ---")
    
    validated_tables = {}
    
    for table_name, df in staging_tables.items():
        print(f"\nProcessing Table: {table_name} ({len(df)} rows)")
        df_validated = df.copy() # Work on a copy
        
        # 1. Null Value Check & Report
        null_counts = df_validated.isnull().sum()
        if null_counts.sum() > 0:
            print("  ⚠️ Null values detected in columns:")
            print(null_counts[null_counts > 0].to_string())
            
        # 2. Duplicate Check & Report
        duplicate_rows = df_validated.duplicated().sum()
        if duplicate_rows > 0:
            print(f"  ❌ {duplicate_rows} duplicate rows found and will be removed.")
            df_validated.drop_duplicates(inplace=True)
            print(f"  Rows remaining: {len(df_validated)}")

        # 3. Standardization (Iterate over columns)
        for col in df_validated.columns:
            
            # Standardization Rule 1: Apply to object (string) columns
            if df_validated[col].dtype == 'object':
                df_validated[col] = standardize_string_column(df_validated[col])
                
            # Standardization Rule 2: Try to standardize columns with 'bool' in name
            # This is a heuristic and might need customization based on actual data
            if 'is_' in col.lower() or 'flag' in col.lower():
                 try:
                    df_validated[col] = standardize_boolean_column(df_validated[col])
                 except Exception as e:
                    print(f"  🚫 Could not standardize boolean column {col}: {e}")

        
        # 4. Final Type Check (Optional)
        # You can add logic here to check if standardized columns now have expected types,
        # e.g., checking if the 'is_active' column is now boolean.
        
        validated_tables[table_name] = df_validated
        
    print("\n--- Data Quality Checks and Standardization Complete ---")
    return validated_tables

# NOTE: The main orchestration script (orchestrator.py) will call this function.
# Example usage in orchestrator.py:
# staging_tables = check_and_report_quality(staging_tables)