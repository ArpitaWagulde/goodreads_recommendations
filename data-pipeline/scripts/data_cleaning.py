import pandas as pd
import numpy as np
import ast
import os
import logging

# ------------------------------
# Safe parsing of values
# ------------------------------
def safe_parse(val):
    """Parse a value safely into list/dict or leave as string."""
    # Already a NumPy array → convert to list
    if isinstance(val, np.ndarray):
        return val.tolist()
    
    # Already list/dict → keep as is
    if isinstance(val, (list, dict)):
        return val
    
    # NaN / None → empty list
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    
    # Strings that look like lists/dicts
    val_str = str(val).strip()
    if val_str.startswith('[') or val_str.startswith('{'):
        try:
            parsed = ast.literal_eval(val_str)
            if isinstance(parsed, (list, dict)):
                return parsed
            return val_str
        except Exception:
            return val_str
    
    # Otherwise, return stripped string
    return val_str

# ------------------------------
# Flatten nested columns
# ------------------------------
def flatten_column(val, key=None):
    """
    Convert list/dict values into simple list or string.
    - Lists of dicts: extract 'name' if exists
    - Lists of strings: keep as is
    - Dicts: keep as string
    """
    if isinstance(val, list):
        flattened = []
        for item in val:
            if isinstance(item, dict) and 'name' in item:
                flattened.append(item['name'])
            elif isinstance(item, str):
                flattened.append(item)
        return flattened
    elif isinstance(val, dict):
        if key and key in val:
            return val[key]
        return str(val)
    else:
        return val

# ------------------------------
# Main cleaning function
# ------------------------------
def clean_goodreads_df(df: pd.DataFrame, save=True, out_dir="../data/processed") -> pd.DataFrame:
    logging.info(f"🔹 Cleaning Goodreads data (rows={len(df)})")
    df = df.copy()
    
    for col in df.columns:
        # Numeric columns
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = df[col].astype(int)
        
        # Object / String columns
        elif pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].apply(safe_parse)
            
            # Flatten nested columns dynamically
            # For list/dict columns, create a new column with '_flat' suffix
            flattened_col_name = f"{col}_flat"
            df[flattened_col_name] = df[col].apply(flatten_column)
        
        # Other types
        else:
            df[col] = df[col].fillna(0)
    
    # Optional saving
    if save:
        os.makedirs(out_dir, exist_ok=True)
        file_path = os.path.join(out_dir, "goodreads_cleaned.csv")
        df.to_csv(file_path, index=False)
        logging.info(f"✅ Cleaned data saved to {file_path}")
    
    return df
