# etl/transform/cleaning.py (only the function shown)
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def standardize_column_names(df):
    # Handle MultiIndex columns (flatten them with underscore)
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for col in df.columns:
            # join parts with underscore, skip empty parts
            joined = "_".join([str(c).strip() for c in col if c is not None and str(c).strip() != ""])
            new_cols.append(joined or "unnamed")
        df.columns = new_cols
    else:
        # Ensure all column names are strings
        df.columns = df.columns.map(lambda x: str(x).strip())

    # Now safely apply string transformations without collapsing meaningful separators
    df.columns = (
        pd.Index(df.columns)
        .str.strip()
        .str.lower()
        # Replace spaces with underscore
        .str.replace(r"\s+", "_", regex=True)
        # Replace non-safe chars with underscore (keep dots replaced to underscore instead of removing)
        .str.replace(r"[^a-z0-9_]", "_", regex=True)
        # Collapse multiple underscores into single
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    return df



def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full cleaning step for transform layer.
    - Standardizes column names
    - Drops fully empty rows
    - Fills remaining NaNs
    - Removes duplicates
    """
    logger.info("Cleaning dataframe...")
    df = standardize_column_names(df)
    df = df.dropna(how="all")
    df = df.fillna("")
    df = df.drop_duplicates()
    logger.info(f"Cleaned dataframe → rows: {len(df)}, columns: {df.columns.tolist()}")
    return df