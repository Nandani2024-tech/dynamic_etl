#cleanining
import logging
import pandas as pd
import numpy as np
import json

logger = logging.getLogger(__name__)


def standardize_column_names(df):
    # Handle MultiIndex columns (flatten them with underscore)
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for col in df.columns:
            joined = "_".join([str(c).strip() for c in col if c is not None and str(c).strip() != ""])
            new_cols.append(joined or "unnamed")
        df.columns = new_cols
    else:
        df.columns = df.columns.map(lambda x: str(x).strip())

    df.columns = (
        pd.Index(df.columns)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    return df


# 🔥 NEW: Fixes "unhashable type: dict"
def make_hashable(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert nested dicts/lists to JSON strings
    so drop_duplicates() will not crash.
    """
    return df.apply(
        lambda col: col.map(
            lambda x: json.dumps(x, sort_keys=True)
            if isinstance(x, (dict, list))
            else x
        )
    )


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full cleaning step for transform layer.
    - Standardizes column names
    - Drops fully empty rows
    - Fills remaining NaNs
    - Removes duplicates safely (works with nested dict/list)
    """
    logger.info("Cleaning dataframe...")

    df = standardize_column_names(df)

    # Remove rows that are fully NaN
    df = df.dropna(how="all")

    # Replace NaN with empty string
    df = df.fillna("")

    # 🛡 SAFE duplicate removal (patched)
    try:
        safe_df = make_hashable(df)
        mask = safe_df.duplicated()
        df = df[~mask]
    except Exception as e:
        logger.error(f"Duplicate removal failed: {e}")
        # fallback = keep original df without removing duplicates
        pass

    logger.info(f"Cleaned dataframe → rows: {len(df)}, columns: {df.columns.tolist()}")
    return df