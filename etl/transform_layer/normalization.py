"""
Normalization module for the transform layer.

This module handles:
- Numeric normalization (type casting, coercion)
- Date parsing
- Standardizing categorical values
- Lowercasing / formatting certain fields
- Normalizing IDs, postal codes, country codes, etc. (as needed)

Main public function:
    normalize(df: pd.DataFrame) -> pd.DataFrame
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
#  Numeric Normalization
# ---------------------------------------------------------

def normalize_numeric_columns(df: pd.DataFrame, numeric_cols: list) -> pd.DataFrame:
    """
    Convert columns to numeric, coercing errors to NaN.
    """
    logger.debug("Normalizing numeric columns...")

    df = df.copy()
    for col in numeric_cols:
        if col in df.columns:
            logger.debug(f"Converting '{col}' to numeric...")
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------
#  Datetime Normalization
# ---------------------------------------------------------

def normalize_datetime_columns(df: pd.DataFrame, datetime_cols: list) -> pd.DataFrame:
    """
    Convert date/time columns to pandas datetime format.
    """
    logger.debug("Normalizing datetime columns...")

    df = df.copy()
    for col in datetime_cols:
        if col in df.columns:
            logger.debug(f"Parsing datetime field '{col}'...")
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


# ---------------------------------------------------------
#  Categorical Normalization
# ---------------------------------------------------------

def standardize_string_columns(df: pd.DataFrame, string_cols: list) -> pd.DataFrame:
    """
    Normalize common categorical/string fields:
    - lowercase text
    - collapse repeated spaces
    - remove punctuation if needed (configurable)
    """
    logger.debug("Normalizing string/categorical columns...")

    df = df.copy()
    for col in string_cols:
        if col in df.columns:
            logger.debug(f"Standardizing '{col}'...")
            df[col] = (
                df[col]
                .astype("string")
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
                .str.lower()
            )

    return df


# ---------------------------------------------------------
#  ID / Code Normalization (optional)
# ---------------------------------------------------------

def normalize_code_fields(df: pd.DataFrame, fields: list) -> pd.DataFrame:
    """
    Normalize fields like country codes, postal codes, category codes.
    - uppercase codes
    - strip whitespace
    """
    logger.debug("Normalizing code-like fields...")

    df = df.copy()
    for col in fields:
        if col in df.columns:
            logger.debug(f"Standardizing code field '{col}'...")
            df[col] = (
                df[col]
                .astype("string")
                .str.strip()
                .str.upper()
            )

    return df


# ---------------------------------------------------------
#  Main Normalization Pipeline
# ---------------------------------------------------------

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Master normalization function.

    This is where you define the schema for:
    - which columns should be numeric
    - which should be datetime
    - which should be categorical
    - which should be code fields
    """

    logger.info("Running normalization pipeline...")
    df = df.copy()

    # ----------------------------------------
    # 1. Define column-level normalization schema (customize!)
    # ----------------------------------------

    numeric_cols = ["age", "price", "amount"]   # Example numeric columns
    datetime_cols = ["created_at", "updated_at", "dob"]  # Example datetime fields
    string_cols = ["name", "category"]          # Example string fields
    code_fields = ["country", "country_code", "postal_code"]  # Example code fields

    # ----------------------------------------
    # 2. Apply normalization steps in order
    # ----------------------------------------

    df = normalize_numeric_columns(df, numeric_cols)
    logger.debug("Numeric normalization complete")

    df = normalize_datetime_columns(df, datetime_cols)
    logger.debug("Datetime normalization complete")

    df = standardize_string_columns(df, string_cols)
    logger.debug("String normalization complete")

    df = normalize_code_fields(df, code_fields)
    logger.debug("Code-field normalization complete")

    # ----------------------------------------
    # Final log
    # ----------------------------------------
    logger.info("Normalization pipeline complete")
    logger.debug(f"Normalized DataFrame: {len(df)} rows, {df.shape[1]} columns")

    return df
