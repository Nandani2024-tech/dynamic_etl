"""
Run the full ETL pipeline:
1. Extract data from raw files
2. Transform the data (clean, normalize, enrich, convert types)
3. Load raw and processed data into the database
"""

import logging
import pandas as pd
from etl.extract import extract_data, detect_file_type
from etl.transform_layer import run_transform_pipeline
from etl.load import load_data

# ---------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# ETL Runner
# ---------------------------------------------------------
def run_etl(file_path: str):
    """
    Executes full ETL for a single input file.
    """
    logger.info(f"Starting ETL for file: {file_path}")

    # ----------------------
    # 1. EXTRACT
    # ----------------------
    try:
        file_type = detect_file_type(file_path)
        logger.info(f"Detected file type: {file_type}")
        df_raw = extract_data(file_path)
        if df_raw.empty:
            logger.warning("No data extracted. ETL aborted.")
            return
        logger.info(f"Extracted {len(df_raw)} rows")
    except Exception as e:
        logger.exception(f"Extraction failed: {e}")
        return

    # ----------------------
    # 2. TRANSFORM
    # ----------------------
    try:
        df_transformed = run_transform_pipeline(df_raw)
        logger.info(f"Transformation complete. {len(df_transformed)} rows after transform")
    except Exception as e:
        logger.exception(f"Transformation failed: {e}")
        return

    # ----------------------
    # 3. LOAD
    # ----------------------
    try:
        raw_collection = "raw_data"
        processed_collection = "processed_data"
        raw_count, processed_count = load_data(
            raw_df=df_raw,
            processed_df=df_transformed,
            raw_collection=raw_collection,
            processed_collection=processed_collection
        )
        logger.info(f"Load complete: {raw_count} raw rows, {processed_count} processed rows")
    except Exception as e:
        logger.exception(f"Load failed: {e}")
        return

    logger.info("ETL pipeline finished successfully!")


# ---------------------------------------------------------
# CLI / Direct Execution
# ---------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run ETL pipeline on a single file")
    parser.add_argument("file_path", type=str, help="Path to the input file (json, csv, txt, etc.)")
    args = parser.parse_args()

    run_etl(args.file_path)
