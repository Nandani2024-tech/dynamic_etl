import logging
from .db_config import get_db_client
from .writer_raw import write_raw
from .writer_processed import write_processed
from .schema_tracker import save_schema

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def load_data(raw_df, processed_df, raw_collection="raw_data", processed_collection="processed_data"):
    """
    Load data into database:
    1. Save raw data
    2. Save processed data
    3. Track schema versions
    """
    db = get_db_client()

    # Save raw data
    logger.info("Loading raw data...")
    raw_count = write_raw(raw_df, db, raw_collection)

    # Save processed data
    logger.info("Loading processed data...")
    processed_count = write_processed(processed_df, db, processed_collection)

    # Save schemas
    logger.info("Saving schema for raw and processed data...")
    save_schema(db, raw_collection, raw_df)
    save_schema(db, processed_collection, processed_df)

    logger.info(f"Load complete: {raw_count} raw rows, {processed_count} processed rows")
    return raw_count, processed_count


# Optional CLI test
if __name__ == "__main__":
    import pandas as pd
    df_raw = pd.DataFrame({"id": [1,2], "name": ["Alice","Bob"]})
    df_processed = df_raw.copy()
    load_data(df_raw, df_processed)
