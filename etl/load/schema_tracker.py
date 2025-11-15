from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def get_previous_schema(db, collection_name):
    """
    Get the most recent schema saved for a collection/file.
    """
    return db.schema_logs.find_one(
        {"collection_name": collection_name},
        sort=[("timestamp", -1)]
    )

def save_schema(db, collection_name, df):
    """
    Save the current schema of a DataFrame to schema_logs.
    """
    schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    record = {
        "collection_name": collection_name,
        "schema": schema,
        "row_count": len(df),
        "timestamp": datetime.utcnow()
    }
    db.schema_logs.insert_one(record)
    logger.info(f"Schema saved for '{collection_name}' with {len(df)} rows.")
