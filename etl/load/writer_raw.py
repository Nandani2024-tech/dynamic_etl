import logging

logger = logging.getLogger(__name__)

def write_raw(df, db, collection_name):
    """
    Save raw extracted DataFrame to MongoDB collection.
    """
    if df.empty:
        logger.warning("Empty DataFrame received, skipping raw write.")
        return 0

    records = df.to_dict(orient="records")
    result = db[collection_name].insert_many(records)
    logger.info(f"Inserted {len(result.inserted_ids)} raw records into '{collection_name}'")
    return len(result.inserted_ids)
