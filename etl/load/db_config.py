import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()  # Load environment variables from .env

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("MONGO_DB")

def get_db_client():
    """
    Returns a connected MongoClient instance.
    """
    if not MONGO_URI:
        raise ValueError("MONGO_URI not set in environment")
    client = MongoClient(MONGO_URI)
    return client[DATABASE_NAME]
