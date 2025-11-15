# etl/extract/extract.py

import os
import time
import pandas as pd
from .file_handlers import READERS   # ← FIXED IMPORT


def detect_file_type(file_path):
    """
    Detects file type based on its extension.
    """
    ext = os.path.splitext(file_path)[1].lower().replace('.', '')
    return ext


def extract_data(file_path):
    """
    Extracts data using the correct reader from file_handlers,
    measures time taken, and handles errors gracefully.
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_type = detect_file_type(file_path)
        print(f"\n📂 Detected file type: {file_type.upper()}")

        reader = READERS.get(file_type)
        if reader is None:
            print(f"⚠️ Unsupported file type: {file_type}")
            return pd.DataFrame()

        # Start timing extraction
        start_time = time.time()
        df = reader(file_path)
        duration = time.time() - start_time

        # Flatten nested JSON if detected
        if file_type == "json" and not df.empty:
            try:
                df = pd.json_normalize(df.to_dict(orient="records"))
            except Exception:
                pass

        record_count = len(df)
        print(f"✅ Extracted {record_count} records from {file_path} in {duration:.2f}s")

        if not df.empty:
            print(df.head())
        else:
            print("⚠️ No data extracted.")

        return df

    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        return pd.DataFrame()


# ==========================================================
# Standalone Runner (Optional)
# ==========================================================
if __name__ == "__main__":
    BASE = os.path.join(os.path.dirname(__file__), "..", "..")
    DATA_DIR = os.path.join(BASE, "data")

    test_files = [
        os.path.join(DATA_DIR, "day1.json"),
        os.path.join(DATA_DIR, "day2.csv"),
        os.path.join(DATA_DIR, "day3.html"),
        os.path.join(DATA_DIR, "day4.txt"),
    ]

    for file in test_files:
        print("\n-----------------------------------------")
        extract_data(file)
