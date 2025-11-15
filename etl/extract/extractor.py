#extractor.py
import os
import time
import json
import pandas as pd
from .file_handlers import READERS


# ============================================================
# 🔥 1. Universal JSON Flattener (handles ANY nesting)
# ============================================================
def flatten_json(data, prefix=""):
    """
    Recursively flattens ANY JSON structure:
    - nested dicts
    - nested lists
    - lists of dicts
    - lists with mixed types
    - irregular/missing keys
    """
    out = {}

    def recurse(value, key_prefix=""):
        if isinstance(value, dict):
            for k, v in value.items():
                recurse(v, f"{key_prefix}{k}_")

        elif isinstance(value, list):
            for i, item in enumerate(value):
                recurse(item, f"{key_prefix}{i}_")

        else:
            out[key_prefix[:-1]] = value

    recurse(data, prefix)
    return out


# ============================================================
# 🔥 2. Smart JSON Extractor — handles ANY JSON file
# ============================================================
def extract_json_safely(filepath):
    """
    Handles:
    - root is LIST
    - root is DICT
    - DICT contains arrays of dicts
    - DICT contains mixed data
    - flattening AND row-expansion
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []

    # --------------------------------------------------------
    # CASE 1: JSON = LIST at top-level
    # --------------------------------------------------------
    if isinstance(data, list):
        return pd.DataFrame([flatten_json(item) for item in data])

    # --------------------------------------------------------
    # CASE 2: JSON = DICT at root
    # --------------------------------------------------------
    if isinstance(data, dict):

        # Find all lists inside dictionary
        candidate_lists = [(key, v) for key, v in data.items() if isinstance(v, list)]

        if candidate_lists:
            # Choose the largest list as the "rows"
            root_key, largest_list = max(candidate_lists, key=lambda x: len(x[1]))

            for item in largest_list:
                flat = flatten_json(item)

                # include non-list root fields as context
                for k, v in data.items():
                    if not isinstance(v, list):
                        flat[k] = v

                rows.append(flat)

            return pd.DataFrame(rows)

        # --------------------------------------------------------
        # If no lists found: flatten entire dictionary as ONE row
        # --------------------------------------------------------
        return pd.DataFrame([flatten_json(data)])

    # --------------------------------------------------------
    # CASE 3: Primitive types (string/number/null at root)
    # --------------------------------------------------------
    return pd.DataFrame([{"value": data}])


# ============================================================
# 🔥 3. Patch-2: Make list columns rectangular
# ============================================================
def normalize_list_columns(df):
    """
    Ensures lists inside columns have equal length.
    """
    for col in df.columns:
        series = df[col]

        if series.apply(lambda x: isinstance(x, list)).any():
            max_len = series.apply(lambda x: len(x) if isinstance(x, list) else 1).max()

            df[col] = series.apply(
                lambda x:
                    x + [None] * (max_len - len(x))
                    if isinstance(x, list)
                    else [x] + [None] * (max_len - 1)
            )

    return df


# ============================================================
# 🔥 4. Main extract_data() – with smart JSON handling
# ============================================================
def detect_file_type(file_path):
    return os.path.splitext(file_path)[1].lower().replace(".", "")


def extract_data(file_path):
    """
    Universal extraction for CSV, JSON, Excel, HTML, XML, TSV, TXT...
    JSON uses smart recursive flattening.
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_type = detect_file_type(file_path)
        print(f"\n📂 Detected file type: {file_type.upper()}")

        start_time = time.time()

        # ---- JSON gets special handling ----
        if file_type == "json":
            df = extract_json_safely(file_path)
        else:
            reader = READERS.get(file_type)
            if not reader:
                print(f"⚠️ Unsupported file type: {file_type}")
                return pd.DataFrame()
            df = reader(file_path)

        # ---- Patch-2 for list columns ----
        df = normalize_list_columns(df)

        duration = time.time() - start_time
        print(f"✅ Extracted {len(df)} rows from {file_path} in {duration:.2f}s")

        return df

    except Exception as e:
        print(f"❌ Extraction error: {e}")
        return pd.DataFrame()