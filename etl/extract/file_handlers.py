# etl/extract/file_handlers.py

import json
import pandas as pd


def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return pd.DataFrame(data)


READERS = {
    "json": read_json,
    "csv": pd.read_csv,
    "txt": lambda path: pd.read_csv(path, sep=",", engine="python"),
    "html": lambda path: pd.read_html(path, flavor="bs4")[0],
    "xlsx": lambda path: pd.read_excel(path, engine="openpyxl"),
    "xls": lambda path: pd.read_excel(path, engine="xlrd", dtype=str),
    "tsv": lambda path: pd.read_csv(path, sep="\t"),
    "xml": pd.read_xml,
    "parquet": pd.read_parquet,
}
