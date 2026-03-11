"""
extractor.py  —  Pulls data from all 3 sources into raw DataFrames
  Source 1: JSON-RPC API  → product catalog
  Source 2: Excel file    → sales transactions
  Source 3: CSV file      → inventory snapshot
"""

import logging
import pandas as pd
from pathlib import Path
from source_api import call

logger = logging.getLogger("pipeline.extractor")

class MultiSourceExtractor:
    def __init__(self, excel_path: str, csv_path: str):
        self.excel_path = excel_path
        self.csv_path   = csv_path

    def extract_api(self) -> pd.DataFrame:
        resp = call("catalog.getProducts", {"limit": 100})
        if "error" in resp:
            raise RuntimeError(f"API error: {resp['error']['message']}")
        df = pd.DataFrame(resp["result"])
        logger.info("  [API]   %d product catalog records", len(df))
        return df

    def extract_excel(self) -> pd.DataFrame:
        df = pd.read_excel(self.excel_path, sheet_name="Sales_Transactions", dtype=str)
        logger.info("  [Excel] %d sales transaction rows", len(df))
        return df

    def extract_csv(self) -> pd.DataFrame:
        df = pd.read_csv(self.csv_path, dtype=str)
        logger.info("  [CSV]   %d inventory rows", len(df))
        return df

    def extract_all(self) -> dict[str, pd.DataFrame]:
        logger.info("── Extracting from 3 sources ──")
        result = {
            "catalog":   self.extract_api(),
            "sales":     self.extract_excel(),
            "inventory": self.extract_csv(),
        }
        logger.info("── Extraction complete ──")
        return result
