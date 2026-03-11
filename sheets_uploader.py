"""
sheets_uploader.py  —  Pushes all merged datasets to Google Sheets

Tabs created:
  1. Unified_Products     ← one row per product (the main dashboard source)
  2. Sales_Transactions   ← enriched transaction detail
  3. Catalog              ← clean API catalog
  4. Inventory_Summary    ← aggregated inventory per product
  5. Pipeline_Log         ← append-only run history

Auth (priority order):
  1. GOOGLE_SERVICE_ACCOUNT_FILE env var  (service-account JSON)
  2. GOOGLE_APPLICATION_CREDENTIALS env var  (ADC)
  3. OAuth2 browser flow  (writes token.json locally)

Set GOOGLE_SHEETS_ID to push to existing sheet, else a new one is created.
"""

import logging
import os
from datetime import datetime
from typing import Optional
import pandas as pd

logger = logging.getLogger("pipeline.sheets")

SCOPES = ["https://spreadsheets.google.com/feeds",
          "https://www.googleapis.com/auth/drive"]

def _gspread():
    import gspread
    return gspread

def _auth():
    import gspread
    from google.oauth2 import service_account
    sa = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    if sa and os.path.exists(sa):
        creds = service_account.Credentials.from_service_account_file(sa, scopes=SCOPES)
        logger.info("Auth: service-account")
        return gspread.authorize(creds)
    adc = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if adc:
        import google.auth
        creds, _ = google.auth.default(scopes=SCOPES)
        logger.info("Auth: ADC")
        return gspread.authorize(creds)
    # OAuth2 fallback
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    token = "token.json"
    creds = None
    if os.path.exists(token):
        creds = Credentials.from_authorized_user_file(token, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("client_secrets.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token, "w") as f:
            f.write(creds.to_json())
    logger.info("Auth: OAuth2")
    return gspread.authorize(creds)

def _ensure_ws(ss, title, rows=10000, cols=60):
    try:
        return ss.worksheet(title)
    except Exception:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)

def _push_df(ws, df: pd.DataFrame):
    ws.clear()
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].astype(str).replace("NaT", "")
        elif pd.api.types.is_bool_dtype(out[col]):
            out[col] = out[col].astype(str)
        else:
            out[col] = out[col].fillna("").astype(str)
    ws.update([out.columns.tolist()] + out.values.tolist(),
              value_input_option="USER_ENTERED")

SHEET_MAP = [
    ("unified_products",     "Unified_Products"),
    ("unified_transactions", "Sales_Transactions"),
    ("catalog",              "Product_Catalog"),
    ("inventory_clean",      "Inventory_Summary"),
]

class SheetsUploader:
    def __init__(self):
        self.sheet_id    = os.getenv("GOOGLE_SHEETS_ID", "")
        self.sheet_title = os.getenv("SHEETS_TITLE",
                           f"Unified Data Pipeline – {datetime.utcnow():%Y-%m-%d}")

    def push(self, datasets: dict[str, pd.DataFrame], run_ts: Optional[datetime] = None) -> str:
        run_ts = run_ts or datetime.utcnow()
        client = _auth()

        if self.sheet_id:
            try:
                ss = client.open_by_key(self.sheet_id)
                logger.info("Opened sheet: %s", self.sheet_id)
            except Exception:
                ss = client.create(self.sheet_title)
                logger.info("Created new sheet: %s", ss.id)
        else:
            ss = client.create(self.sheet_title)
            ss.share(None, perm_type="anyone", role="reader")
            logger.info("Created new public sheet: %s", ss.id)

        for key, tab_name in SHEET_MAP:
            if key not in datasets:
                continue
            df = datasets[key]
            ws = _ensure_ws(ss, tab_name)
            _push_df(ws, df)
            logger.info("  ✓ '%s' → %s  (%d rows)", key, tab_name, len(df))

        self._write_log(ss, datasets, run_ts)
        url = f"https://docs.google.com/spreadsheets/d/{ss.id}"
        logger.info("Sheets URL: %s", url)
        return url

    def _write_log(self, ss, datasets, run_ts):
        ws = _ensure_ws(ss, "Pipeline_Log", rows=1000, cols=15)
        try:
            existing = ws.get_all_values()
        except Exception:
            existing = []

        row = [
            run_ts.isoformat(),
            len(datasets.get("catalog", [])),
            len(datasets.get("sales_clean", [])),
            len(datasets.get("inventory_clean", [])),
            len(datasets.get("unified_products", [])),
            len(datasets.get("unified_transactions", [])),
            "success",
        ]
        header = [["run_timestamp","catalog_rows","sales_rows","inventory_rows",
                   "unified_product_rows","transaction_rows","status"]]
        if not existing:
            ws.update(header + [row], value_input_option="USER_ENTERED")
        else:
            ws.insert_rows([row], row=len(existing) + 1)
        logger.info("  ✓ Pipeline_Log updated")
