"""
pipeline.py  —  Main orchestrator
Combines JSON-RPC API + Excel + CSV → cleans → merges on product_code → Google Sheets

Usage:
  python pipeline.py --run-once                  # single run
  python pipeline.py --schedule 07:00            # daily daemon at 07:00 UTC
  DRY_RUN=1 python pipeline.py --run-once        # skip Sheets upload (test mode)

Env vars:
  GOOGLE_SERVICE_ACCOUNT_FILE   path to service-account JSON
  GOOGLE_SHEETS_ID              existing spreadsheet (optional)
  SHEETS_TITLE                  spreadsheet title for new sheets
  DRY_RUN                       1 = skip upload
  LOG_LEVEL                     DEBUG | INFO | WARNING
"""

import argparse, json, logging, os, signal, sys, time
from datetime import datetime
from pathlib import Path

import pandas as pd
import schedule

from logger_config   import setup_logging
from generate_sources import generate_excel, generate_csv
from extractor       import MultiSourceExtractor
from cleaner_merger  import clean_and_merge

logger = logging.getLogger("pipeline.main")

BASE   = Path(__file__).parent
RAW    = BASE / "data" / "raw"
CLEAN  = BASE / "data" / "clean"
EXCEL  = str(RAW / "sales_data.xlsx")
CSV    = str(RAW / "inventory_data.csv")

for d in (RAW, CLEAN): d.mkdir(parents=True, exist_ok=True)

# ── pipeline run ──────────────────────────────────────────────────────────────

def run_pipeline() -> dict:
    ts    = datetime.utcnow()
    start = ts
    logger.info("═"*62)
    logger.info("  UNIFIED PIPELINE  started  %s", ts.isoformat())
    logger.info("═"*62)
    summary = {"run_timestamp": ts.isoformat(), "status": "running"}

    try:
        # 0 ── Generate source files (if missing) ──────────────────────────────
        if not Path(EXCEL).exists():
            logger.info("[0/4] Generating source files …")
            generate_excel(EXCEL)
            generate_csv(CSV)
        else:
            logger.info("[0/4] Source files already present, skipping generation.")

        # 1 ── Extract ─────────────────────────────────────────────────────────
        logger.info("[1/4] Extracting from JSON-RPC API + Excel + CSV …")
        raw = MultiSourceExtractor(EXCEL, CSV).extract_all()
        summary["raw"] = {k: len(v) for k, v in raw.items()}

        # 2 ── Clean & Merge ───────────────────────────────────────────────────
        logger.info("[2/4] Cleaning & merging on product_code …")
        datasets = clean_and_merge(raw)
        summary["clean"] = {k: len(v) for k, v in datasets.items()}

        # 3 ── Save local snapshots ────────────────────────────────────────────
        logger.info("[3/4] Saving local CSV snapshots …")
        tag = ts.strftime("%Y%m%d_%H%M%S")
        for name, df in datasets.items():
            p = CLEAN / f"{name}_{tag}.csv"
            df.to_csv(p, index=False)
            logger.info("  Saved %s  (%d rows)", p.name, len(df))
        qr_path = CLEAN / f"summary_{tag}.json"
        with open(qr_path, "w") as f:
            json.dump(summary["clean"], f, indent=2)

        # 4 ── Upload ──────────────────────────────────────────────────────────
        if os.getenv("DRY_RUN", "0") == "1":
            logger.warning("[4/4] DRY_RUN — skipping Google Sheets upload.")
            summary["sheets_url"] = "DRY_RUN"
        else:
            logger.info("[4/4] Uploading to Google Sheets …")
            from sheets_uploader import SheetsUploader
            url = SheetsUploader().push(datasets, run_ts=ts)
            summary["sheets_url"] = url

        dur = round((datetime.utcnow() - start).total_seconds(), 2)
        summary.update({"status": "success", "duration_sec": dur})
        logger.info("═"*62)
        logger.info("  DONE  %.1fs  →  %s", dur, summary.get("sheets_url",""))
        logger.info("═"*62)

    except Exception as e:
        summary.update({"status": "error", "error": str(e)})
        logger.exception("Pipeline failed: %s", e)

    return summary

# ── scheduler ──────────────────────────────────────────────────────────────────

def start_scheduler(run_time: str):
    logger.info("Scheduling daily run at %s UTC …", run_time)
    schedule.every().day.at(run_time).do(run_pipeline)
    run_pipeline()                          # immediate first run
    def _stop(sig, _): logger.info("Stopping."); sys.exit(0)
    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)
    while True:
        schedule.run_pending()
        time.sleep(60)

# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Multi-source → Sheets ETL Pipeline")
    g  = ap.add_mutually_exclusive_group()
    g.add_argument("--run-once",  action="store_true")
    g.add_argument("--schedule",  metavar="HH:MM", default="07:00")
    ap.add_argument("--log-level", default=os.getenv("LOG_LEVEL","INFO"))
    args = ap.parse_args()
    setup_logging(args.log_level)
    if args.run_once:
        s = run_pipeline()
        print("\n── Summary ──")
        print(json.dumps(s, indent=2, default=str))
        sys.exit(0 if s["status"] == "success" else 1)
    else:
        start_scheduler(args.schedule)

if __name__ == "__main__":
    main()
