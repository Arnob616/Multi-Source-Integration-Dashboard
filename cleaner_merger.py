"""
cleaner_merger.py  —  Cleans each source then merges on product_code

Cleaning rules
──────────────
  catalog   : normalise booleans, parse launch_date
  sales     : standardise product_code, coerce numeric cols, drop negatives, fill nulls
  inventory : standardise product_code, coerce numerics, fill missing stock

Merge strategy
──────────────
  1. Aggregate sales → one row per product_code
  2. Aggregate inventory → one row per product_code (summed across warehouses)
  3. LEFT JOIN catalog ← sales  (keep all catalog products)
  4. LEFT JOIN result  ← inventory
  Produces: unified_products  (one row per product_code, all metrics attached)

Also produces: unified_transactions  (full sales detail + catalog fields joined in)
"""

import logging
import re
import pandas as pd
import numpy as np

logger = logging.getLogger("pipeline.cleaner")

# ─── helpers ──────────────────────────────────────────────────────────────────

def _normalise_code(series: pd.Series) -> pd.Series:
    """
    Canonical product code: uppercase, hyphens, strip spaces.
    Handles: PRD_1010, prd.1010, PRD-1010  → PRD-1010
    """
    return (
        series.astype(str)
              .str.strip()
              .str.upper()
              .str.replace(r"[_\.\s]+", "-", regex=True)
              .str.replace(r"[^A-Z0-9\-]", "", regex=True)
    )

def _to_numeric(series: pd.Series) -> pd.Series:
    """Strip non-numeric characters then coerce to float."""
    cleaned = series.astype(str).str.replace(r"[^\d\.\-]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")

def _report(name, before, after, notes):
    logger.info("  [%s] %d → %d rows | fixes: %s", name, before, after, "; ".join(notes))

# ─── source cleaners ──────────────────────────────────────────────────────────

def clean_catalog(df: pd.DataFrame) -> pd.DataFrame:
    notes = []
    raw_n = len(df)
    df = df.copy()
    df["product_code"] = _normalise_code(df["product_code"])
    df["launch_date"]  = pd.to_datetime(df["launch_date"], errors="coerce")
    df["unit_cost"]    = pd.to_numeric(df["unit_cost"], errors="coerce")
    df["is_active"]    = df["is_active"].astype(str).str.lower().isin(["true", "1", "yes"])
    notes.append("codes normalised; dates parsed; is_active → bool")
    df = df.drop_duplicates("product_code")
    _report("catalog", raw_n, len(df), notes)
    return df.reset_index(drop=True)

def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    notes = []
    raw_n = len(df)
    df = df.copy()

    df["product_code"] = _normalise_code(df["product_code"])
    df["sale_date"]    = pd.to_datetime(df["sale_date"], errors="coerce")
    df["region"]       = df["region"].fillna("Unknown").str.strip()

    for col in ["units_sold", "unit_price", "discount_pct", "gross_revenue", "net_revenue"]:
        df[col] = _to_numeric(df[col])

    # Drop returns (negative units)
    neg = (df["units_sold"] < 0).sum()
    df  = df[df["units_sold"].fillna(0) >= 0]
    notes.append(f"dropped {neg} negative-unit rows")

    # Recalculate revenues where corrupted
    mask = df["gross_revenue"].isna()
    df.loc[mask, "gross_revenue"] = (df.loc[mask,"units_sold"] * df.loc[mask,"unit_price"]).round(2)
    mask2 = df["net_revenue"].isna()
    df.loc[mask2, "net_revenue"]  = (df.loc[mask2,"gross_revenue"] * (1 - df.loc[mask2,"discount_pct"].fillna(0))).round(2)
    notes.append("revenues recalculated where null")

    df["sale_year_month"] = df["sale_date"].dt.to_period("M").astype(str)
    df["sale_year"]       = df["sale_date"].dt.year
    df["sale_quarter"]    = "Q" + df["sale_date"].dt.quarter.astype(str) + " " + df["sale_date"].dt.year.astype(str)

    df = df.drop_duplicates("transaction_id")
    notes.append("region nulls → 'Unknown'; derived date columns added")
    _report("sales", raw_n, len(df), notes)
    return df.reset_index(drop=True)

def clean_inventory(df: pd.DataFrame) -> pd.DataFrame:
    notes = []
    raw_n = len(df)
    df = df.copy()

    df["product_code"] = _normalise_code(df["product_code"])

    for col in ["stock_on_hand","reserved_stock","reorder_point","reorder_qty","unit_weight_kg","stock_value_usd"]:
        df[col] = _to_numeric(df[col])

    df["stock_on_hand"]   = df["stock_on_hand"].fillna(0)
    df["reserved_stock"]  = df["reserved_stock"].fillna(0)
    df["available_stock"] = (df["stock_on_hand"] - df["reserved_stock"]).clip(lower=0)
    df["below_reorder"]   = df["stock_on_hand"] < df["reorder_point"]
    df["stock_out"]       = df["stock_on_hand"] == 0
    df["last_counted_date"] = pd.to_datetime(df["last_counted_date"], errors="coerce")
    notes.append("codes normalised; nulls filled; available_stock derived")
    _report("inventory", raw_n, len(df), notes)
    return df.reset_index(drop=True)

# ─── aggregators ──────────────────────────────────────────────────────────────

def aggregate_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse sales to one row per product_code."""
    agg = df.groupby("product_code", as_index=False).agg(
        total_transactions = ("transaction_id",   "count"),
        total_units_sold   = ("units_sold",        "sum"),
        total_gross_revenue= ("gross_revenue",     "sum"),
        total_net_revenue  = ("net_revenue",       "sum"),
        avg_unit_price     = ("unit_price",         "mean"),
        avg_discount_pct   = ("discount_pct",       "mean"),
        first_sale_date    = ("sale_date",          "min"),
        last_sale_date     = ("sale_date",          "max"),
        regions_sold_in    = ("region",            lambda x: "|".join(sorted(x.dropna().unique()))),
        top_region         = ("region",            lambda x: x.value_counts().idxmax() if len(x) else "Unknown"),
    )
    agg["total_gross_revenue"] = agg["total_gross_revenue"].round(2)
    agg["total_net_revenue"]   = agg["total_net_revenue"].round(2)
    agg["avg_unit_price"]      = agg["avg_unit_price"].round(2)
    agg["avg_discount_pct"]    = agg["avg_discount_pct"].round(4)
    return agg

def aggregate_inventory(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse inventory to one row per product_code (sum across warehouses)."""
    agg = df.groupby("product_code", as_index=False).agg(
        total_stock_on_hand  = ("stock_on_hand",    "sum"),
        total_reserved_stock = ("reserved_stock",   "sum"),
        total_available_stock= ("available_stock",  "sum"),
        total_stock_value_usd= ("stock_value_usd",  "sum"),
        warehouse_count      = ("warehouse",        "nunique"),
        warehouses           = ("warehouse",        lambda x: "|".join(sorted(x.unique()))),
        any_below_reorder    = ("below_reorder",    "any"),
        any_stock_out        = ("stock_out",        "any"),
    )
    agg["total_stock_value_usd"] = agg["total_stock_value_usd"].round(2)
    return agg

# ─── merge orchestrator ───────────────────────────────────────────────────────

def merge_all(catalog: pd.DataFrame, sales: pd.DataFrame, inventory: pd.DataFrame):
    """
    Returns:
        unified_products     — one row per product_code, all 3 sources merged
        unified_transactions — full sales rows enriched with catalog fields
    """
    logger.info("── Merging 3 sources on product_code ──")

    sales_agg = aggregate_sales(sales)
    inv_agg   = aggregate_inventory(inventory)

    # Unified products (catalog is the spine)
    unified = catalog.merge(sales_agg, on="product_code", how="left")
    unified = unified.merge(inv_agg,   on="product_code", how="left")

    # Derived KPIs
    unified["revenue_per_unit"] = (unified["total_net_revenue"] / unified["total_units_sold"]).round(2)
    unified["gross_margin_usd"] = (unified["total_net_revenue"] - unified["unit_cost"] * unified["total_units_sold"]).round(2)
    unified["gross_margin_pct"] = (unified["gross_margin_usd"] / unified["total_net_revenue"].replace(0, np.nan) * 100).round(2)
    unified["inventory_turnover"]= (unified["total_units_sold"] / unified["total_stock_on_hand"].replace(0, np.nan)).round(2)
    unified["has_sales"]         = unified["total_transactions"].notna()
    unified["has_inventory"]     = unified["total_stock_on_hand"].notna()

    # Unified transactions (full detail, catalog fields joined)
    catalog_slim = catalog[["product_code","product_name","category","subcategory","supplier","unit_cost","is_active"]]
    txns = sales.merge(catalog_slim, on="product_code", how="left")

    matched_p = unified["has_sales"].sum()
    unmatched_p = (~unified["has_sales"]).sum()
    logger.info(
        "  Unified products: %d rows | %d with sales | %d catalog-only",
        len(unified), matched_p, unmatched_p
    )
    logger.info("  Unified transactions: %d rows", len(txns))
    logger.info("── Merge complete ──")
    return unified, txns

# ─── entry point ──────────────────────────────────────────────────────────────

def clean_and_merge(raw: dict[str, pd.DataFrame]):
    logger.info("── Cleaning all sources ──")
    catalog   = clean_catalog(raw["catalog"])
    sales     = clean_sales(raw["sales"])
    inventory = clean_inventory(raw["inventory"])
    logger.info("── Cleaning complete ──")
    unified, txns = merge_all(catalog, sales, inventory)
    return {
        "catalog":              catalog,
        "sales_clean":          sales,
        "inventory_clean":      inventory,
        "unified_products":     unified,
        "unified_transactions": txns,
    }
