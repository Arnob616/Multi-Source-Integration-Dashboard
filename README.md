# Multi-Source Unified Data Pipeline
### JSON-RPC API + Excel + CSV → Google Sheets → Looker Studio Dashboard

---

## Project Overview

This pipeline merges **3 data sources** on `product_code`, cleans them with
pandas, pushes a unified dataset to Google Sheets, and powers a Looker Studio
analytics dashboard — fully automated on a daily schedule.

```
┌─────────────────────┐   ┌──────────────────────┐   ┌───────────────────┐
│  Source 1           │   │  Source 2            │   │  Source 3         │
│  JSON-RPC API       │   │  Excel (.xlsx)       │   │  CSV file         │
│  Product Catalog    │   │  Sales Transactions  │   │  Inventory Data   │
│  80 products        │   │  350 rows            │   │  159 rows         │
└────────┬────────────┘   └──────────┬───────────┘   └─────────┬─────────┘
         │                           │                          │
         └───────────────────────────┼──────────────────────────┘
                                     ▼
                          ┌──────────────────────┐
                          │  extractor.py        │
                          │  Pull all 3 sources  │
                          └──────────┬───────────┘
                                     ▼
                          ┌──────────────────────┐
                          │  cleaner_merger.py   │
                          │  Clean + Merge       │
                          │  on product_code     │
                          └──────────┬───────────┘
                                     ▼
                          ┌──────────────────────┐
                          │  sheets_uploader.py  │
                          │  Push to 5 tabs      │
                          └──────────┬───────────┘
                                     ▼
                          ┌──────────────────────┐
                          │  Google Sheets       │
                          │  Unified_Products    │
                          │  Sales_Transactions  │
                          │  Product_Catalog     │
                          │  Inventory_Summary   │
                          │  Pipeline_Log        │
                          └──────────┬───────────┘
                                     ▼
                          ┌──────────────────────┐
                          │  Looker Studio       │
                          │  Dashboard           │
                          └──────────────────────┘
```

---

## File Map

| File | Purpose |
|---|---|
| `pipeline.py` | Main orchestrator + CLI + daily scheduler |
| `source_api.py` | Mock JSON-RPC 2.0 API (product catalog) |
| `generate_sources.py` | Generates Excel + CSV source files with dirty data |
| `extractor.py` | Pulls all 3 sources into raw DataFrames |
| `cleaner_merger.py` | Cleans each source, merges on product_code, computes KPIs |
| `sheets_uploader.py` | Authenticates with Google + pushes all tabs |
| `logger_config.py` | Coloured console + rotating log files |
| `requirements.txt` | Python dependencies |

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Test locally (no Google account needed)
```bash
DRY_RUN=1 python pipeline.py --run-once
```
Runs the full Extract → Clean → Merge cycle.
Saves CSV snapshots to `data/clean/` and skips the Sheets upload.

---

## Connect Google Sheets

### Option A — Service Account (recommended)

1. Go to **console.cloud.google.com** → select or create a project
2. Enable **Google Sheets API** + **Google Drive API**
3. Go to **IAM → Service Accounts** → Create service account
4. Download the **JSON key file**
5. Run:

```bash
export GOOGLE_SERVICE_ACCOUNT_FILE=/path/to/service-account.json
python pipeline.py --run-once
```

The pipeline creates a new spreadsheet and prints the URL.

### Option B — OAuth2 (local dev)

1. Create OAuth2 Desktop credentials in Cloud Console
2. Download `client_secrets.json` into this folder
3. First run opens a browser for consent; saves `token.json` afterwards

```bash
python pipeline.py --run-once
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DRY_RUN` | `0` | `1` = skip upload, save CSVs only |
| `GOOGLE_SERVICE_ACCOUNT_FILE` | — | Path to SA JSON key |
| `GOOGLE_SHEETS_ID` | — | Push to existing sheet |
| `SHEETS_TITLE` | `Unified Data Pipeline – YYYY-MM-DD` | New spreadsheet name |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` |

---

## Scheduling

### Run once
```bash
python pipeline.py --run-once
```

### Daily daemon (runs at 07:00 UTC every day)
```bash
python pipeline.py --schedule 07:00
```

### Custom time
```bash
python pipeline.py --schedule 14:30
```

The scheduler also runs immediately on startup, then repeats daily.
Stop with **Ctrl+C**.

---

## Data Cleaning Rules

### Source 1 — API Catalog
- Normalise `product_code` (uppercase, hyphen-separated)
- Parse `launch_date` to datetime
- Coerce `unit_cost` to float
- Normalise `is_active` to boolean

### Source 2 — Excel Sales
- Standardise `product_code` (handles PRD_1010, prd.1010 → PRD-1010)
- Strip " USD" suffix from `gross_revenue`, coerce to float
- Drop rows with negative `units_sold` (returns)
- Fill null `region` with "Unknown"
- Recalculate `gross_revenue` / `net_revenue` where null
- Add derived: `sale_year_month`, `sale_year`, `sale_quarter`

### Source 3 — CSV Inventory
- Standardise `product_code` (handles lowercase, dot-separated)
- Coerce numeric columns
- Fill null `stock_on_hand` with 0
- Derive `available_stock = stock_on_hand - reserved_stock`
- Recalculate `below_reorder` and `stock_out` flags

### Unified Products (merged)
- LEFT JOIN: catalog ← sales aggregated per product
- LEFT JOIN: result  ← inventory aggregated per product
- Computed KPIs: `revenue_per_unit`, `gross_margin_usd`, `gross_margin_pct`,
  `inventory_turnover`, `has_sales`, `has_inventory`

---

## Looker Studio Dashboard Setup

### Step 1 — Connect your Google Sheet

1. Go to **lookerstudio.google.com** and click **+ Create → Report**
2. Choose **Google Sheets** as the data source
3. Select your spreadsheet and the **Unified_Products** tab → click **Add**
4. Click **Add to report**

### Step 2 — Add a second data source (transactions)

1. In the report, go to **Resource → Manage added data sources → Add a data source**
2. Connect the same spreadsheet, **Sales_Transactions** tab

---

### Step 3 — Build these 6 charts

#### Chart 1 — Revenue by Category (Bar Chart)
- Data source: `Unified_Products`
- Dimension: `category`
- Metric: `total_net_revenue`
- Sort: descending by revenue

#### Chart 2 — Top 10 Products by Revenue (Horizontal Bar)
- Data source: `Unified_Products`
- Dimension: `product_name`
- Metric: `total_net_revenue`
- Filter: Top 10 by `total_net_revenue`

#### Chart 3 — Gross Margin % by Subcategory (Bar Chart)
- Data source: `Unified_Products`
- Dimension: `subcategory`
- Metric: `gross_margin_pct` (aggregation: Average)

#### Chart 4 — Monthly Revenue Trend (Time Series)
- Data source: `Sales_Transactions`
- Dimension: `sale_year_month`
- Metric: `net_revenue` (SUM)

#### Chart 5 — Sales by Region (Pie / Donut Chart)
- Data source: `Sales_Transactions`
- Dimension: `region`
- Metric: `net_revenue` (SUM)

#### Chart 6 — Inventory Health Table
- Data source: `Unified_Products`
- Columns: `product_name`, `category`, `total_stock_on_hand`,
  `total_available_stock`, `any_below_reorder`, `any_stock_out`,
  `inventory_turnover`
- Sort: `any_stock_out` descending (show problems first)

---

### Step 4 — Add KPI Scorecards (top of dashboard)

Add 4 **Scorecard** charts using `Unified_Products`:

| Scorecard Label | Metric | Aggregation |
|---|---|---|
| Total Products | `product_code` | Count Distinct |
| Total Revenue | `total_net_revenue` | Sum |
| Avg Gross Margin | `gross_margin_pct` | Average |
| Products Below Reorder | `any_below_reorder` | Sum (count trues) |

---

### Step 5 — Add Filters

Add **Filter Controls** at the top:
- **Category** filter → dimension: `category`
- **Supplier** filter → dimension: `supplier`
- **Active Products** filter → dimension: `is_active`

---

### Step 6 — Style recommendations

- Set report theme: **Simple Dark** or **Material**
- Add a title text box: "Product Performance Dashboard"
- Add subtitle: "Source: ERP API + Sales Excel + Inventory CSV"
- Group the 4 scorecards in a header row
- Use consistent blue (`#1F4E79`) for bar charts
- Enable **Auto-refresh** (every 4 hours) under Report Settings

---

## Output Structure

```
data/
  raw/
    sales_data.xlsx          ← generated Excel source
    inventory_data.csv       ← generated CSV source
  clean/
    catalog_YYYYMMDD.csv
    sales_clean_YYYYMMDD.csv
    inventory_clean_YYYYMMDD.csv
    unified_products_YYYYMMDD.csv     ← main dashboard source
    unified_transactions_YYYYMMDD.csv
    summary_YYYYMMDD.json

logs/
  pipeline_YYYY-MM-DD.log    ← rotated daily, 30-day retention
```

## Google Sheets Tabs

| Tab | Contents |
|---|---|
| `Unified_Products` | One row per product, all KPIs merged |
| `Sales_Transactions` | Full enriched transaction detail |
| `Product_Catalog` | Clean API catalog |
| `Inventory_Summary` | Clean inventory per product/warehouse |
| `Pipeline_Log` | Append-only run history |
