"""
source_api.py  —  Mock JSON-RPC 2.0 API
Simulates a Product Catalog service.
Returns: product_code, name, category, subcategory, unit_cost, supplier, launch_date
"""

import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(10)

CATEGORIES = {
    "Electronics":  ["Laptops", "Monitors", "Peripherals", "Networking"],
    "Office":       ["Furniture", "Stationery", "Storage", "Lighting"],
    "Industrial":   ["Machinery", "Safety", "Tools", "Hydraulics"],
    "Software":     ["Licenses", "Subscriptions", "Support", "Training"],
}
SUPPLIERS = ["TechCorp Ltd", "GlobalParts Inc", "SwiftSupply Co", "ProSource GmbH", "AlphaVend LLC"]

def _make_product(i: int) -> dict:
    cat   = random.choice(list(CATEGORIES))
    subcat= random.choice(CATEGORIES[cat])
    code  = f"PRD-{1000 + i:04d}"
    launch= datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
    return {
        "product_code": code,
        "product_name": f"{fake.word().title()} {subcat[:-1] if subcat.endswith('s') else subcat}",
        "category":     cat,
        "subcategory":  subcat,
        "unit_cost":    round(random.uniform(5.0, 2500.0), 2),
        "supplier":     random.choice(SUPPLIERS),
        "launch_date":  launch.strftime("%Y-%m-%d"),
        "is_active":    random.choice([True, True, True, False]),
    }

_PRODUCTS = [_make_product(i) for i in range(80)]

METHODS = {
    "catalog.getProducts": lambda p: _PRODUCTS[p.get("offset", 0): p.get("offset", 0) + p.get("limit", 80)],
    "catalog.getProduct":  lambda p: next((x for x in _PRODUCTS if x["product_code"] == p.get("code")), None),
}

def call(method: str, params: dict = None, req_id: int = 1) -> dict:
    params = params or {}
    if method not in METHODS:
        return {"jsonrpc": "2.0", "id": req_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"}}
    result = METHODS[method](params)
    return {"jsonrpc": "2.0", "id": req_id, "result": result}
