"""
etl_pipeline.py
Extract → Transform → Load pipeline for the Retail Data Warehouse.

Sources : CSV files in ../data/
Target  : SQLite database  ../warehouse/retail_dw.db
           (swap connection string for PostgreSQL in production)
"""

import csv
import sqlite3
import os
import sys
from datetime import date, datetime

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR      = os.path.join(BASE_DIR, "data")
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")
DB_PATH       = os.path.join(WAREHOUSE_DIR, "retail_dw.db")
SCHEMA_PATH   = os.path.join(WAREHOUSE_DIR, "schema.sql")


# ── Helpers ───────────────────────────────────────────────────────────────────
def read_csv(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# ── Extract ───────────────────────────────────────────────────────────────────
def extract():
    log("EXTRACT — loading source CSV files")
    data = {
        "customers":   read_csv("customers.csv"),
        "products":    read_csv("products.csv"),
        "orders":      read_csv("orders.csv"),
        "order_items": read_csv("order_items.csv"),
    }
    for k, v in data.items():
        log(f"  {k}: {len(v):,} rows")
    return data


# ── Transform ─────────────────────────────────────────────────────────────────
def build_date_dim(orders):
    """Generate one row per unique order date."""
    MONTHS = ["January","February","March","April","May","June",
              "July","August","September","October","November","December"]
    DAYS   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

    seen   = {}
    for row in orders:
        d = date.fromisoformat(row["order_date"])
        date_id = int(d.strftime("%Y%m%d"))
        if date_id not in seen:
            seen[date_id] = {
                "date_id":      date_id,
                "full_date":    str(d),
                "day":          d.day,
                "month":        d.month,
                "month_name":   MONTHS[d.month - 1],
                "quarter":      (d.month - 1) // 3 + 1,
                "year":         d.year,
                "week_of_year": d.isocalendar()[1],
                "day_of_week":  DAYS[d.weekday()],
                "is_weekend":   d.weekday() >= 5,
            }
    return list(seen.values())


def transform(data):
    log("TRANSFORM — building dimensional model")

    customers = data["customers"]
    products  = data["products"]
    orders    = data["orders"]
    items     = data["order_items"]

    # dim_date
    dim_date = build_date_dim(orders)
    date_lookup = {r["full_date"]: r["date_id"] for r in dim_date}

    # dim_customer
    dim_customer = [
        {
            "customer_id": int(c["customer_id"]),
            "first_name":  c["first_name"],
            "last_name":   c["last_name"],
            "full_name":   f"{c['first_name']} {c['last_name']}",
            "email":       c["email"],
            "city":        c["city"],
            "segment":     c["segment"],
        }
        for c in customers
    ]

    # dim_product
    dim_product = [
        {
            "product_id":   int(p["product_id"]),
            "product_name": p["product_name"],
            "category":     p["category"],
            "unit_cost":    float(p["unit_cost"]),
            "supplier":     p["supplier"],
        }
        for p in products
    ]

    # channel lookup  (built from dim_channel seeds in schema)
    channel_map = {"online": 1, "mobile_app": 2, "store": 3}

    # product cost lookup
    cost_lookup = {int(p["product_id"]): float(p["unit_cost"]) for p in products}

    # order lookup
    order_lookup = {int(o["order_id"]): o for o in orders}

    # fact_sales
    fact_sales = []
    skipped = 0
    for item in items:
        oid        = int(item["order_id"])
        order      = order_lookup.get(oid)
        if not order:
            skipped += 1
            continue

        product_id = int(item["product_id"])
        qty        = int(item["quantity"])
        unit_price = float(item["unit_price"])
        discount   = float(item["discount"])
        unit_cost  = cost_lookup.get(product_id, 0.0)

        gross_revenue = round(qty * unit_price, 4)
        net_revenue   = round(gross_revenue * (1 - discount), 4)
        cogs          = round(qty * unit_cost, 4)
        gross_margin  = round(net_revenue - cogs, 4)

        channel_id = channel_map.get(order["channel"], 1)
        date_id    = date_lookup.get(order["order_date"])

        fact_sales.append({
            "order_id":       oid,
            "order_item_id":  int(item["order_item_id"]),
            "date_id":        date_id,
            "customer_id":    int(order["customer_id"]),
            "product_id":     product_id,
            "channel_id":     channel_id,
            "quantity":       qty,
            "unit_price":     unit_price,
            "unit_cost":      unit_cost,
            "discount":       discount,
            "gross_revenue":  gross_revenue,
            "net_revenue":    net_revenue,
            "cogs":           cogs,
            "gross_margin":   gross_margin,
            "status":         order["status"],
        })

    if skipped:
        log(f"  ⚠ skipped {skipped} items with no matching order")

    log(f"  dim_date:     {len(dim_date):,} rows")
    log(f"  dim_customer: {len(dim_customer):,} rows")
    log(f"  dim_product:  {len(dim_product):,} rows")
    log(f"  fact_sales:   {len(fact_sales):,} rows")

    return dim_date, dim_customer, dim_product, fact_sales


# ── Load ──────────────────────────────────────────────────────────────────────
def load(dim_date, dim_customer, dim_product, fact_sales):
    log("LOAD — writing to SQLite data warehouse")

    # Fresh DB each run
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")

    # Apply schema
    with open(SCHEMA_PATH, "r") as f:
        cur.executescript(f.read())
    conn.commit()

    def bulk_insert(table, rows, cols):
        placeholders = ",".join(["?"] * len(cols))
        sql = f"INSERT OR REPLACE INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
        cur.executemany(sql, [tuple(r[c] for c in cols) for r in rows])
        conn.commit()
        log(f"  ✓ {table}: {len(rows):,} rows loaded")

    bulk_insert("dim_date",
                dim_date,
                ["date_id","full_date","day","month","month_name",
                 "quarter","year","week_of_year","day_of_week","is_weekend"])

    bulk_insert("dim_customer",
                dim_customer,
                ["customer_id","first_name","last_name","full_name","email","city","segment"])

    bulk_insert("dim_product",
                dim_product,
                ["product_id","product_name","category","unit_cost","supplier"])

    bulk_insert("fact_sales",
                fact_sales,
                ["order_id","order_item_id","date_id","customer_id","product_id",
                 "channel_id","quantity","unit_price","unit_cost","discount",
                 "gross_revenue","net_revenue","cogs","gross_margin","status"])

    conn.close()
    log(f"  Database saved → {DB_PATH}")


# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log("=" * 55)
    log("Retail Data Warehouse — ETL Pipeline")
    log("=" * 55)
    raw                                          = extract()
    dim_date, dim_customer, dim_product, fact    = transform(raw)
    load(dim_date, dim_customer, dim_product, fact)
    log("=" * 55)
    log("Pipeline complete ✓")
