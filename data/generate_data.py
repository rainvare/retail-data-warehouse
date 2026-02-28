"""
generate_data.py
Generates synthetic retail data and saves it as CSV files.
No external dependencies required beyond Python stdlib + csv/random.
"""

import csv
import random
import os
from datetime import date, timedelta

random.seed(42)

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Config ──────────────────────────────────────────────────────────────────
N_CUSTOMERS   = 500
N_PRODUCTS    = 80
N_ORDERS      = 3000
START_DATE    = date(2022, 1, 1)
END_DATE      = date(2024, 12, 31)

# ── Helpers ─────────────────────────────────────────────────────────────────
def rand_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def write_csv(filename, fieldnames, rows):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓ {filename} ({len(rows)} rows)")

# ── Customers ────────────────────────────────────────────────────────────────
FIRST_NAMES = ["Emma","Liam","Olivia","Noah","Ava","Ethan","Sophia","Mason",
               "Isabella","William","Mia","James","Charlotte","Benjamin","Amelia",
               "Lucas","Harper","Henry","Evelyn","Alexander","Luna","Sebastian",
               "Camila","Jack","Aria","Daniel","Scarlett","Michael","Victoria","Owen"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller",
               "Davis","Martinez","Wilson","Anderson","Taylor","Thomas","Jackson",
               "White","Harris","Martin","Thompson","Young","Robinson","Lewis","Walker"]
CITIES      = ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia",
               "San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville",
               "Buenos Aires","Bogotá","Lima","Santiago","Mexico City","São Paulo"]
SEGMENTS    = ["Retail", "Wholesale", "Online"]

customers = []
for i in range(1, N_CUSTOMERS + 1):
    customers.append({
        "customer_id":   i,
        "first_name":    random.choice(FIRST_NAMES),
        "last_name":     random.choice(LAST_NAMES),
        "email":         f"customer{i}@example.com",
        "city":          random.choice(CITIES),
        "segment":       random.choice(SEGMENTS),
        "registered_at": rand_date(date(2020, 1, 1), START_DATE),
    })

write_csv("customers.csv",
          ["customer_id","first_name","last_name","email","city","segment","registered_at"],
          customers)

# ── Products ─────────────────────────────────────────────────────────────────
CATEGORIES = {
    "Electronics":  ["Laptop","Smartphone","Tablet","Headphones","Smartwatch","Keyboard","Monitor"],
    "Clothing":     ["T-Shirt","Jeans","Jacket","Sneakers","Dress","Hoodie","Shorts"],
    "Home & Garden":["Blender","Coffee Maker","Vacuum","Lamp","Pillow","Rug","Plant Pot"],
    "Sports":       ["Running Shoes","Yoga Mat","Bicycle","Dumbbell","Tennis Racket","Backpack"],
    "Books":        ["Fiction Novel","Self-Help","Cookbook","Science","Biography","Children Book"],
}

products = []
pid = 1
for category, names in CATEGORIES.items():
    for name in names:
        base_price = round(random.uniform(8, 900), 2)
        cost       = round(base_price * random.uniform(0.4, 0.65), 2)
        products.append({
            "product_id":   pid,
            "product_name": name,
            "category":     category,
            "unit_price":   base_price,
            "unit_cost":    cost,
            "supplier":     f"Supplier_{random.randint(1, 15)}",
        })
        pid += 1
        if pid > N_PRODUCTS:
            break
    if pid > N_PRODUCTS:
        break

write_csv("products.csv",
          ["product_id","product_name","category","unit_price","unit_cost","supplier"],
          products)

# ── Orders + Order Items ─────────────────────────────────────────────────────
STATUSES  = ["completed", "completed", "completed", "returned", "cancelled"]
CHANNELS  = ["online", "online", "store", "store", "mobile_app"]
PAYMENT   = ["credit_card", "debit_card", "paypal", "bank_transfer", "cash"]

orders      = []
order_items = []
item_id     = 1

for oid in range(1, N_ORDERS + 1):
    order_date = rand_date(START_DATE, END_DATE)
    customer   = random.choice(customers)
    status     = random.choice(STATUSES)
    channel    = random.choice(CHANNELS)
    payment    = random.choice(PAYMENT)
    n_items    = random.randint(1, 5)

    order_total = 0
    selected_products = random.sample(products, k=min(n_items, len(products)))

    for product in selected_products:
        qty      = random.randint(1, 4)
        discount = round(random.choice([0, 0, 0, 0.05, 0.10, 0.15]), 2)
        price    = product["unit_price"]
        subtotal = round(price * qty * (1 - discount), 2)
        order_total += subtotal

        order_items.append({
            "order_item_id": item_id,
            "order_id":      oid,
            "product_id":    product["product_id"],
            "quantity":      qty,
            "unit_price":    price,
            "discount":      discount,
            "subtotal":      subtotal,
        })
        item_id += 1

    orders.append({
        "order_id":     oid,
        "customer_id":  customer["customer_id"],
        "order_date":   order_date,
        "status":       status,
        "channel":      channel,
        "payment_method": payment,
        "order_total":  round(order_total, 2),
    })

write_csv("orders.csv",
          ["order_id","customer_id","order_date","status","channel","payment_method","order_total"],
          orders)

write_csv("order_items.csv",
          ["order_item_id","order_id","product_id","quantity","unit_price","discount","subtotal"],
          order_items)

print("\nData generation complete.")
