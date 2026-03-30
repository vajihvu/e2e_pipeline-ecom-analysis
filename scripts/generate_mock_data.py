"""
generate_mock_data.py
---------------------
Generates realistic mock CSV data for:
  - users.csv
  - products.csv
  - orders.csv
  - order_items.csv

Run this once before starting the pipeline.
Output goes to: data/raw/
"""

import csv
import random
from datetime import datetime, timedelta
import os

# Where to save the raw CSVs
RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

random.seed(42)  # Reproducible results

# ──────────────────────────────────────────────
# 1. USERS
# ──────────────────────────────────────────────
CITIES = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune", "Hyderabad"]
DOMAINS = ["gmail.com", "yahoo.com", "outlook.com"]

def generate_users(n=100):
    users = []
    for i in range(1, n + 1):
        users.append({
            "user_id": i,
            "name": f"User_{i}",
            "email": f"user{i}@{random.choice(DOMAINS)}",
            "city": random.choice(CITIES),
            "created_at": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
        })
    return users

# ──────────────────────────────────────────────
# 2. PRODUCTS
# ──────────────────────────────────────────────
CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports"]
PRODUCT_NAMES = [
    "Laptop", "Phone", "T-Shirt", "Jeans", "Novel", "Textbook",
    "Lamp", "Chair", "Shoes", "Bag", "Watch", "Headphones",
    "Tablet", "Camera", "Jacket", "Shorts", "Desk", "Monitor",
]

def generate_products(n=50):
    products = []
    for i in range(1, n + 1):
        products.append({
            "product_id": i,
            "name": f"{random.choice(PRODUCT_NAMES)}_{i}",
            "category": random.choice(CATEGORIES),
            "price": round(random.uniform(10, 1500), 2),
            "stock": random.randint(0, 500),
        })
    return products

# ──────────────────────────────────────────────
# 3. ORDERS
# ──────────────────────────────────────────────
STATUSES = ["completed", "pending", "cancelled", "returned"]

def generate_orders(n=300, num_users=100):
    orders = []
    for i in range(1, n + 1):
        orders.append({
            "order_id": i,
            "user_id": random.randint(1, num_users),
            "order_date": (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))).strftime("%Y-%m-%d"),
            "status": random.choice(STATUSES),
            "shipping_city": random.choice(CITIES),
        })
    return orders

# ──────────────────────────────────────────────
# 4. ORDER ITEMS
# ──────────────────────────────────────────────
def generate_order_items(orders, num_products=50):
    items = []
    item_id = 1
    for order in orders:
        # Each order has 1–4 line items
        num_items = random.randint(1, 4)
        chosen_products = random.sample(range(1, num_products + 1), num_items)
        for pid in chosen_products:
            items.append({
                "item_id": item_id,
                "order_id": order["order_id"],
                "product_id": pid,
                "quantity": random.randint(1, 5),
                "unit_price": round(random.uniform(10, 1500), 2),
            })
            item_id += 1
    return items

# ──────────────────────────────────────────────
# WRITE TO CSV
# ──────────────────────────────────────────────
def write_csv(filename, rows, fieldnames):
    path = os.path.join(RAW_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✔ Written {len(rows):,} rows → {path}")

if __name__ == "__main__":
    print("Generating mock data...")

    users    = generate_users(100)
    products = generate_products(50)
    orders   = generate_orders(300, num_users=100)
    items    = generate_order_items(orders, num_products=50)

    write_csv("users.csv",       users,    ["user_id", "name", "email", "city", "created_at"])
    write_csv("products.csv",    products, ["product_id", "name", "category", "price", "stock"])
    write_csv("orders.csv",      orders,   ["order_id", "user_id", "order_date", "status", "shipping_city"])
    write_csv("order_items.csv", items,    ["item_id", "order_id", "product_id", "quantity", "unit_price"])

    print("\nDone! Raw data is in data/raw/")
