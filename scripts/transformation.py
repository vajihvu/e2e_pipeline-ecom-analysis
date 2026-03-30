"""
transformation.py
-----------------
Step 2 of the pipeline: Transformation (Pandas version)

What this does:
  1. Reads raw CSVs into DataFrames
  2. Cleans data (drops nulls, casts types)
  3. Joins orders + order_items + products + users
  4. Computes:
       - total_order_value  (per order)
       - daily_sales        (revenue aggregated by date)
  5. Saves processed outputs to data/processed/ as CSV

Note: Originally written in PySpark, but adapted to Pandas for Windows compatibility.
"""

import os
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR       = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)


def read_raw_data() -> dict:
    log.info("Reading raw CSV files...")
    dfs = {
        "users":       pd.read_csv(f"{RAW_DIR}/users.csv"),
        "products":    pd.read_csv(f"{RAW_DIR}/products.csv"),
        "orders":      pd.read_csv(f"{RAW_DIR}/orders.csv"),
        "order_items": pd.read_csv(f"{RAW_DIR}/order_items.csv"),
    }
    for name, df in dfs.items():
        log.info(f"  {name}: {len(df):,} rows, {len(df.columns)} columns")
    return dfs


def clean_data(dfs: dict) -> dict:
    log.info("Cleaning data...")

    users = dfs["users"].dropna(subset=["user_id", "email"]).copy()
    users["user_id"] = users["user_id"].astype(int)
    users["created_at"] = pd.to_datetime(users["created_at"]).dt.date
    users = users.drop_duplicates(subset=["user_id"])

    products = dfs["products"].dropna(subset=["product_id", "price"]).copy()
    products["product_id"] = products["product_id"].astype(int)
    products["price"] = products["price"].astype(float)
    products["stock"] = products["stock"].astype(int)
    products = products.drop_duplicates(subset=["product_id"])

    orders = dfs["orders"].dropna(subset=["order_id", "user_id", "order_date"]).copy()
    orders["order_id"] = orders["order_id"].astype(int)
    orders["user_id"] = orders["user_id"].astype(int)
    orders["order_date"] = pd.to_datetime(orders["order_date"]).dt.date
    orders = orders[orders["status"] == "completed"]
    orders = orders.drop_duplicates(subset=["order_id"])

    order_items = dfs["order_items"].dropna(subset=["item_id", "order_id", "product_id"]).copy()
    order_items["order_id"] = order_items["order_id"].astype(int)
    order_items["product_id"] = order_items["product_id"].astype(int)
    order_items["quantity"] = order_items["quantity"].astype(int)
    order_items["unit_price"] = order_items["unit_price"].astype(float)
    order_items = order_items.drop_duplicates(subset=["item_id"])

    log.info("  Cleaning complete.")
    return {
        "users": users,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }


def build_fact_orders(dfs: dict) -> pd.DataFrame:
    log.info("Building enriched orders fact table...")

    items = dfs["order_items"].copy()
    items["line_total"] = (items["quantity"] * items["unit_price"]).round(2)

    fact = items.merge(dfs["orders"], on="order_id", how="inner")
    
    users = dfs["users"].rename(columns={"name": "user_name", "city": "user_city"})
    fact = fact.merge(users[["user_id", "user_name", "user_city"]], on="user_id", how="left")
    
    products = dfs["products"].rename(columns={"name": "product_name", "category": "product_category"})
    fact = fact.merge(products[["product_id", "product_name", "product_category"]], on="product_id", how="left")

    cols = [
        "item_id", "order_id", "order_date", "status", "user_id", "user_name", "user_city",
        "product_id", "product_name", "product_category", "quantity", "unit_price", "line_total"
    ]
    fact = fact[cols]
    
    log.info(f"  fact_orders: {len(fact):,} rows")
    return fact


def build_order_totals(fact_orders: pd.DataFrame) -> pd.DataFrame:
    log.info("Computing total order values...")

    totals = fact_orders.groupby(["order_id", "order_date", "user_id", "user_name", "user_city"]).agg(
        total_order_value=("line_total", "sum"),
        total_items=("quantity", "sum"),
        unique_products=("product_id", "nunique")
    ).reset_index()

    totals["total_order_value"] = totals["total_order_value"].round(2)
    totals = totals.sort_values("order_date")
    
    log.info(f"  order_totals: {len(totals):,} rows")
    return totals


def build_daily_sales(fact_orders: pd.DataFrame) -> pd.DataFrame:
    log.info("Computing daily sales...")

    daily = fact_orders.groupby("order_date").agg(
        daily_revenue=("line_total", "sum"),
        num_orders=("order_id", "nunique"),
        units_sold=("quantity", "sum")
    ).reset_index()

    daily["daily_revenue"] = daily["daily_revenue"].round(2)
    daily = daily.sort_values("order_date")
    
    log.info(f"  daily_sales: {len(daily):,} rows")
    return daily


def save_as_csv(df: pd.DataFrame, name: str):
    path_dir = os.path.join(PROCESSED_DIR, name)
    os.makedirs(path_dir, exist_ok=True)
    
    # Write as part-00000.csv so load_to_warehouse.py finds it correctly
    file_path = os.path.join(path_dir, "part-00000.csv")
    df.to_csv(file_path, index=False)
    log.info(f"  Saved → {path_dir}/")


def run_transformation():
    log.info("=" * 50)
    log.info("Starting transformation step")
    log.info("=" * 50)

    raw_dfs  = read_raw_data()
    clean    = clean_data(raw_dfs)

    fact_orders  = build_fact_orders(clean)
    order_totals = build_order_totals(fact_orders)
    daily_sales  = build_daily_sales(fact_orders)

    save_as_csv(clean["users"],    "dim_users")
    save_as_csv(clean["products"], "dim_products")
    save_as_csv(fact_orders,       "fact_orders")
    save_as_csv(order_totals,      "order_totals")
    save_as_csv(daily_sales,       "daily_sales")

    log.info("=" * 50)
    log.info("Transformation complete. Files saved to data/processed/")
    log.info("=" * 50)


if __name__ == "__main__":
    run_transformation()
