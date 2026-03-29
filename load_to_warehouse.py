"""
load_to_warehouse.py
--------------------
Step 3 of the pipeline: Load to PostgreSQL Data Warehouse

What this does:
  1. Reads processed CSV files from data/processed/
  2. Loads them into PostgreSQL tables:
       - dim_users
       - dim_products
       - fact_orders
  3. Uses TRUNCATE + INSERT strategy (full refresh)
     In production, you'd use upserts (INSERT ON CONFLICT).

Prerequisites:
  - PostgreSQL running (see docker-compose.yml or local install)
  - Tables already created (run sql/create_tables.sql first)
"""

import os
import csv
import logging
import psycopg2
from psycopg2.extras import execute_values
from config.db_config import DB_CONFIG

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")


def get_connection():
    """Create and return a PostgreSQL connection."""
    conn = psycopg2.connect(**DB_CONFIG)
    log.info("Connected to PostgreSQL.")
    return conn


def find_csv_in_folder(folder_path: str) -> str:
    """
    Spark writes CSVs into a folder (part-00000*.csv).
    This finds the actual data file inside the output folder.
    """
    for fname in os.listdir(folder_path):
        if fname.startswith("part-") and fname.endswith(".csv"):
            return os.path.join(folder_path, fname)
    raise FileNotFoundError(f"No part-*.csv file found in: {folder_path}")


def read_csv(folder_name: str) -> tuple[list[dict], list[str]]:
    """
    Read a Spark-output CSV folder.
    Returns: (list of row dicts, list of column names)
    """
    folder_path = os.path.join(PROCESSED_DIR, folder_name)
    csv_file    = find_csv_in_folder(folder_path)

    with open(csv_file, newline="") as f:
        reader = csv.DictReader(f)
        rows   = [row for row in reader]
        cols   = reader.fieldnames or []

    log.info(f"  Read {len(rows):,} rows from {folder_name}/")
    return rows, cols


def safe_val(v):
    """Convert empty strings to None so PostgreSQL stores NULL, not ''."""
    return None if v == "" else v


# ──────────────────────────────────────────────
# LOAD FUNCTIONS — one per table
# ──────────────────────────────────────────────

def load_dim_users(conn):
    rows, _ = read_csv("dim_users")

    data = [
        (
            safe_val(r["user_id"]),
            safe_val(r["name"]),
            safe_val(r["email"]),
            safe_val(r["city"]),
            safe_val(r["created_at"]),
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dim_users RESTART IDENTITY CASCADE;")
        execute_values(
            cur,
            """
            INSERT INTO dim_users (user_id, name, email, city, created_at)
            VALUES %s
            ON CONFLICT (user_id) DO NOTHING;
            """,
            data,
        )
    conn.commit()
    log.info(f"  ✔ dim_users loaded: {len(data):,} rows")


def load_dim_products(conn):
    rows, _ = read_csv("dim_products")

    data = [
        (
            safe_val(r["product_id"]),
            safe_val(r["name"]),
            safe_val(r["category"]),
            safe_val(r["price"]),
            safe_val(r["stock"]),
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dim_products RESTART IDENTITY CASCADE;")
        execute_values(
            cur,
            """
            INSERT INTO dim_products (product_id, name, category, price, stock)
            VALUES %s
            ON CONFLICT (product_id) DO NOTHING;
            """,
            data,
        )
    conn.commit()
    log.info(f"  ✔ dim_products loaded: {len(data):,} rows")


def load_fact_orders(conn):
    rows, _ = read_csv("fact_orders")

    data = [
        (
            safe_val(r["item_id"]),
            safe_val(r["order_id"]),
            safe_val(r["order_date"]),
            safe_val(r["status"]),
            safe_val(r["user_id"]),
            safe_val(r["user_name"]),
            safe_val(r["user_city"]),
            safe_val(r["product_id"]),
            safe_val(r["product_name"]),
            safe_val(r["product_category"]),
            safe_val(r["quantity"]),
            safe_val(r["unit_price"]),
            safe_val(r["line_total"]),
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE fact_orders RESTART IDENTITY CASCADE;")
        execute_values(
            cur,
            """
            INSERT INTO fact_orders (
                item_id, order_id, order_date, status,
                user_id, user_name, user_city,
                product_id, product_name, product_category,
                quantity, unit_price, line_total
            ) VALUES %s
            ON CONFLICT (item_id) DO NOTHING;
            """,
            data,
        )
    conn.commit()
    log.info(f"  ✔ fact_orders loaded: {len(data):,} rows")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def run_load():
    log.info("=" * 50)
    log.info("Starting warehouse load step")
    log.info("=" * 50)

    conn = get_connection()

    try:
        load_dim_users(conn)
        load_dim_products(conn)
        load_fact_orders(conn)
    finally:
        conn.close()
        log.info("Connection closed.")

    log.info("=" * 50)
    log.info("Load complete. Data is now in PostgreSQL.")
    log.info("=" * 50)


if __name__ == "__main__":
    run_load()
