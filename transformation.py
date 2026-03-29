"""
transformation.py
-----------------
Step 2 of the pipeline: Transformation (PySpark)

What this does:
  1. Reads raw CSVs into PySpark DataFrames
  2. Cleans data (drops nulls, casts types)
  3. Joins orders + order_items + products + users
  4. Computes:
       - total_order_value  (per order)
       - daily_sales        (revenue aggregated by date)
  5. Saves processed outputs to data/processed/ as CSV
     (Parquet is more efficient in production, but CSV is easier to inspect)

PySpark runs in local mode here — no cluster needed.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType, DateType

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
RAW_DIR       = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)


def get_spark() -> SparkSession:
    """Create or get a SparkSession running in local mode."""
    return (
        SparkSession.builder
        .appName("EcommerceETL")
        .master("local[*]")          # Use all available CPU cores
        .config("spark.sql.shuffle.partitions", "4")  # Low for local dev
        .getOrCreate()
    )


# ──────────────────────────────────────────────
# STEP 1: READ RAW DATA
# ──────────────────────────────────────────────
def read_raw_data(spark: SparkSession) -> dict:
    """
    Read all raw CSVs into DataFrames.
    inferSchema=True lets Spark detect column types automatically.
    """
    log.info("Reading raw CSV files...")

    dfs = {
        "users":       spark.read.csv(f"{RAW_DIR}/users.csv",       header=True, inferSchema=True),
        "products":    spark.read.csv(f"{RAW_DIR}/products.csv",     header=True, inferSchema=True),
        "orders":      spark.read.csv(f"{RAW_DIR}/orders.csv",       header=True, inferSchema=True),
        "order_items": spark.read.csv(f"{RAW_DIR}/order_items.csv",  header=True, inferSchema=True),
    }

    for name, df in dfs.items():
        log.info(f"  {name}: {df.count():,} rows, {len(df.columns)} columns")

    return dfs


# ──────────────────────────────────────────────
# STEP 2: CLEAN DATA
# ──────────────────────────────────────────────
def clean_data(dfs: dict) -> dict:
    """
    Basic cleaning:
      - Drop rows where key ID columns are null
      - Cast columns to correct types
      - Drop duplicates
    """
    log.info("Cleaning data...")

    # --- Users ---
    users = (
        dfs["users"]
        .dropna(subset=["user_id", "email"])       # Must have an ID and email
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .withColumn("created_at", F.to_date("created_at", "yyyy-MM-dd"))
        .drop_duplicates(["user_id"])
    )

    # --- Products ---
    products = (
        dfs["products"]
        .dropna(subset=["product_id", "price"])
        .withColumn("product_id", F.col("product_id").cast(IntegerType()))
        .withColumn("price", F.col("price").cast(FloatType()))
        .withColumn("stock", F.col("stock").cast(IntegerType()))
        .drop_duplicates(["product_id"])
    )

    # --- Orders ---
    # Only keep "completed" orders for revenue analysis
    orders = (
        dfs["orders"]
        .dropna(subset=["order_id", "user_id", "order_date"])
        .withColumn("order_id", F.col("order_id").cast(IntegerType()))
        .withColumn("user_id",  F.col("user_id").cast(IntegerType()))
        .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .filter(F.col("status") == "completed")    # Only completed orders
        .drop_duplicates(["order_id"])
    )

    # --- Order Items ---
    order_items = (
        dfs["order_items"]
        .dropna(subset=["item_id", "order_id", "product_id"])
        .withColumn("order_id",   F.col("order_id").cast(IntegerType()))
        .withColumn("product_id", F.col("product_id").cast(IntegerType()))
        .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(FloatType()))
        .drop_duplicates(["item_id"])
    )

    log.info("  Cleaning complete.")
    return {
        "users": users,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }


# ──────────────────────────────────────────────
# STEP 3: TRANSFORM — ENRICHED ORDERS
# ──────────────────────────────────────────────
def build_fact_orders(dfs: dict):
    """
    Join orders + order_items + users + products into one wide table.

    Each row = one order line item, enriched with:
      - user info (name, city)
      - product info (name, category)
      - line_total = quantity × unit_price
    """
    log.info("Building enriched orders fact table...")

    # Calculate line total for each item
    items_with_total = dfs["order_items"].withColumn(
        "line_total",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    )

    # Join order items → orders → users → products
    fact = (
        items_with_total
        .join(dfs["orders"],   on="order_id",   how="inner")
        .join(dfs["users"],    on="user_id",     how="left")
        .join(dfs["products"], on="product_id",  how="left")
        # Select and rename to clean output schema
        .select(
            F.col("item_id"),
            F.col("order_id"),
            F.col("order_date"),
            F.col("status"),
            F.col("user_id"),
            F.col("users.name").alias("user_name"),
            F.col("users.city").alias("user_city"),
            F.col("product_id"),
            F.col("products.name").alias("product_name"),
            F.col("products.category").alias("product_category"),
            F.col("quantity"),
            F.col("unit_price"),
            F.col("line_total"),
        )
    )

    log.info(f"  fact_orders: {fact.count():,} rows")
    return fact


# ──────────────────────────────────────────────
# STEP 4: TRANSFORM — TOTAL ORDER VALUE
# ──────────────────────────────────────────────
def build_order_totals(fact_orders):
    """
    Aggregate fact_orders to get total value per order.
    Result: one row per order with total_order_value.
    """
    log.info("Computing total order values...")

    order_totals = (
        fact_orders
        .groupBy("order_id", "order_date", "user_id", "user_name", "user_city")
        .agg(
            F.round(F.sum("line_total"), 2).alias("total_order_value"),
            F.sum("quantity").alias("total_items"),
            F.countDistinct("product_id").alias("unique_products"),
        )
        .orderBy("order_date")
    )

    log.info(f"  order_totals: {order_totals.count():,} rows")
    return order_totals


# ──────────────────────────────────────────────
# STEP 5: TRANSFORM — DAILY SALES
# ──────────────────────────────────────────────
def build_daily_sales(fact_orders):
    """
    Aggregate fact_orders by date to get daily revenue.
    Useful for time-series charts and trending.
    """
    log.info("Computing daily sales...")

    daily_sales = (
        fact_orders
        .groupBy("order_date")
        .agg(
            F.round(F.sum("line_total"), 2).alias("daily_revenue"),
            F.countDistinct("order_id").alias("num_orders"),
            F.sum("quantity").alias("units_sold"),
        )
        .orderBy("order_date")
    )

    log.info(f"  daily_sales: {daily_sales.count():,} rows")
    return daily_sales


# ──────────────────────────────────────────────
# STEP 6: SAVE PROCESSED DATA
# ──────────────────────────────────────────────
def save_as_csv(df, name: str):
    """
    Save a Spark DataFrame to a single CSV file in data/processed/.

    Note: Spark writes partitioned files by default (part-0000*.csv).
    We use coalesce(1) to merge into one file for simplicity.
    In production, you'd keep partitions for performance.
    """
    path = os.path.join(PROCESSED_DIR, name)

    # coalesce(1) = write to a single partition/file
    df.coalesce(1).write.csv(path, header=True, mode="overwrite")
    log.info(f"  Saved → {path}/")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def run_transformation():
    log.info("=" * 50)
    log.info("Starting transformation step")
    log.info("=" * 50)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")  # Suppress verbose Spark logs

    # Read → Clean → Transform → Save
    raw_dfs  = read_raw_data(spark)
    clean    = clean_data(raw_dfs)

    fact_orders  = build_fact_orders(clean)
    order_totals = build_order_totals(fact_orders)
    daily_sales  = build_daily_sales(fact_orders)

    # Save clean dimension tables too (for warehouse loading)
    save_as_csv(clean["users"],    "dim_users")
    save_as_csv(clean["products"], "dim_products")
    save_as_csv(fact_orders,       "fact_orders")
    save_as_csv(order_totals,      "order_totals")
    save_as_csv(daily_sales,       "daily_sales")

    log.info("=" * 50)
    log.info("Transformation complete. Files saved to data/processed/")
    log.info("=" * 50)

    spark.stop()


if __name__ == "__main__":
    run_transformation()
