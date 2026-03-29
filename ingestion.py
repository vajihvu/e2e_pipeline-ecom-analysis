"""
ingestion.py
------------
Step 1 of the pipeline: Ingestion

Responsibilities:
  - Read CSV files from data/raw/
  - Validate each file (not empty, expected columns present)
  - Log any issues found
  - If validation passes, confirm data is ready for transformation

This simulates what would be a "copy from S3 → raw zone" step
in a cloud pipeline. Here we just read local files.
"""

import os
import csv
import logging

# ──────────────────────────────────────────────
# LOGGING SETUP
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR  = os.path.join(BASE_DIR, "data", "raw")

# ──────────────────────────────────────────────
# EXPECTED SCHEMA: file → required columns
# ──────────────────────────────────────────────
EXPECTED_SCHEMA = {
    "users.csv":       {"user_id", "name", "email", "city", "created_at"},
    "products.csv":    {"product_id", "name", "category", "price", "stock"},
    "orders.csv":      {"order_id", "user_id", "order_date", "status", "shipping_city"},
    "order_items.csv": {"item_id", "order_id", "product_id", "quantity", "unit_price"},
}


def validate_file(filepath: str, expected_columns: set) -> bool:
    """
    Checks:
      1. File exists on disk
      2. File is not empty (has at least 1 data row)
      3. CSV header contains all expected columns

    Returns True if all checks pass, False otherwise.
    """
    filename = os.path.basename(filepath)

    # Check 1: File exists
    if not os.path.exists(filepath):
        log.error(f"[{filename}] File not found: {filepath}")
        return False

    # Check 2 & 3: Open and inspect the CSV
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)

        # Get the column names from the header row
        actual_columns = set(reader.fieldnames or [])

        # Check: All expected columns must be present
        missing = expected_columns - actual_columns
        if missing:
            log.error(f"[{filename}] Missing columns: {missing}")
            return False

        # Check: Must have at least one data row
        rows = list(reader)
        if len(rows) == 0:
            log.error(f"[{filename}] File is empty (no data rows)")
            return False

        log.info(f"[{filename}] ✔ Valid — {len(rows):,} rows, columns OK")

    return True


def run_ingestion() -> bool:
    """
    Main ingestion function.
    Validates all expected files in the raw data directory.

    Returns True if all files pass validation.
    Raises an exception if any file fails — this stops the Airflow DAG.
    """
    log.info("=" * 50)
    log.info("Starting ingestion step")
    log.info(f"Reading from: {RAW_DIR}")
    log.info("=" * 50)

    all_valid = True

    for filename, expected_cols in EXPECTED_SCHEMA.items():
        filepath = os.path.join(RAW_DIR, filename)
        is_valid = validate_file(filepath, expected_cols)
        if not is_valid:
            all_valid = False

    if not all_valid:
        raise ValueError("Ingestion failed: one or more files did not pass validation.")

    log.info("=" * 50)
    log.info("Ingestion complete. All files validated successfully.")
    log.info("=" * 50)
    return True


# Run directly for testing
if __name__ == "__main__":
    run_ingestion()
