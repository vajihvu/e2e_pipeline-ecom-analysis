"""
ecommerce_pipeline_dag.py
-------------------------
Airflow DAG: E-Commerce Data Pipeline

Flow:
  [ingest_data] → [transform_data] → [load_to_warehouse]

Schedule: Runs daily at midnight.
Retries:  2 retries with a 5-minute wait between attempts.

How to use:
  1. Copy this file to your Airflow dags/ folder
  2. Set PIPELINE_BASE_DIR in the DAG or as an Airflow Variable
  3. Trigger manually or let the scheduler run it
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# ──────────────────────────────────────────────
# PATH SETUP
# Reads PIPELINE_BASE_DIR from environment (set in .env or docker-compose.yml).
# This avoids hardcoded paths that break on other machines.
#
# How to set it:
#   Local:  export PIPELINE_BASE_DIR=/your/path/to/ecommerce-pipeline
#   Docker: set in docker-compose.yml under environment:
# ──────────────────────────────────────────────
PIPELINE_BASE_DIR = os.getenv("PIPELINE_BASE_DIR")

if not PIPELINE_BASE_DIR:
    raise EnvironmentError(
        "PIPELINE_BASE_DIR environment variable is not set.\n"
        "Set it to the absolute path of the ecommerce-pipeline project root.\n"
        "Example: export PIPELINE_BASE_DIR=/home/yourname/projects/ecommerce-pipeline"
    )

sys.path.insert(0, PIPELINE_BASE_DIR)

# Import the functions we want to run as tasks
from scripts.ingestion        import run_ingestion
from scripts.transformation   import run_transformation
from scripts.load_to_warehouse import run_load


# ──────────────────────────────────────────────
# DEFAULT ARGUMENTS
# Applied to all tasks unless overridden.
# ──────────────────────────────────────────────
default_args = {
    "owner":            "data_engineer",
    "depends_on_past":  False,          # Don't wait for previous run to succeed
    "start_date":       datetime(2024, 1, 1),
    "retries":          2,              # Retry a failed task twice
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,          # Set True + configure SMTP in production
}


# ──────────────────────────────────────────────
# DAG DEFINITION
# ──────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="Daily ETL: ingest raw CSVs → PySpark transform → load to PostgreSQL",
    schedule_interval="0 0 * * *",   # Every day at midnight (cron syntax)
    catchup=False,                   # Don't backfill missed runs
    tags=["ecommerce", "etl"],
) as dag:

    # ── Task 1: Ingestion ──────────────────────
    # Validates that raw CSV files exist and have correct schemas.
    # Raises an error if any file is missing or malformed — stops the pipeline.
    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=run_ingestion,
        doc_md="""
        **Ingestion Task**
        - Reads raw CSV files from `data/raw/`
        - Validates schema and row count
        - Fails fast if any file is invalid
        """,
    )

    # ── Task 2: Transformation ─────────────────
    # PySpark job: clean, join, aggregate raw data.
    # Outputs processed CSVs to data/processed/.
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=run_transformation,
        doc_md="""
        **Transformation Task**
        - Runs PySpark job in local mode
        - Cleans nulls, casts types, deduplicates
        - Joins orders + items + users + products
        - Computes daily_sales and order_totals
        - Saves results to `data/processed/`
        """,
    )

    # ── Task 3: Load ───────────────────────────
    # Loads processed CSVs into PostgreSQL tables.
    load_task = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=run_load,
        doc_md="""
        **Load Task**
        - Connects to PostgreSQL
        - Truncates and reloads: dim_users, dim_products, fact_orders
        - Full refresh strategy (simple and reliable for daily batch)
        """,
    )

    # ── Task Order ─────────────────────────────
    # Defines the execution sequence using >> (bitshift operator)
    ingest_task >> transform_task >> load_task
