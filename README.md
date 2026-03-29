# 🛒 End-to-End Data Pipeline — E-Commerce Analytics

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apachespark)
![Airflow](https://img.shields.io/badge/Airflow-2.8-017CEE?logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

A production-style batch ETL pipeline that processes daily e-commerce order data — from raw CSV files through PySpark transformation into a PostgreSQL data warehouse, fully orchestrated by Apache Airflow.

> Represents pipelines commonly used in D2C and marketplace platforms (order management, daily revenue reporting, customer analytics).

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Apache Airflow DAG                       │
│                  (scheduled daily at midnight)                  │
│                                                                 │
│   ┌─────────────┐    ┌──────────────────┐    ┌─────────────┐   │
│   │   INGEST    │───▶│   TRANSFORM      │───▶│    LOAD     │   │
│   │             │    │                  │    │             │   │
│   │ Validate    │    │ PySpark (local)  │    │ PostgreSQL  │   │
│   │ raw CSVs    │    │ Clean, join,     │    │ dim_users   │   │
│   │ Schema check│    │ aggregate        │    │ dim_products│   │
│   └─────────────┘    └──────────────────┘    │ fact_orders │   │
│                                              └─────────────┘   │
└─────────────────────────────────────────────────────────────────┘
         │                      │                     │
    data/raw/             data/processed/        SQL Analytics
  (CSV files)            (Spark output)         (BI queries)
```

---

## Project Structure

```
ecommerce-pipeline/
├── data/
│   ├── raw/                        ← generated CSVs land here (git-ignored)
│   └── processed/                  ← Spark output (git-ignored)
│
├── scripts/
│   ├── __init__.py                 ← makes scripts/ importable by Airflow
│   ├── generate_mock_data.py       ← one-time: creates users/products/orders CSVs
│   ├── ingestion.py                ← Step 1: validate raw files
│   ├── transformation.py           ← Step 2: PySpark ETL
│   └── load_to_warehouse.py        ← Step 3: load into PostgreSQL
│
├── dags/
│   └── ecommerce_pipeline_dag.py   ← Airflow DAG wiring all 3 steps
│
├── sql/
│   ├── create_tables.sql           ← DDL: warehouse schema (run once)
│   └── analytics_queries.sql       ← BI queries: revenue, top products, etc.
│
├── config/
│   └── db_config.py                ← reads DB credentials from .env
│
├── .env.example                    ← template — copy to .env and fill in
├── .gitignore
├── docker-compose.yml              ← spins up Postgres + Airflow
└── requirements.txt
```

---

## Data Model (Star Schema)

```
      dim_users                  dim_products
     ───────────                ─────────────
     user_id (PK)               product_id (PK)
     name                       name
     email                      category
     city                       price
     created_at                 stock
           \                       /
            ▼                     ▼
               fact_orders
               ────────────
               item_id (PK)
               order_id
               order_date
               status
               user_id  ──────────▶ dim_users
               user_name   (denorm)
               product_id ─────────▶ dim_products
               product_name (denorm)
               product_category
               quantity
               unit_price
               line_total
```

Fact table is **denormalized** (user/product names stored inline) so analytics queries run fast without extra joins.

---

## Quick Start

### Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.10+ | |
| Java | 8 or 11 | Required by PySpark — check with `java -version` |
| Docker + Docker Compose | any recent | For Postgres + Airflow |

---

### Option A — Docker (Recommended)

**1. Clone and configure**
```bash
git clone https://github.com/your-username/ecommerce-pipeline.git
cd ecommerce-pipeline

cp .env.example .env
# Edit .env — set PIPELINE_BASE_DIR to the absolute path of this folder
# e.g. PIPELINE_BASE_DIR=/home/yourname/projects/ecommerce-pipeline
```

**2. Start services**
```bash
docker-compose up -d
```
- Airflow UI → http://localhost:8080 (user: `admin` / pass: `admin`)
- PostgreSQL → `localhost:5432` (connect via DBeaver or any SQL client)
- Schema is auto-created on first Postgres start via `sql/create_tables.sql`

**3. Generate mock data**
```bash
pip install -r requirements.txt
python scripts/generate_mock_data.py
```

**4. Run the pipeline**

Via Airflow UI:
- Open http://localhost:8080
- Find `ecommerce_pipeline` DAG → click ▶ (Trigger DAG)

Or run scripts directly:
```bash
python scripts/ingestion.py
python scripts/transformation.py
python scripts/load_to_warehouse.py
```

**5. Run analytics queries**
```bash
psql -h localhost -U postgres -d ecommerce -f sql/analytics_queries.sql
```

---

### Option B — No Docker (Manual Setup)

**1. Clone and install**
```bash
git clone https://github.com/your-username/ecommerce-pipeline.git
cd ecommerce-pipeline
pip install -r requirements.txt
```

**2. Configure environment**
```bash
cp .env.example .env
# Fill in your Postgres credentials and PIPELINE_BASE_DIR
```

**3. Set up PostgreSQL**
```bash
psql -U postgres -c "CREATE DATABASE ecommerce;"
psql -U postgres -d ecommerce -f sql/create_tables.sql
```

**4. Generate data and run**
```bash
python scripts/generate_mock_data.py
python scripts/ingestion.py
python scripts/transformation.py
python scripts/load_to_warehouse.py
```

---

## What Each Step Does

### Step 1 — Ingestion (`ingestion.py`)
Validates all raw files before anything runs:
- File exists on disk
- Header contains all expected columns
- At least one data row is present

Fails loudly on any issue — stops the pipeline before bad data enters the system.

### Step 2 — Transformation (`transformation.py`)
PySpark job in local mode:
1. Reads 4 raw CSVs into DataFrames
2. Cleans: drops null IDs, casts column types, deduplicates
3. Filters to `completed` orders only
4. Joins: `order_items → orders → users → products`
5. Computes `line_total = quantity × unit_price`
6. Aggregates `order_totals` (per order) and `daily_sales` (per date)
7. Saves all results to `data/processed/`

### Step 3 — Load (`load_to_warehouse.py`)
- Reads Spark-output CSV folders
- Connects to PostgreSQL via `psycopg2`
- TRUNCATE + bulk INSERT into `dim_users`, `dim_products`, `fact_orders`
- Uses `execute_values` for efficient batch inserts

### Airflow DAG (`ecommerce_pipeline_dag.py`)
- 3 tasks in sequence: `ingest_data → transform_data → load_to_warehouse`
- Daily schedule: `0 0 * * *` (midnight)
- 2 retries with 5-minute delay on failure
- `catchup=False` — won't attempt to backfill missed historical runs

---

## Analytics Queries

Queries in `sql/analytics_queries.sql`:

| Query | Business Question |
|-------|------------------|
| Top 5 products by revenue | What are our best-selling products? |
| Daily revenue | How does revenue trend over time? |
| Top 10 customers | Who are our highest-value customers? |
| Revenue by category | Which categories drive the most sales? |
| Orders by city | Where are our customers located? |
| Monthly revenue summary | How are we tracking month over month? |

---

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `PIPELINE_BASE_DIR` | Absolute path to project root | `/home/user/ecommerce-pipeline` |
| `POSTGRES_HOST` | Postgres hostname | `localhost` |
| `POSTGRES_PORT` | Postgres port | `5432` |
| `POSTGRES_DB` | Database name | `ecommerce` |
| `POSTGRES_USER` | Database user | `postgres` |
| `POSTGRES_PASSWORD` | Database password | `your_password` |

---

## Scaling to Production

| This project | Production equivalent |
|---|---|
| Local CSV files | AWS S3 / GCS buckets |
| PySpark local mode | EMR / Dataproc cluster |
| PostgreSQL | Redshift / Snowflake / BigQuery |
| CSV output | Parquet with date partitioning |
| Full refresh (TRUNCATE+INSERT) | Incremental loads with upserts |
| Sequential Airflow executor | Celery or Kubernetes executor |

---

## License

MIT
