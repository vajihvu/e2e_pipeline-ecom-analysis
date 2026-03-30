-- create_tables.sql
-- ------------------
-- Creates the data warehouse schema in PostgreSQL.
-- Run this ONCE before loading data.
--
-- Tables:
--   dim_users     → user dimension
--   dim_products  → product dimension
--   fact_orders   → order line items fact table
--
-- Star schema (simple): fact_orders references both dimensions via IDs.

-- ────────────────────────────────────────────
-- Drop tables if re-running (useful during dev)
-- ────────────────────────────────────────────
DROP TABLE IF EXISTS fact_orders   CASCADE;
DROP TABLE IF EXISTS dim_users     CASCADE;
DROP TABLE IF EXISTS dim_products  CASCADE;

-- ────────────────────────────────────────────
-- DIMENSION: dim_users
-- One row per user
-- ────────────────────────────────────────────
CREATE TABLE dim_users (
    user_id     INTEGER      PRIMARY KEY,
    name        VARCHAR(100),
    email       VARCHAR(150) UNIQUE,
    city        VARCHAR(100),
    created_at  DATE
);

-- ────────────────────────────────────────────
-- DIMENSION: dim_products
-- One row per product
-- ────────────────────────────────────────────
CREATE TABLE dim_products (
    product_id  INTEGER       PRIMARY KEY,
    name        VARCHAR(150),
    category    VARCHAR(100),
    price       NUMERIC(10,2),
    stock       INTEGER
);

-- ────────────────────────────────────────────
-- FACT: fact_orders
-- One row per order line item (order + product)
-- Denormalized: includes user and product names
-- for easier querying without joins.
-- ────────────────────────────────────────────
CREATE TABLE fact_orders (
    item_id          INTEGER        PRIMARY KEY,
    order_id         INTEGER        NOT NULL,
    order_date       DATE,
    status           VARCHAR(50),

    -- User info (denormalized from dim_users)
    user_id          INTEGER,
    user_name        VARCHAR(100),
    user_city        VARCHAR(100),

    -- Product info (denormalized from dim_products)
    product_id       INTEGER,
    product_name     VARCHAR(150),
    product_category VARCHAR(100),

    -- Financials
    quantity         INTEGER,
    unit_price       NUMERIC(10,2),
    line_total       NUMERIC(10,2),

    -- Foreign key constraints (optional — remove if you want fast bulk loads)
    CONSTRAINT fk_user    FOREIGN KEY (user_id)    REFERENCES dim_users(user_id),
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);

-- ────────────────────────────────────────────
-- INDEXES — speeds up the analytics queries
-- ────────────────────────────────────────────
CREATE INDEX idx_fact_orders_date     ON fact_orders (order_date);
CREATE INDEX idx_fact_orders_user     ON fact_orders (user_id);
CREATE INDEX idx_fact_orders_product  ON fact_orders (product_id);

-- Confirm
SELECT 'Tables created successfully.' AS status;
