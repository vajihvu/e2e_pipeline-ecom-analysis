-- analytics_queries.sql
-- ----------------------
-- Business intelligence queries on the data warehouse.
-- Run these after data has been loaded into PostgreSQL.
--
-- Queries:
--   1. Top 5 products by total revenue
--   2. Daily revenue trend
--   3. Top 10 customers by spend
--   4. Revenue breakdown by category
--   5. City-wise order distribution


-- ════════════════════════════════════════════════════════
-- QUERY 1: Top 5 Products by Revenue
-- "Which products make us the most money?"
-- ════════════════════════════════════════════════════════
SELECT
    product_id,
    product_name,
    product_category,
    SUM(quantity)               AS total_units_sold,
    ROUND(SUM(line_total), 2)  AS total_revenue
FROM
    fact_orders
GROUP BY
    product_id, product_name, product_category
ORDER BY
    total_revenue DESC
LIMIT 5;


-- ════════════════════════════════════════════════════════
-- QUERY 2: Daily Revenue
-- "How much did we earn each day?"
-- Useful for trend lines and detecting anomalies.
-- ════════════════════════════════════════════════════════
SELECT
    order_date,
    COUNT(DISTINCT order_id)    AS num_orders,
    SUM(quantity)               AS units_sold,
    ROUND(SUM(line_total), 2)  AS daily_revenue
FROM
    fact_orders
GROUP BY
    order_date
ORDER BY
    order_date;


-- ════════════════════════════════════════════════════════
-- QUERY 3: Top 10 Customers by Total Spend
-- "Who are our highest-value customers?"
-- ════════════════════════════════════════════════════════
SELECT
    user_id,
    user_name,
    user_city,
    COUNT(DISTINCT order_id)    AS total_orders,
    ROUND(SUM(line_total), 2)  AS total_spent
FROM
    fact_orders
GROUP BY
    user_id, user_name, user_city
ORDER BY
    total_spent DESC
LIMIT 10;


-- ════════════════════════════════════════════════════════
-- QUERY 4: Revenue by Product Category
-- "Which categories drive the most revenue?"
-- ════════════════════════════════════════════════════════
SELECT
    product_category,
    COUNT(DISTINCT product_id)  AS num_products,
    SUM(quantity)               AS units_sold,
    ROUND(SUM(line_total), 2)  AS category_revenue,
    -- Percentage of total revenue
    ROUND(
        100.0 * SUM(line_total) / SUM(SUM(line_total)) OVER (),
        2
    ) AS revenue_pct
FROM
    fact_orders
GROUP BY
    product_category
ORDER BY
    category_revenue DESC;


-- ════════════════════════════════════════════════════════
-- QUERY 5: Orders by City
-- "Which cities generate the most orders?"
-- ════════════════════════════════════════════════════════
SELECT
    user_city,
    COUNT(DISTINCT order_id)    AS total_orders,
    COUNT(DISTINCT user_id)     AS unique_customers,
    ROUND(SUM(line_total), 2)  AS total_revenue
FROM
    fact_orders
GROUP BY
    user_city
ORDER BY
    total_revenue DESC;


-- ════════════════════════════════════════════════════════
-- QUERY 6 (BONUS): Monthly Revenue Summary
-- Aggregates revenue by year-month for reporting.
-- ════════════════════════════════════════════════════════
SELECT
    TO_CHAR(order_date, 'YYYY-MM')  AS month,
    COUNT(DISTINCT order_id)         AS num_orders,
    ROUND(SUM(line_total), 2)       AS monthly_revenue
FROM
    fact_orders
GROUP BY
    TO_CHAR(order_date, 'YYYY-MM')
ORDER BY
    month;
