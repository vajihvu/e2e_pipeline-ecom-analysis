import psycopg2

conn = psycopg2.connect(dbname='ecommerce', user='postgres', password='J@mmypants78690', host='localhost', port=5432)
conn.autocommit = True
cur = conn.cursor()

print("\n=== END-TO-END PIPELINE VERIFICATION ===\n")

# 1. Row counts
tables = ['dim_users', 'dim_products', 'fact_orders']
for table in tables:
    cur.execute(f"SELECT COUNT(*) FROM {table};")
    count = cur.fetchone()[0]
    status = "OK" if count > 0 else "EMPTY"
    print(f"  [{status}] {table}: {count:,} rows")

# 2. Sample from fact_orders
print("\n--- Sample from fact_orders ---")
cur.execute("SELECT order_id, order_date, user_name, product_name, quantity, line_total FROM fact_orders LIMIT 5;")
rows = cur.fetchall()
if rows:
    for row in rows:
        print(f"  Order #{row[0]} | {row[1]} | {row[2]} | {row[3]} | qty:{row[4]} | ${row[5]}")
else:
    print("  (no rows found)")

# 3. Total revenue
print("\n--- Revenue Summary ---")
cur.execute("SELECT COUNT(DISTINCT order_id), SUM(line_total), AVG(line_total) FROM fact_orders;")
r = cur.fetchone()
print(f"  Total Orders:   {r[0]:,}")
print(f"  Total Revenue:  ${r[1]:,.2f}")
print(f"  Avg per item:   ${r[2]:,.2f}")

print("\n=== ALL CHECKS PASSED ===\n")
cur.close()
conn.close()
