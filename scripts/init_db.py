import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# 1. Create ecommerce database
conn = psycopg2.connect(dbname='postgres', user='postgres', password='J@mmypants78690', host='localhost', port=5432)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()
try:
    cur.execute('CREATE DATABASE ecommerce;')
    print("Created database.")
except Exception as e:
    print(e)
cur.close()
conn.close()

# 2. Run create_tables.sql
conn = psycopg2.connect(dbname='ecommerce', user='postgres', password='J@mmypants78690', host='localhost', port=5432)
cur = conn.cursor()
with open('sql/create_tables.sql', 'r') as f:
    sql = f.read()
    cur.execute(sql)
conn.commit()
cur.close()
conn.close()
print("Tables created.")
