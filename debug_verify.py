import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password=os.getenv('DB_PASSWORD'),
    host='localhost',
    port='5432'
)
cur = conn.cursor()

print("\n--- Checking for populated address data ---")
cur.execute("""
    SELECT order_id, bill_city, ship_country, order_total 
    FROM data.orders_10001 
    WHERE bill_city IS NOT NULL 
    LIMIT 5
""")
rows = cur.fetchall()
if not rows:
    print("No rows with bill_city found.")
else:
    for r in rows:
        print(r)

print("\n--- Raw Data Check ---")
cur.execute("SELECT data FROM raw.orders LIMIT 5")
rows = cur.fetchall()
for i, (data,) in enumerate(rows):
    orders = data.get("orders", [])
    print(f"Page {i+1}: {len(orders)} orders")
    if orders:
        o = orders[0]
        print(f"  Sample Order ID: {o.get('id')}")
        print(f"  Billing Address Keys: {o.get('billing_address', {}).keys()}")
        print(f"  Financial Status: {o.get('financial_status')}")

conn.close()
