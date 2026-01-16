import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': os.getenv('DB_PASSWORD'),
    'host': 'localhost',
    'port': '5432'
}

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("""
    SELECT column_name, data_type, ordinal_position 
    FROM information_schema.columns 
    WHERE table_schema='data' 
    AND table_name='orders_10001' 
    ORDER BY ordinal_position
""")
columns = cur.fetchall()
for c in columns:
    print(f"{c[2]}: {c[0]} ({c[1]})")

conn.close()
