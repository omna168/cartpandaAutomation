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


def transform_and_insert():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("Connected to database.")

    # --- SCHEMA FIX: Ensure unique_order_key is TEXT ---
    # The target table defines unique_order_key as BIGINT, but we generate a 
    # composite string key (Order-Item). We must alter the column to TEXT.
    try:
        cur.execute("ALTER TABLE data.orders_10001 ALTER COLUMN unique_order_key TYPE TEXT;")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Notice: Could not alter column type (might already be correct): {e}")
    # ---------------------------------------------------

    # ------------------------------------------------------------
    # 1. Read all target columns from replica table
    # ------------------------------------------------------------
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='data'
        AND table_name='orders_10001'
        ORDER BY ordinal_position;
    """)

    TARGET_COLUMNS = [row[0] for row in cur.fetchall()]
    print(f"Loaded {len(TARGET_COLUMNS)} target columns.")

    # ------------------------------------------------------------
    # 2. Build dynamic INSERT statement
    # ------------------------------------------------------------
    insert_query = f"""
        INSERT INTO data.orders_10001 ({','.join(TARGET_COLUMNS)})
        VALUES ({','.join(['%s'] * len(TARGET_COLUMNS))})
        ON CONFLICT (unique_order_key) DO NOTHING;
    """

    # ------------------------------------------------------------
    # 3. Fetch raw JSON pages
    # ------------------------------------------------------------
    cur.execute("SELECT data FROM raw.orders;")
    pages = cur.fetchall()

    total_rows = 0

    # Helper: create empty row template
    def build_empty_row():
        return {col: None for col in TARGET_COLUMNS}

    # ------------------------------------------------------------
    # 4. Transform raw JSON → target structure
    # ------------------------------------------------------------
    for page in pages:
        page_json = page[0]
        orders = page_json.get("orders", [])

        for order in orders:
            order_id = order.get("id")
            email = order.get("email")
            currency = order.get("currency")
            created_at = order.get("created_at")

            transactions = order.get("transactions", [])
            transaction_id = transactions[0].get("id") if transactions else None

            financial_status = order.get("financial_status")
            payment_status = order.get("payment_status")
            order_status = financial_status or payment_status

            is_approved = (financial_status == "paid")
            is_refund = (financial_status == "refunded")
            is_cancelled = (order.get("cancelled_at") is not None)

            items = order.get("line_items", [])

            for item in items:
                item_id = item.get("id")
                product_name = item.get("title") or item.get("name")
                price = item.get("price")
                quantity = item.get("quantity")

                unique_order_key = f"{order_id}-{item_id}"

                # Build full row with NULL defaults
                row = build_empty_row()

                # Fill available mapped fields
                row["unique_order_key"] = unique_order_key
                row["order_id"] = str(order_id)
                row["transaction_id"] = str(transaction_id) if transaction_id else None
                row["bill_email"] = email
                row["product_name"] = product_name
                row["order_total"] = price
                row["quantity"] = quantity
                row["currency"] = currency
                row["order_status"] = order_status
                row["is_approved"] = is_approved
                row["is_refund"] = is_refund
                row["is_cancelled"] = is_cancelled
                row["created_on"] = created_at

                # Execute insert
                cur.execute(insert_query, [row[col] for col in TARGET_COLUMNS])
                total_rows += 1

    # ------------------------------------------------------------
    # 5. Commit and close
    # ------------------------------------------------------------
    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {total_rows} rows into data.orders_10001 ✅")


# ------------------------------------------------------------
# Run
# ------------------------------------------------------------
if __name__ == "__main__":
    transform_and_insert()
