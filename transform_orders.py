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

    # ------------------------------------------------------------
    # Ensure schema exists
    # ------------------------------------------------------------
    cur.execute("CREATE SCHEMA IF NOT EXISTS data;")
    conn.commit()

    # ------------------------------------------------------------
    # Load target columns dynamically from table
    # ------------------------------------------------------------
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='data'
        AND table_name='orders_10001'
        ORDER BY ordinal_position;
    """)

    TARGET_COLUMNS = [row[0] for row in cur.fetchall()]
    print(f"Loaded {len(TARGET_COLUMNS)} columns from data.orders_10001")

    # ------------------------------------------------------------
    # Build INSERT statement dynamically
    # ------------------------------------------------------------
    insert_query = f"""
        INSERT INTO data.orders_10001 ({','.join(TARGET_COLUMNS)})
        VALUES ({','.join(['%s'] * len(TARGET_COLUMNS))})
        ON CONFLICT (unique_order_key) DO NOTHING;
    """

    # ------------------------------------------------------------
    # Fetch raw JSON pages
    # ------------------------------------------------------------
    cur.execute("SELECT data FROM raw.orders;")
    pages = cur.fetchall()

    total_rows = 0

    # Helper: empty row template
    def empty_row():
        return {col: None for col in TARGET_COLUMNS}

    # Helper: clean decimals
    def clean_decimal(val):
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return val
        if isinstance(val, str):
            return val.replace(",", "").strip()
        return None

    # ------------------------------------------------------------
    # Transform JSON → Table
    # ------------------------------------------------------------
    for page in pages:
        page_json = page[0]
        orders = page_json.get("orders", [])

        for order in orders:
            order_id = order.get("id")
            created_at = order.get("created_at")
            currency = order.get("currency")
            customer = order.get("customer") or {}
            bill_addr = order.get("billing_address") or {}
            ship_addr = order.get("shipping_address") or {}
            transactions = order.get("transactions") or []
            first_txn = transactions[0] if transactions else {}

            items = order.get("line_items", [])

            for item in items:
                item_id = item.get("id")

                row = empty_row()

                # Required primary key
                row["unique_order_key"] = f"{order_id}-{item_id}"

                # Core order fields
                row["order_id"] = str(order_id)
                row["transaction_id"] = str(first_txn.get("id"))
                row["customer_number"] = str(customer.get("id"))
                row["currency"] = currency
                row["created_at"] = created_at

                # Product
                row["product_id"] = str(item.get("product_id"))
                row["product_name"] = item.get("name") or item.get("title")

                # Financial
                row["total_price"] = clean_decimal(order.get("total_price"))
                row["subtotal_price"] = clean_decimal(order.get("subtotal_price"))
                row["total_tax"] = clean_decimal(order.get("total_tax"))
                row["line_items_count"] = order.get("line_items_count")

                # Status fields
                row["financial_status"] = order.get("financial_status")
                row["fulfillment_status"] = order.get("fulfillment_status")
                row["payment_status"] = order.get("payment_status")

                # Billing info
                row["bill_first"] = bill_addr.get("first_name")
                row["bill_last"] = bill_addr.get("last_name")
                row["bill_email"] = order.get("email")
                row["bill_phone"] = bill_addr.get("phone")
                row["bill_address1"] = bill_addr.get("address1")
                row["bill_address2"] = bill_addr.get("address2")
                row["bill_city"] = bill_addr.get("city")
                row["bill_state"] = bill_addr.get("province")
                row["bill_zip"] = bill_addr.get("zip")
                row["bill_country"] = bill_addr.get("country")

                # Shipping info
                row["ship_first"] = ship_addr.get("first_name")
                row["ship_last"] = ship_addr.get("last_name")
                row["ship_address1"] = ship_addr.get("address1")
                row["ship_address2"] = ship_addr.get("address2")
                row["ship_city"] = ship_addr.get("city")
                row["ship_state"] = ship_addr.get("province")
                row["ship_zip"] = ship_addr.get("zip")
                row["ship_country"] = ship_addr.get("country")

                # Flags
                row["is_test"] = bool(order.get("test"))
                row["is_cancelled"] = order.get("cancelled_at") is not None
                row["is_refund"] = order.get("financial_status") == "refunded"
                row["is_chargeback"] = order.get("chargeback_at") is not None
                row["is_approved"] = order.get("financial_status") == "paid"

                # Insert row
                cur.execute(insert_query, [row[col] for col in TARGET_COLUMNS])
                total_rows += 1

    # ------------------------------------------------------------
    # Finish
    # ------------------------------------------------------------
    conn.commit()
    cur.close()
    conn.close()

    print(f"\nInserted {total_rows} rows into data.orders_10001 ✅")


# ------------------------------------------------------------
# Run
# ------------------------------------------------------------
if __name__ == "__main__":
    transform_and_insert()
