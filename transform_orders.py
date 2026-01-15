import psycopg2
import json
from datetime import datetime

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres123',
    'host': 'localhost',
    'port': '5432'
}

def transform_and_insert():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Load raw JSON pages
    cur.execute("SELECT data FROM raw.orders;")
    pages = cur.fetchall()

    insert_query = """
        INSERT INTO data.orders_10057 (
            unique_order_key,
            order_id,
            transaction_id,
            customer_email,
            product_name,
            product_price,
            quantity,
            currency,
            order_status,
            is_approved,
            is_refund,
            is_cancelled,
            created_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (unique_order_key) DO NOTHING;
    """

    total_rows = 0

    for page in pages:
        page_json = page[0]

        orders = page_json.get("orders", [])
        for order in orders:
            order_id = order.get("id")
            email = order.get("email")
            currency = order.get("currency")
            created_at = order.get("created_at")

            financial_status = order.get("financial_status")
            payment_status = order.get("payment_status")
            order_status = financial_status or payment_status

            is_approved = (financial_status == "paid")
            is_refund = (financial_status == "refunded")
            is_cancelled = (order.get("cancelled_at") is not None)

            items = order.get("line_items", [])

            for item in items:
                item_id = item.get("id")
                product_name = item.get("title")
                price = item.get("price")
                quantity = item.get("quantity")

                unique_order_key = f"{order_id}-{item_id}"

                cur.execute(insert_query, (
                    unique_order_key,
                    order_id,
                    order_id,          # transaction_id fallback
                    email,
                    product_name,
                    price,
                    quantity,
                    currency,
                    order_status,
                    is_approved,
                    is_refund,
                    is_cancelled,
                    created_at
                ))

                total_rows += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {total_rows} rows into data.orders_10057")

if __name__ == "__main__":
    transform_and_insert()
