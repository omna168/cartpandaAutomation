

import requests
import psycopg2
import json
import time
import os
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- CONFIGURATION ---
API_TOKEN = os.getenv('CARTPANDA_API_KEY')
if not API_TOKEN:
    raise ValueError("Error: CARTPANDA_API_KEY environment variable is missing. Please check your .env file.")

API_BASE_URL = "https://accounts.cartpanda.com/api/v3/aya-marketing/orders?include=items,transactions,customer"

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': os.getenv('DB_PASSWORD'),
    'host': 'localhost',
    'port': '5432'
}

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to Database: {e}")
        return None

def setup_database(conn):
    """Creates the schema and table if they don't exist."""
    try:
        cur = conn.cursor()
        
        # Create Schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        
        # Create Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.orders (
                id SERIAL PRIMARY KEY,
                data JSONB,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        conn.commit()
        cur.close()
        print("Database schema and table verified.")
    except psycopg2.Error as e:
        print(f"Error setting up database: {e}")
        conn.rollback()

def fetch_and_store_orders():
    """Main function to fetch pages from API and insert into DB."""
    
    conn = get_db_connection()
    if not conn:
        return # Exit if DB connection fails

    setup_database(conn)
    cur = conn.cursor()

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    page = 1
    
    print(f"Starting fetch from {API_BASE_URL}...")

    while True:
        try:
            print(f"Fetching page {page}...")
            
            # Make the API request with pagination parameter
            response = requests.get(API_BASE_URL, headers=headers, params={'page': page})
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Parse JSON
            api_data = response.json()

            # --- PAGINATION HANDLING ---
            # We need to decide when to stop. 
            # Strategy: Stop if the response doesn't contain the expected list or is empty.
            # Assuming standard structure: {'orders': [...]} or just [...]
            
            if isinstance(api_data, dict):
                orders_list = api_data.get('orders', [])
                # If 'orders' key exists but is empty, or if implicit dict is empty
                if 'orders' in api_data and not orders_list:
                    print(f"Page {page} has no orders. Stopping.")
                    break
            elif isinstance(api_data, list):
                if not api_data:
                    print(f"Page {page} is empty list. Stopping.")
                    break
            else:
                # Fallback: if data is null or empty
                if not api_data:
                     print("No data received. Stopping.")
                     break
            
            # --- INSERT INTO DATABASE ---
            # Inserting the ENTIRE page response into the 'data' column
            insert_query = "INSERT INTO raw.orders (data) VALUES (%s);"
            cur.execute(insert_query, (json.dumps(api_data),))
            conn.commit()
            
            print(f"Successfully saved page {page} to database.")
            
            # Move to next page
            page += 1
            
            # Respectful delay to avoid hitting rate limits
            time.sleep(0.5)

        except requests.exceptions.HTTPError as err:
            print(f"HTTP Error on page {page}: {err}")
            # If 404, it might mean we passed the last page
            if response.status_code == 404:
                print("Reached 404 (likely end of pages). Stopping.")
                break
            else:
                print("Aborting due to API error.")
                break
        except requests.exceptions.RequestException as e:
            print(f"Network Error: {e}")
            break
        except psycopg2.Error as e:
            print(f"Database Insert Error: {e}")
            conn.rollback()
            break
        except Exception as e:
            print(f"Unexpected Error: {e}")
            break

    # Cleanup
    cur.close()
    conn.close()
    print("Job finished.")

if __name__ == "__main__":
    fetch_and_store_orders()
