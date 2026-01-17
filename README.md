# CartPanda Automation

This project provides an automated pipeline for fetching order data from the **CartPanda API**, storing it in its raw format, and transforming it into structured database tables for analysis.

## Project Structure

- **`fetch_cartpanda_orders.py`**: Connects to the CartPanda API to download order data (including items, transactions, and customer info). It stores the raw JSON responses into the `raw.orders` table in PostgreSQL.
- **`transform_orders.py`**: ETL script that reads raw JSON data from `raw.orders`, parses it, and populates the structured table `data.orders_10001`.
- **`sql/create_raw_tables.sql`**: SQL definitions for creating the initial raw data storage schema.
- **`debug_schema.py`**: Utility script to inspect the table schema and columns of `data.orders_10001`.
- **`debug_verify.py`**: specific verification script to check for populated data (like addresses and totals) in the transformed tables.

## Prerequisites

- **Python 3.x**
- **PostgreSQL**: A local or remote instance running on port `5432`.
- **CartPanda API Access**: You need a valid API key.

## Setup

1. **Install Dependencies**

   Ensure you have the required Python packages installed:

   ```bash
   pip install -r requirements.txt
   ```
   *(Common dependencies: `requests`, `psycopg2`, `python-dotenv`)*

2. **Environment Configuration**

   Create a `.env` file in the root directory with the following variables:

   ```ini
   CARTPANDA_API_KEY=your_cartpanda_api_token
   DB_PASSWORD=your_postgres_password
   ```

   *Note: The scripts assume default `postgres` user and `localhost` connection. Modify `DB_CONFIG` in the scripts if your setup differs.*

3. **Database Initialization**

   The scripts handle basic schema creation, but ensure your PostgreSQL server is running. `fetch_cartpanda_orders.py` will create the `raw` schema and `raw.orders` table automatically on first run.

## Usage

### 1. Fetch Orders
Download the latest orders from CartPanda and save them to the database (Raw Layer).

```bash
python fetch_cartpanda_orders.py
```

### 2. Transform Data
Parse the raw JSON data and insert it into the analytic tables (Data Layer).

```bash
python transform_orders.py
```

### 3. Debug & Verify
Check the correctness of the transformed data.

```bash
python debug_verify.py
```
