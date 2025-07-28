# etl/load_fangraphs.py
# Script to load fangraphs data from the local csvs files into the local database

# Importing Python Packages
import os
import csv
import psycopg2
from dotenv import load_dotenv
import logging
from datetime import datetime

# Connecting to database
load_dotenv(dotenv_path='../.env')

DB_PARAMS = {
    "host": os.getenv("PGHOST"),
    "dbname": os.getenv("PGDATABASE"),
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "port": os.getenv("PGPORT"),
}

# File path to where csvs are stored
CSV_DIR = "../data/processed/fangraphs"

# Set up logging
LOG_DIR = "../logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = datetime.now().strftime("fangraphs_load_%Y-%m-%d_%H%M%S.txt")
LOG_PATH = os.path.join(LOG_DIR, log_filename)

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.ERROR,
    format="%(asctime)s — %(levelname)s — %(message)s",
    filemode="w"  # Overwrite per run
)


# Functions to clean tables
# Function to clean some of the columns to get rid of problematic symbols and change values to floats or integers
def parse_value(value):
    if value in ("", None):
        return None
    try:
        if isinstance(value, str):
            value = value.replace("%", "").replace("$", "").strip()
            float_val = float(value)
        else:
            float_val = float(value)
        return int(float_val) if float_val.is_integer() else float_val
    except (ValueError, TypeError):
        return str(value).strip()

# Function to put quotes around column names that interfere with psql/sql settings (column names like "name" or columns
# with special characters
def format_column_list(columns):
    return ", ".join([f'"{col}"' if not col.isidentifier() else col for col in columns])

# Function to load the csvs into the local database
def load_csv_to_table(filename, conn):
    table_name = os.path.splitext(filename)[0].lower()
    filepath = os.path.join(CSV_DIR, filename)
    print(f"📥 Loading `{table_name}` from `{filename}`...")

    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            print(f"⚠️ Skipping {filename}: No header row found.")
            return

        # More column formatting to ensure consistancy all around
        columns = [col.strip().lower() for col in reader.fieldnames]
        col_str = format_column_list(columns)
        placeholder_str = ", ".join(["%s"] * len(columns))

        # Safe Upserting
        pk_fields = ('idfg', 'season') # Primary Keys
        update_fields = [col for col in columns if col not in pk_fields]
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_fields])
        where_clause = " OR ".join([f"{table_name}.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in update_fields])

        # Insert SQL statement based off db/schema_fangraphs.sql table definitions
        insert_sql = f"""
        INSERT INTO {table_name} ({col_str})
        VALUES ({placeholder_str})
        ON CONFLICT (idfg, season)
        DO UPDATE SET {update_clause}
        WHERE {where_clause}
        """

        # Creating the cursor to interact with database
        with conn.cursor() as cur:
            for row in reader:
                try:
                    cleaned = {k.strip().lower(): v for k, v in row.items()}
                    values = [parse_value(cleaned.get(col)) for col in columns]

                    if len(values) != len(columns):
                        raise ValueError(f"Mismatch: {len(values)} values vs {len(columns)} columns")

                    cur.execute(insert_sql, values)

                except Exception as e:
                    conn.rollback()
                    error_msg = f" Row Insert error in '{table_name}': {e}"
                    print(error_msg)
                    logging.error(error_msg)

        conn.commit()
        print(f"✅ Finished loading `{table_name}`")

# Function which actually opens the connection to the database and runs the load
def main():
    with psycopg2.connect(**DB_PARAMS) as conn:
        for filename in os.listdir(CSV_DIR):
            if filename.endswith(".csv"):
                load_csv_to_table(filename, conn)
        print("🎉 All FanGraphs tables loaded.")

# Makes sure that main only runs when the script (load_fangraphs in this case) is called directly not when/if this is
# imported from somewhere else
if __name__ == "__main__":
    main()
