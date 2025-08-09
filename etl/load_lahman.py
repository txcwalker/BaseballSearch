# etl/load_lahman.py

# Script to load lahman data from the local csvs files into the local database

# Importing Python Packages
import os
import csv
import psycopg2
from dotenv import load_dotenv
import logging
from datetime import datetime

# Set up logging
LOG_DIR = "../logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = datetime.now().strftime("lahman_load_%Y-%m-%d_%H%M%S.txt")
LOG_PATH = os.path.join(LOG_DIR, log_filename)

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.ERROR,
    format="%(asctime)s — %(levelname)s — %(message)s",
    filemode="w"
)

# Conflict targets by table for safe UPSERTs
UPSERT_KEYS = {
    "people": ["playerid"],
    "batting": ["playerid", "yearid", "stint"],
    "pitching": ["playerid", "yearid", "stint"],
    "fielding": ["playerid", "yearid", "stint", "pos"],
    "salaries": ["playerid", "yearid", "teamid", "lgid"],

}


# Connecting to database
load_dotenv(dotenv_path='../.env')

DB_PARAMS = {
    "dbname": os.getenv("PGDATABASE"),
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "host": os.getenv("PGHOST"),
    "port": os.getenv("PGPORT"),
}

# File path where lahman dta is stored
CSV_DIR = "../data/lahman_raw"

# Function to Load csvs into tables as dictate by db/schema_lahman.sql
def load_csv_to_table(filename, conn, valid_playerids=None):
    table_name = os.path.splitext(filename)[0].lower()
    filepath = os.path.join(CSV_DIR, filename)
    print(f"📥 Loading {filename} into `{table_name}`...")

    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        # Rename column names so that they do not start with numbers
        COLUMN_RENAMES = {
            "2b": "doubles",
            "3b": "triples",
            "2B": "doubles",
            "3B": "triples",
            # Add more if needed
        }

        # Ensuring the tables have headers
        original_fields = reader.fieldnames
        if not original_fields:
            print(f"⚠️  Skipping {filename}: No header row found.")
            return

        # Standrdizing column names for consistancy
        columns = [COLUMN_RENAMES.get(col.strip().lower(), col.strip().lower()) for col in original_fields]

        with conn.cursor() as cur:
            insert_count = 0
            fail_count = 0

            for i, row in enumerate(reader):
                try:
                    # Special skip logic for halloffame
                    if table_name == "halloffame":
                        pid = row.get("playerID") or row.get("playerid")
                        if not pid or pid not in valid_playerids:
                            continue

                    # Clean up row data
                    row_cleaned = {k.strip().lower(): v for k, v in row.items()} # Lower cases, removes NONE values
                    values = [row_cleaned.get(col, None) if row_cleaned.get(col, "") != "" else None for col in columns]

                    # Sage Insert and silent skip for duplicates
                    placeholders = ", ".join(["%s"] * len(columns))
                    column_names = ", ".join([f'"{col}"' for col in columns])

                    conflict_cols = UPSERT_KEYS.get(table_name)
                    if conflict_cols:
                        conflict_str = ", ".join(conflict_cols)
                        update_fields = [col for col in columns if col not in conflict_cols]
                        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_fields])
                        where_clause = " OR ".join(
                            [f"{table_name}.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in update_fields])

                        sql = f"""
                            INSERT INTO {table_name} ({column_names})
                            VALUES ({placeholders})
                            ON CONFLICT ({conflict_str}) DO UPDATE
                            SET {update_clause}
                            WHERE {where_clause}
                        """
                    else:
                        sql = f"""
                            INSERT INTO {table_name} ({column_names})
                            VALUES ({placeholders})
                            ON CONFLICT DO NOTHING
                        """

                except Exception as e:
                    conn.rollback()
                    msg = f"Row {i} failed in `{table_name}`: {e}"
                    print("⚠️", msg)
                    logging.error(msg)
                    fail_count += 1

            conn.commit()
            print(f"✅ Finished loading `{table_name}` — {insert_count} inserted, {fail_count} failed.\n")

def main():
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        # Preload valid playerids for halloffame
        valid_playerids = set()
        with conn.cursor() as cur:
            cur.execute("SELECT playerid FROM people")
            valid_playerids = {row[0] for row in cur.fetchall()}

        for filename in os.listdir(CSV_DIR):
            if filename.endswith(".csv"):
                if filename.lower() == "halloffame.csv":
                    load_csv_to_table(filename, conn, valid_playerids)
                else:
                    load_csv_to_table(filename, conn)
        print("🎉 All tables loaded.")
    finally:
        conn.close()

# Makes sure that main only runs when the script (load_lahman in this case) is called directly not when/if this is
# imported from somewhere else
if __name__ == "__main__":
    main()
