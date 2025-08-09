# etl/load_all_to_rds.py

# script for a one time load of local tables into aws rds

import os
import csv
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

# Load .env.aws for RDS credentials
load_dotenv(Path(__file__).resolve().parents[1] / ".env.awsrds")

DB_PARAMS = {
    "dbname": os.getenv("AWSDATABASE"),
    "user": os.getenv("AWSUSER"),
    "password": os.getenv("AWSPASSWORD"),
    "host": os.getenv("AWSHOST"),
    "port": os.getenv("AWSPORT"),
}

# paths for where the files are stored
FANGRAPHS_DIR = "../data/processed/fangraphs"
LAHMAN_DIR = "../data/lahman_raw"
BRIDGE_FILE = "../data/processed/lahman_fangraphs_bridge.csv"

#
def load_with_copy(conn, filepath):
    # Grabbing table name from the path
    table_name = os.path.splitext(os.path.basename(filepath))[0].lower()
    print(f"📥 Loading {filepath} into `{table_name}`...")

    # OPening and reading first line (header) to find column names
    with open(filepath, 'r', encoding='utf-8') as f:
        header = [col.strip().lower() for col in f.readline().strip().split(',')]
        columns = ", ".join([f'"{col}"' if not col.isidentifier() else col for col in header])

    # Ensures a clean slate, removes existing table if exists
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE" )

        # Uses the first X rows to guess each columns data type, creating SQL CREATE TABLE notation as seen in fangraphs_schema.sql
        def infer_sql_type(value):
            if value == "" or value is None:
                return "TEXT"
            try:
                int(value)
                return "INTEGER"
            except ValueError:
                try:
                    float(value)
                    return "REAL"
                except ValueError:
                    return "TEXT"

        # Read first N rows to guess types
        with open(filepath, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            raw_header = next(reader)
            header = [col.strip().lower() for col in raw_header]  # force lowercase

            # Read a few rows and ensure each row uses lowercase keys
            sample_rows = [row for _, row in zip(range(10), reader)]
            sample = [dict(zip(header, row)) for row in sample_rows]

            inferred_types = {}
            for col in header:
                sample_vals = [row[col] for row in sample if col in row and row[col] not in ("", None)]
                first_val = sample_vals[0] if sample_vals else ""
                inferred_types[col] = infer_sql_type(first_val)

        col_types = ", ".join([f'"{col}" {inferred_types[col]}' for col in header])

        # Creates the table
        cur.execute(f"CREATE TABLE {table_name} ({col_types})")

        # bulk loads the tables
        with open(filepath, 'r', encoding='utf-8') as f:
            next(f)  # skip header
            cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", f)

        conn.commit()

    print(f"✅ Finished loading `{table_name}`")

def main():
    conn = psycopg2.connect(**DB_PARAMS)

    # Load FanGraphs
    for f in os.listdir(FANGRAPHS_DIR):
        if f.endswith(".csv"):
            load_with_copy(conn, os.path.join(FANGRAPHS_DIR, f))

    # Load Lahman
    for f in os.listdir(LAHMAN_DIR):
        if f.endswith(".csv"):
            load_with_copy(conn, os.path.join(LAHMAN_DIR, f))

    # Load bridge table
    load_with_copy(conn, BRIDGE_FILE)

    conn.close()

if __name__ == "__main__":
    main()
