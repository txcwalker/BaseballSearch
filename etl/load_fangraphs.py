# etl/load_all_fangraphs.py

import os
import csv
import psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path='../.env')

DB_PARAMS = {
    "host": os.getenv("PGHOST"),
    "dbname": os.getenv("PGDATABASE"),
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "port": os.getenv("PGPORT"),
}

CSV_DIR = "../data/processed/fangraphs"

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

def format_column_list(columns):
    return ", ".join([f'"{col}"' if not col.isidentifier() else col for col in columns])

def load_csv_to_table(filename, conn):
    table_name = os.path.splitext(filename)[0].lower()
    filepath = os.path.join(CSV_DIR, filename)
    print(f"📥 Loading `{table_name}` from `{filename}`...")

    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            print(f"⚠️ Skipping {filename}: No header row found.")
            return

        columns = [col.strip().lower() for col in reader.fieldnames]
        col_str = format_column_list(columns)
        placeholder_str = ", ".join(["%s"] * len(columns))

        pk_fields = ('idfg', 'season')
        update_fields = [col for col in columns if col not in pk_fields]
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_fields])
        where_clause = " OR ".join([f"{table_name}.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in update_fields])

        insert_sql = f"""
        INSERT INTO {table_name} ({col_str})
        VALUES ({placeholder_str})
        ON CONFLICT (idfg, season)
        DO UPDATE SET {update_clause}
        WHERE {where_clause}
        """

        with conn.cursor() as cur:
            for row in reader:
                try:
                    cleaned = {k.strip().lower(): v for k, v in row.items()}
                    values = [parse_value(cleaned.get(col)) for col in columns]

                    if len(values) != len(columns):
                        raise ValueError(f"Mismatch: {len(values)} values vs {len(columns)} columns")

                    cur.execute(insert_sql, values)

                except Exception:
                    conn.rollback()

        conn.commit()
        print(f"✅ Finished loading `{table_name}`")

def main():
    with psycopg2.connect(**DB_PARAMS) as conn:
        for filename in os.listdir(CSV_DIR):
            if filename.endswith(".csv"):
                load_csv_to_table(filename, conn)
        print("🎉 All FanGraphs tables loaded.")

if __name__ == "__main__":
    main()
