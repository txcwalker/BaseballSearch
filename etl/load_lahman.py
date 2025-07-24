import os
import csv
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "dbname": os.getenv("PGDATABASE", "baseball"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "Sqr693202!?!"),
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
}

CSV_DIR = "../data/lahman_raw"

def load_csv_to_table(filename, conn, valid_playerids=None):
    table_name = os.path.splitext(filename)[0].lower()
    filepath = os.path.join(CSV_DIR, filename)
    print(f"📥 Loading {filename} into `{table_name}`...")

    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        # Normalize column names
        COLUMN_RENAMES = {
            "2b": "doubles",
            "3b": "triples",
            # Add more if needed
        }

        original_fields = reader.fieldnames
        if not original_fields:
            print(f"⚠️  Skipping {filename}: No header row found.")
            return

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
                    row_cleaned = {k.strip().lower(): v for k, v in row.items()}
                    values = [row_cleaned.get(col, None) if row_cleaned.get(col, "") != "" else None for col in columns]
                    placeholders = ", ".join(["%s"] * len(columns))
                    column_names = ", ".join([f'"{col}"' for col in columns])
                    sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                    cur.execute(sql, values)
                    insert_count += 1
                except Exception as e:
                    print(f"❌ Row {i} failed in `{table_name}`: {e}")
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

if __name__ == "__main__":
    main()
