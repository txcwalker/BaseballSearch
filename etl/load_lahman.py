# etl/load_lahman.py
#
# Incrementally loads new-season rows from the local Lahman CSVs
# (data/lahman_raw/) into AWS RDS. Only inserts rows for a year that isn't
# already in the DB for that table -- never updates or deletes existing rows.
# `people` is handled separately (no season column): only playerids not
# already present are inserted. Pure dimension tables with no season concept
# (schools, parks, teamsfranchises) are intentionally not touched here.
#
# Defaults to --dry-run (report what WOULD be inserted, write nothing).
# Pass --commit to actually write to the database.

import argparse
import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env.awsrds")

DB_PARAMS = {
    "dbname": os.environ["AWSDATABASE"],
    "user": os.environ["AWSUSER"],
    "password": os.environ["AWSPASSWORD"],
    "host": os.environ["AWSHOST"],
    "port": os.environ["AWSPORT"],
}

CSV_DIR = ROOT / "data" / "lahman_raw"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

log_path = LOG_DIR / datetime.now().strftime("lahman_load_%Y-%m-%d_%H%M%S.txt")
logging.basicConfig(filename=log_path, level=logging.INFO,
                     format="%(asctime)s - %(levelname)s - %(message)s", filemode="w")

# table_name -> (csv_filename, year_column). All Lahman season tables use
# "yearid" except homegames, which uses "yearkey".
YEAR_KEYED_TABLES = {
    "batting": ("Batting.csv", "yearid"),
    "pitching": ("Pitching.csv", "yearid"),
    "fielding": ("Fielding.csv", "yearid"),
    "fieldingof": ("FieldingOF.csv", "yearid"),
    "fieldingofsplit": ("FieldingOFsplit.csv", "yearid"),
    "teams": ("Teams.csv", "yearid"),
    "teamshalf": ("TeamsHalf.csv", "yearid"),
    "battingpost": ("BattingPost.csv", "yearid"),
    "pitchingpost": ("PitchingPost.csv", "yearid"),
    "fieldingpost": ("FieldingPost.csv", "yearid"),
    "allstarfull": ("AllstarFull.csv", "yearid"),
    "appearances": ("Appearances.csv", "yearid"),
    "managers": ("Managers.csv", "yearid"),
    "managershalf": ("ManagersHalf.csv", "yearid"),
    "awardsplayers": ("AwardsPlayers.csv", "yearid"),
    "awardsmanagers": ("AwardsManagers.csv", "yearid"),
    "awardsshareplayers": ("AwardsSharePlayers.csv", "yearid"),
    "awardssharemanagers": ("AwardsShareManagers.csv", "yearid"),
    "halloffame": ("HallOfFame.csv", "yearid"),
    "salaries": ("Salaries.csv", "yearid"),
    "seriespost": ("SeriesPost.csv", "yearid"),
    "collegeplaying": ("CollegePlaying.csv", "yearid"),
    "homegames": ("HomeGames.csv", "yearkey"),
}

# Not handled here: no season column, rarely change, not needed for
# "bring season stats current" -- schools, parks, teamsfranchises.


def get_table_columns(cur, table_name):
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s",
        (table_name,),
    )
    return {r[0] for r in cur.fetchall()}


def read_csv_rows(csv_path):
    # utf-8-sig strips a BOM if present (Teams.csv/People.csv have one);
    # a no-op if absent (Batting.csv does not).
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = [{k.strip().lower(): v for k, v in row.items()} for row in reader]
    return rows


def insert_rows(cur, table_name, columns, rows, commit: bool):
    if not rows:
        return 0
    col_list = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f'INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})'
    values = [
        tuple((row.get(c) or None) for c in columns)
        for row in rows
    ]
    if commit:
        cur.executemany(sql, values)
    return len(values)


def load_year_keyed_table(conn, table_name, csv_filename, year_col, commit: bool):
    csv_path = CSV_DIR / csv_filename
    if not csv_path.exists():
        print(f"[skip] {table_name}: {csv_filename} not found")
        return

    with conn.cursor() as cur:
        db_columns = get_table_columns(cur, table_name)
        cur.execute(f'SELECT COALESCE(MAX("{year_col}"), 0) FROM {table_name}')
        max_year_in_db = cur.fetchone()[0]

    rows = read_csv_rows(csv_path)
    if not rows:
        print(f"[skip] {table_name}: CSV is empty")
        return

    # CSV column names -> only the ones that actually exist on the table
    # (drops stray columns like People.csv's leading "id" that don't map
    # to anything real, instead of assuming every CSV column is valid).
    csv_columns = [c for c in rows[0].keys() if c in db_columns]
    dropped = [c for c in rows[0].keys() if c not in db_columns]

    new_rows = [r for r in rows if int(r.get(year_col) or -1) > max_year_in_db]

    print(f"[{table_name}] DB max {year_col}={max_year_in_db} | CSV rows={len(rows)} | "
          f"new rows to insert={len(new_rows)} | columns used={len(csv_columns)}"
          + (f" | dropped unmatched CSV columns: {dropped}" if dropped else ""))

    if not new_rows:
        return

    with conn.cursor() as cur:
        try:
            n = insert_rows(cur, table_name, csv_columns, new_rows, commit)
            if commit:
                conn.commit()
                print(f"  -> inserted {n} rows into {table_name}")
            else:
                conn.rollback()
                print(f"  -> DRY RUN: would insert {n} rows (nothing written)")
        except Exception as e:
            conn.rollback()
            msg = f"{table_name}: insert failed: {e}"
            print("  -> ERROR:", msg)
            logging.error(msg)


def load_people(conn, commit: bool):
    csv_path = CSV_DIR / "People.csv"
    if not csv_path.exists():
        print("[skip] people: People.csv not found")
        return

    with conn.cursor() as cur:
        db_columns = get_table_columns(cur, "people")
        cur.execute("SELECT playerid FROM people")
        existing_ids = {r[0] for r in cur.fetchall()}

    rows = read_csv_rows(csv_path)
    csv_columns = [c for c in rows[0].keys() if c in db_columns]
    dropped = [c for c in rows[0].keys() if c not in db_columns]
    new_rows = [r for r in rows if r.get("playerid") and r["playerid"] not in existing_ids]

    print(f"[people] existing={len(existing_ids)} | CSV rows={len(rows)} | "
          f"new players to insert={len(new_rows)} | columns used={len(csv_columns)}"
          + (f" | dropped unmatched CSV columns: {dropped}" if dropped else ""))

    if not new_rows:
        return

    with conn.cursor() as cur:
        try:
            n = insert_rows(cur, "people", csv_columns, new_rows, commit)
            if commit:
                conn.commit()
                print(f"  -> inserted {n} new players into people")
            else:
                conn.rollback()
                print(f"  -> DRY RUN: would insert {n} new players (nothing written)")
        except Exception as e:
            conn.rollback()
            msg = f"people: insert failed: {e}"
            print("  -> ERROR:", msg)
            logging.error(msg)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", action="store_true",
                         help="Actually write to the database. Without this flag, runs as a dry run (no writes).")
    parser.add_argument("--only", help="Comma-separated table names to limit this run to (default: all).")
    args = parser.parse_args()

    commit = args.commit
    only = set(args.only.split(",")) if args.only else None

    print(f"Mode: {'COMMIT (writing to AWS RDS)' if commit else 'DRY RUN (no writes)'}")
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        load_people(conn, commit)
        for table_name, (csv_filename, year_col) in YEAR_KEYED_TABLES.items():
            if only and table_name not in only:
                continue
            load_year_keyed_table(conn, table_name, csv_filename, year_col, commit)
    finally:
        conn.close()
    print(f"\nDone. Log: {log_path}")


if __name__ == "__main__":
    main()
