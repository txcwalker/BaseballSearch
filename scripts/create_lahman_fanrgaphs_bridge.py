# scripts/create_lahman_fangraphs_bridge.py

import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

# DB connection setup
DB_PARAMS = {
    "dbname": os.getenv("PGDATABASE"),
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "host": os.getenv("PGHOST"),
    "port": os.getenv("PGPORT"),
}

def fetch_query(cur, query):
    cur.execute(query)
    return cur.fetchall(), [desc[0] for desc in cur.description]

def main():
    with psycopg2.connect(**DB_PARAMS) as conn:
        cur = conn.cursor()

        # Fetch player info from Lahman
        lahman_query = """
        SELECT playerid, namefirst, namelast, teamid, yearid
        FROM batting
        JOIN people USING (playerid)
        """
        lahman_rows, lahman_cols = fetch_query(cur, lahman_query)
        lahman_df = pd.DataFrame(lahman_rows, columns=lahman_cols)
        lahman_df["name"] = lahman_df["namefirst"].str.strip() + " " + lahman_df["namelast"].str.strip()

        # Fetch player info from FanGraphs
        fg_query = """
        SELECT DISTINCT idfg, name, team, season
        FROM fangraphs_batting_lahman_like
        """
        fg_rows, fg_cols = fetch_query(cur, fg_query)
        fg_df = pd.DataFrame(fg_rows, columns=fg_cols)

        # Normalize for merge
        fg_df["name"] = fg_df["name"].str.strip()
        lahman_df["team"] = lahman_df["teamid"].str.upper()
        lahman_df.rename(columns={"yearid": "season"}, inplace=True)

        # Merge on name, season, team
        bridge_df = pd.merge(fg_df, lahman_df, on=["name", "season", "team"], how="inner")
        bridge_df = bridge_df[["idfg", "playerid"]].drop_duplicates()

        # Create table
        cur.execute("DROP TABLE IF EXISTS lahman_fangraphs_bridge")
        cur.execute("""
            CREATE TABLE lahman_fangraphs_bridge (
                idfg INT,
                playerid VARCHAR,
                PRIMARY KEY (idfg, playerid)
            )
        """)

        # Insert data
        for _, row in bridge_df.iterrows():
            cur.execute(
                "INSERT INTO lahman_fangraphs_bridge (idfg, playerid) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (row["idfg"], row["playerid"])
            )

        conn.commit()
        print(f"✅ Inserted {len(bridge_df)} rows into lahman_fangraphs_bridge.")

if __name__ == "__main__":
    main()
