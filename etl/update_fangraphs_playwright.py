# etl/update_fangraphs_playwright.py
import os
import time
from datetime import date
from pathlib import Path
import pandas as pd
import pg8000.native
from curl_cffi import requests
from dotenv import load_dotenv

# ---------------- Env & DB params ----------------
load_dotenv(Path(__file__).resolve().parents[1] / ".env.awsrds")

DB_CONFIG = {
    "database": os.getenv("AWSDATABASE") or os.getenv("PGDATABASE"),
    "user": os.getenv("AWSUSER") or os.getenv("PGUSER"),
    "password": os.getenv("AWSPASSWORD") or os.getenv("PGPASSWORD"),
    "host": os.getenv("AWSHOST") or os.getenv("PGHOST"),
    "port": int(os.getenv("AWSPORT") or os.getenv("PGPORT") or 5432),
}

YEAR = date.today().year

# ---------------- Fetcher Logic (The Secret Agent) ----------------
def fetch_fangraphs_stealth(stats_type: str, year: int) -> pd.DataFrame:
    api_url = f"https://www.fangraphs.com/api/leaders/major-league/data?age=&pos=all&stats={stats_type}&lg=all&qual=0&type=8&season={year}&month=0&season1={year}&ind=0&team=0&rost=0&filter=&players=0&pageitems=10000"
    
    print(f"🕵️  Creating browser session for {stats_type} {year}...")
    
    # Create a session to hold cookies
    s = requests.Session()
    
    # Step 1: Visit the main leaders page to get cookies/session
    s.get("https://www.fangraphs.com/leaders/major-league", impersonate="chrome120")
    time.sleep(2) # Wait like a human
    
    print(f"📡 Requesting API data...")
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": f"https://www.fangraphs.com/leaders/major-league?stats={stats_type}",
        "X-Requested-With": "XMLHttpRequest",
    }
    
    try:
        resp = s.get(api_url, headers=headers, impersonate="chrome120", timeout=30)
        
        if resp.status_code != 200:
            print(f"❌ Status {resp.status_code}. (Your local IP might be temporarily blocked).")
            return pd.DataFrame()
            
        data = resp.json()
        df = pd.DataFrame(data['data'] if 'data' in data else data)
        
        if not df.empty:
            print(f"✅ Success! Fetched {len(df)} rows.")
            # Rename and normalize
            rename_map = {'PlayerName': 'name', 'playerid': 'idfg', 'TeamNameAbbreviation': 'team', 'TeamName': 'team'}
            df.columns = [c.lower() for c in df.columns]
            df.rename(columns=rename_map, inplace=True)
            return df
    except Exception as e:
        print(f"❌ Error: {e}")
    return pd.DataFrame()

# ---------------- Database Logic ----------------
def clean_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            col_str = out[col].astype(str)
            if col_str.str.contains('%').any():
                out[col] = col_str.str.replace('%', '', regex=False).str.strip()
            try: out[col] = pd.to_numeric(out[col], errors='ignore')
            except: pass
    return out.fillna(0.0)

def upsert_table_pg8000(db: pg8000.native.Connection, df: pd.DataFrame, table_name: str):
    if df.empty: return
    all_cols = list(df.columns)
    key_cols = ["idfg", "season", "team"] if "team" in all_cols else ["idfg", "season"]
    non_key_cols = [c for c in all_cols if c not in key_cols]
    
    col_list = ", ".join([f'"{c}"' for c in all_cols])
    placeholders = ", ".join([f":{c}" for c in all_cols])
    set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols])
    
    sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders}) ON CONFLICT ({", ".join(key_cols)}) DO UPDATE SET {set_clause}'
    
    for row in df.to_dict('records'):
        db.run(sql, **row)

# Table mappings
batting_splits = {
    "fangraphs_batting_lahman_like": ["idfg","season","name","team","g","ab","pa","h","singles","doubles","triples","hr","r","rbi","bb","ibb","so","hbp","sf","sh","sb","cs"],
    "fangraphs_batting_standard_ratios": ["idfg","season","name","team","avg","obp","slg","ops","iso","babip","bb_pc","k_pc","bb_k","gdp"],
    "fangraphs_batting_advanced": ["idfg","season","name","team","woba","wraa","wrc","wrc_plus","war","rar","bat","fld","rep","pos","off","def","dol"],
}
pitching_splits = {
    "fangraphs_pitching_lahman_like": ["idfg","season","name","team","w","l","g","gs","cg","sho","sv","ip","h","r","er","hr","bb","so","hbp","wp","bk","tbf"],
    "fangraphs_pitching_standard_ratios": ["idfg","season","name","team","era","k_9","bb_9","k_bb","h_9","hr_9","avg","whip","babip","lob_pc"],
}

def main():
    print("🔌 Connecting to AWS RDS...")
    db = pg8000.native.Connection(**DB_CONFIG)
    try:
        # Batting
        df_bat = fetch_fangraphs_stealth('bat', YEAR)
        if not df_bat.empty:
            df_bat['season'] = YEAR
            df_bat = clean_and_normalize(df_bat)
            for t, c in batting_splits.items():
                valid = [col for col in c if col in df_bat.columns]
                if len(valid) >= 4:
                    print(f"📁 Updating {t}...")
                    upsert_table_pg8000(db, df_bat[valid], t)
        # Pitching
        df_pit = fetch_fangraphs_stealth('pit', YEAR)
        if not df_pit.empty:
            df_pit['season'] = YEAR
            df_pit = clean_and_normalize(df_pit)
            for t, c in pitching_splits.items():
                valid = [col for col in c if col in df_pit.columns]
                if len(valid) >= 4:
                    print(f"📁 Updating {t}...")
                    upsert_table_pg8000(db, df_pit[valid], t)
        print("✅ Finished.")
    finally:
        db.close()

if __name__ == "__main__":
    main()
